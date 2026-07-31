package nats

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/rs/zerolog"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

const (
	defaultBehaviorPollInterval  = 5 * time.Second
	defaultBehaviorWindow        = 60 * time.Second
	defaultBehaviorWarmup        = 5 * time.Minute
	defaultBehaviorMinSamples    = 50
	defaultBehaviorLatencyFactor = 3.0
	defaultBehaviorRateTolerance = 0.3
	defaultBehaviorSustainFor    = 30 * time.Second
	behaviorEventBuffer          = 16
	behaviorLearnAlpha           = 0.05
	behaviorHealthyAlpha         = 0.02
)

// BehaviorSnapshot is a rate + processing-time fingerprint for a consumer.
type BehaviorSnapshot struct {
	MsgPerMin  float64
	Processing time.Duration
}

// BehaviorAnomalyEvent is emitted when throughput stays near baseline while
// handling latency regresses by LatencyFactor for SustainFor.
type BehaviorAnomalyEvent struct {
	Stream       string
	Durable      string
	Normal       BehaviorSnapshot
	Current      BehaviorSnapshot
	SustainedFor time.Duration
}

// BehaviorFingerprintConfig controls WatchBehaviorFingerprint.
// goalign:ignore
type BehaviorFingerprintConfig struct {
	OnAnomaly func(BehaviorAnomalyEvent)
	// ReportKV, when set, publishes Normal/Current snapshots for nats-consol.
	ReportKV KeyValueKeys
	// ReportBucket defaults to DefaultBehaviorFingerprintKVBucket.
	ReportBucket string
	// PollInterval is how often the rolling window is evaluated (default 5s).
	PollInterval time.Duration
	// Window is the sample window used for current rate/latency (default 60s).
	Window time.Duration
	// Warmup is how long to learn baselines before detecting (default 5m).
	Warmup time.Duration
	// MinSamples required before detection (default 50).
	MinSamples int
	// LatencyFactor fires when current processing >= factor × baseline (default 3).
	LatencyFactor float64
	// RateTolerance is the ± fraction of baseline rate treated as "same throughput" (default 0.3).
	RateTolerance float64
	// SustainFor is how long the anomaly condition must hold before firing (default 30s).
	SustainFor time.Duration
	// CircuitStop stops the watcher after the first anomaly.
	CircuitStop bool
}

func (c BehaviorFingerprintConfig) withDefaults() BehaviorFingerprintConfig {
	out := c
	if out.PollInterval <= 0 {
		out.PollInterval = defaultBehaviorPollInterval
	}
	if out.Window <= 0 {
		out.Window = defaultBehaviorWindow
	}
	if out.Warmup <= 0 {
		out.Warmup = defaultBehaviorWarmup
	}
	if out.MinSamples <= 0 {
		out.MinSamples = defaultBehaviorMinSamples
	}
	if out.LatencyFactor <= 0 {
		out.LatencyFactor = defaultBehaviorLatencyFactor
	}
	if out.RateTolerance <= 0 {
		out.RateTolerance = defaultBehaviorRateTolerance
	}
	if out.SustainFor <= 0 {
		out.SustainFor = defaultBehaviorSustainFor
	}

	return out
}

// EvaluateBehaviorFingerprint reports whether current looks anomalous vs baseline.
// Rate must stay within RateTolerance of baseline while latency exceeds LatencyFactor.
func EvaluateBehaviorFingerprint(current, baseline BehaviorSnapshot, cfg BehaviorFingerprintConfig) bool {
	cfg = cfg.withDefaults()
	if baseline.MsgPerMin <= 0 || baseline.Processing <= 0 {
		return false
	}
	if current.MsgPerMin <= 0 || current.Processing <= 0 {
		return false
	}

	lo := baseline.MsgPerMin * (1 - cfg.RateTolerance)
	hi := baseline.MsgPerMin * (1 + cfg.RateTolerance)
	if current.MsgPerMin < lo || current.MsgPerMin > hi {
		return false
	}

	threshold := time.Duration(float64(baseline.Processing) * cfg.LatencyFactor)

	return current.Processing >= threshold
}

type behaviorSample struct {
	at time.Time
	d  time.Duration
}

// BehaviorFingerprint learns normal msg/min and processing latency, then detects regressions.
// goalign:ignore
type BehaviorFingerprint struct {
	startedAt       time.Time
	breachStart     time.Time
	lastReportAt    time.Time
	sub             Subscription
	events          chan BehaviorAnomalyEvent
	stopCh          chan struct{}
	metrics         *clientMetrics
	samples         []behaviorSample
	cfg             BehaviorFingerprintConfig
	lastCurrent     BehaviorSnapshot
	baselineRate    float64
	totalObserved   int
	baselineLatency time.Duration
	stopOnce        sync.Once
	mu              sync.Mutex
	anomalous       atomic.Bool
	haveBaseline    bool
	haveBreach      bool
}

// WatchBehaviorFingerprint starts learning baselines from Observe samples.
// Register Observe via consumer.OnMessageHandled, or call Observe manually.
func WatchBehaviorFingerprint(
	ctx context.Context,
	sub Subscription,
	cfg BehaviorFingerprintConfig,
	metrics *clientMetrics,
) (*BehaviorFingerprint, error) {
	if sub == nil {
		return nil, fmt.Errorf("behavior fingerprint: subscription is nil")
	}

	cfg = cfg.withDefaults()
	bf := &BehaviorFingerprint{
		sub:       sub,
		cfg:       cfg,
		metrics:   metrics,
		events:    make(chan BehaviorAnomalyEvent, behaviorEventBuffer),
		stopCh:    make(chan struct{}),
		startedAt: time.Now(),
	}
	go bf.loop(ctx)

	return bf, nil
}

// Observe records one completed message handling duration.
func (b *BehaviorFingerprint) Observe(elapsed time.Duration) {
	if b == nil || elapsed < 0 {
		return
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	now := time.Now()
	b.samples = append(b.samples, behaviorSample{at: now, d: elapsed})
	b.totalObserved++
	b.trimLocked(now)
}

// Events returns anomaly notifications (buffered; overflows are dropped).
func (b *BehaviorFingerprint) Events() <-chan BehaviorAnomalyEvent { return b.events }

// Anomalous reports whether an anomaly has been detected (sticky until Stop).
func (b *BehaviorFingerprint) Anomalous() bool { return b.anomalous.Load() }

// Snapshot returns the learned baseline, latest window stats, and whether a baseline exists.
func (b *BehaviorFingerprint) Snapshot() (BehaviorSnapshot, BehaviorSnapshot, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()

	return BehaviorSnapshot{
		MsgPerMin:  b.baselineRate,
		Processing: b.baselineLatency,
	}, b.lastCurrent, b.haveBaseline
}

// Stop ends the watch loop.
func (b *BehaviorFingerprint) Stop() {
	b.stopOnce.Do(func() { close(b.stopCh) })
}

func (b *BehaviorFingerprint) loop(ctx context.Context) {
	ticker := time.NewTicker(b.cfg.PollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-b.stopCh:
			return
		case <-ticker.C:
			b.tick(ctx)
		}
	}
}

func (b *BehaviorFingerprint) tick(ctx context.Context) {
	b.mu.Lock()
	now := time.Now()
	b.trimLocked(now)
	current := b.windowStatsLocked(now)
	b.lastCurrent = current

	learning := now.Sub(b.startedAt) < b.cfg.Warmup ||
		b.totalObserved < b.cfg.MinSamples ||
		now.Sub(b.startedAt) < b.cfg.Window
	normal := BehaviorSnapshot{
		MsgPerMin:  b.baselineRate,
		Processing: b.baselineLatency,
	}
	ready := b.haveBaseline
	anomalyNow := false
	if ready && !learning && current.MsgPerMin > 0 && current.Processing > 0 {
		anomalyNow = EvaluateBehaviorFingerprint(current, normal, b.cfg)
	}

	if learning || !ready {
		if current.MsgPerMin > 0 && current.Processing > 0 {
			b.updateBaselineLocked(current, behaviorLearnAlpha)
		}
		b.haveBreach = false
		b.breachStart = time.Time{}
		b.mu.Unlock()

		return
	}

	if !anomalyNow {
		if nearBehaviorBaseline(current, normal, b.cfg) {
			b.updateBaselineLocked(current, behaviorHealthyAlpha)
		}
		b.haveBreach = false
		b.breachStart = time.Time{}
		stream, durable := "", ""
		shouldReport := b.shouldReportLocked(now)
		if shouldReport {
			b.lastReportAt = now
			stream, durable = b.identityLocked()
		}
		reportNormal, reportCurrent := normal, current
		b.mu.Unlock()
		if shouldReport {
			b.reportKV(ctx, false, stream, durable, reportNormal, reportCurrent, 0)
		}

		return
	}

	if !b.haveBreach {
		b.breachStart = now
		b.haveBreach = true
	}
	sustained := now.Sub(b.breachStart)
	if sustained < b.cfg.SustainFor {
		b.mu.Unlock()

		return
	}
	if b.anomalous.Load() && !b.cfg.CircuitStop {
		b.mu.Unlock()

		return
	}

	stream, durable := b.identityLocked()
	ev := BehaviorAnomalyEvent{
		Stream:       stream,
		Durable:      durable,
		Normal:       normal,
		Current:      current,
		SustainedFor: sustained,
	}
	b.anomalous.Store(true)
	b.haveBreach = false
	b.breachStart = time.Time{}
	b.lastReportAt = now
	b.mu.Unlock()

	b.emit(ev)
	b.reportKV(ctx, true, ev.Stream, ev.Durable, ev.Normal, ev.Current, ev.SustainedFor)

	if b.metrics != nil && b.metrics.behaviorFingerprintAnomaly != nil {
		b.metrics.behaviorFingerprintAnomaly.Add(ctx, 1)
	}

	zerolog.Ctx(ctx).Warn().
		Str("stream", ev.Stream).
		Str("durable", ev.Durable).
		Float64("normal_msg_per_min", ev.Normal.MsgPerMin).
		Dur("normal_processing", ev.Normal.Processing).
		Float64("current_msg_per_min", ev.Current.MsgPerMin).
		Dur("current_processing", ev.Current.Processing).
		Dur("sustained_for", ev.SustainedFor).
		Msg("consumer behavior fingerprint anomaly")

	if b.cfg.CircuitStop {
		b.Stop()
	}
}

func (b *BehaviorFingerprint) shouldReportLocked(now time.Time) bool {
	if b.cfg.ReportKV == nil {
		return false
	}
	if b.lastReportAt.IsZero() {
		return true
	}

	return now.Sub(b.lastReportAt) >= b.cfg.Window
}

func (b *BehaviorFingerprint) reportKV(
	ctx context.Context,
	anomaly bool,
	stream, durable string,
	normal, current BehaviorSnapshot,
	sustained time.Duration,
) {
	if b.cfg.ReportKV == nil || bytesconv.IsEmpty(stream) || bytesconv.IsEmpty(durable) {
		return
	}
	if err := ReportBehaviorFingerprintKV(
		ctx,
		b.cfg.ReportKV,
		b.cfg.ReportBucket,
		stream,
		durable,
		anomaly,
		normal,
		current,
		sustained,
	); err != nil {
		zerolog.Ctx(ctx).Warn().Err(err).
			Str("stream", stream).
			Str("durable", durable).
			Msg("behavior fingerprint kv report failed")
	}
}

func (b *BehaviorFingerprint) updateBaselineLocked(current BehaviorSnapshot, alpha float64) {
	if !b.haveBaseline {
		b.baselineRate = current.MsgPerMin
		b.baselineLatency = current.Processing
		b.haveBaseline = true

		return
	}
	b.baselineRate = ewmaFloat(b.baselineRate, current.MsgPerMin, alpha)
	b.baselineLatency = time.Duration(ewmaFloat(
		float64(b.baselineLatency),
		float64(current.Processing),
		alpha,
	))
}

func (b *BehaviorFingerprint) trimLocked(now time.Time) {
	cut := now.Add(-b.cfg.Window)
	i := 0
	for i < len(b.samples) && b.samples[i].at.Before(cut) {
		i++
	}
	if i > 0 {
		b.samples = append([]behaviorSample(nil), b.samples[i:]...)
	}
}

func (b *BehaviorFingerprint) windowStatsLocked(now time.Time) BehaviorSnapshot {
	_ = now
	n := len(b.samples)
	if n == 0 {
		return BehaviorSnapshot{}
	}

	var sum time.Duration
	for _, s := range b.samples {
		sum += s.d
	}
	mean := sum / time.Duration(n)

	window := b.cfg.Window
	if window <= 0 {
		window = defaultBehaviorWindow
	}
	// Fixed window denominator keeps msg/min stable once observe cadence is steady.
	msgPerMin := float64(n) / window.Minutes()

	return BehaviorSnapshot{
		MsgPerMin:  msgPerMin,
		Processing: mean,
	}
}

func (b *BehaviorFingerprint) identityLocked() (string, string) {
	if b.sub == nil {
		return "", ""
	}
	info, err := b.sub.ConsumerInfo()
	if err != nil || info == nil {
		return "", ""
	}

	return info.Stream, info.Name
}

func (b *BehaviorFingerprint) emit(ev BehaviorAnomalyEvent) {
	if b.cfg.OnAnomaly != nil {
		b.cfg.OnAnomaly(ev)
	}
	trySend(b.events, ev)
}

func nearBehaviorBaseline(current, baseline BehaviorSnapshot, cfg BehaviorFingerprintConfig) bool {
	if baseline.MsgPerMin <= 0 || baseline.Processing <= 0 {
		return false
	}
	if current.MsgPerMin <= 0 || current.Processing <= 0 {
		return false
	}
	// Do not train while latency is drifting up toward the anomaly threshold.
	if current.Processing > time.Duration(float64(baseline.Processing)*1.25) {
		return false
	}
	lo := baseline.MsgPerMin * (1 - cfg.RateTolerance)
	hi := baseline.MsgPerMin * (1 + cfg.RateTolerance)

	return current.MsgPerMin >= lo && current.MsgPerMin <= hi
}

func ewmaFloat(prev, sample, alpha float64) float64 {
	if prev == 0 {
		return sample
	}

	return alpha*sample + (1-alpha)*prev
}

// WatchBehaviorFingerprint is a client helper that hooks OnMessageHandled on the shared consumer.
func (c *client) WatchBehaviorFingerprint(
	ctx context.Context,
	sub Subscription,
	cfg BehaviorFingerprintConfig,
) (*BehaviorFingerprint, error) {
	if cfg.ReportKV == nil && c.kv != nil {
		cfg.ReportKV = c.kv
	}
	bf, err := WatchBehaviorFingerprint(ctx, sub, cfg, c.metrics)
	if err != nil {
		return nil, err
	}

	if c.consumer != nil {
		c.consumer.OnMessageHandled(bf.Observe)
	}

	return bf, nil
}
