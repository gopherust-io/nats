package nats

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gopherust-io/nats/internal/bytesconv"
	"github.com/rs/zerolog"
)

const (
	defaultSlowConsumerPollInterval    = 2 * time.Second
	defaultSlowConsumerSustainFor      = 30 * time.Second
	defaultSlowConsumerPendingThresh   = uint64(1000)
	defaultSlowConsumerLagThresh       = uint64(1000)
	defaultSlowConsumerAckPendingRatio = 0.9
	slowConsumerEventBuffer            = 16
)

const (
	SlowReasonPending    = "pending"
	SlowReasonLag        = "lag"
	SlowReasonAckPending = "ack_pending"
)

// StreamLastSeqFunc returns the stream tip sequence for lag calculation.
type StreamLastSeqFunc func(ctx context.Context, stream string) (uint64, error)

// SlowConsumerConfig controls WatchSlowConsumer threshold detection.
// goalign:ignore
type SlowConsumerConfig struct {
	OnSlow func(SlowConsumerEvent)
	// PollInterval is how often ConsumerInfo is polled (default 2s).
	PollInterval time.Duration
	// SustainFor is how long thresholds must hold before firing (default 30s).
	SustainFor time.Duration
	// PendingThreshold fires when NumPending >= this value (default 1000).
	PendingThreshold uint64
	// LagThreshold fires when stream tip − delivered stream seq >= this value (default 1000).
	LagThreshold uint64
	// AckPendingRatio fires when NumAckPending >= ratio * MaxAckPending (default 0.9).
	// Ignored when MaxAckPending <= 0.
	AckPendingRatio float64
	// CircuitStop stops the watcher after the first slow event.
	CircuitStop bool
}

func (c SlowConsumerConfig) withDefaults() SlowConsumerConfig {
	out := c
	if out.PollInterval <= 0 {
		out.PollInterval = defaultSlowConsumerPollInterval
	}
	if out.SustainFor <= 0 {
		out.SustainFor = defaultSlowConsumerSustainFor
	}
	if out.PendingThreshold == 0 {
		out.PendingThreshold = defaultSlowConsumerPendingThresh
	}
	if out.LagThreshold == 0 {
		out.LagThreshold = defaultSlowConsumerLagThresh
	}
	if out.AckPendingRatio <= 0 {
		out.AckPendingRatio = defaultSlowConsumerAckPendingRatio
	}
	return out
}

// SlowConsumerEvent is emitted when backlog thresholds are sustained.
type SlowConsumerEvent struct {
	Stream        string
	Durable       string
	Reasons       []string
	Pending       uint64
	Lag           uint64
	AckPending    int
	MaxAckPending int
	SustainedFor  time.Duration
}

// EvaluateSlowConsumer returns whether thresholds are met and why.
// Thresholds of 0 mean "use defaults" via withDefaults on cfg first, or pass
// an already-defaulted config.
func EvaluateSlowConsumer(pending, lag uint64, ackPending, maxAckPending int, cfg SlowConsumerConfig) (slow bool, reasons []string) {
	cfg = cfg.withDefaults()
	if pending >= cfg.PendingThreshold {
		reasons = append(reasons, SlowReasonPending)
	}
	if lag >= cfg.LagThreshold {
		reasons = append(reasons, SlowReasonLag)
	}
	if maxAckPending > 0 {
		limit := int(float64(maxAckPending) * cfg.AckPendingRatio)
		if limit < 1 {
			limit = 1
		}
		if ackPending >= limit {
			reasons = append(reasons, SlowReasonAckPending)
		}
	}
	return len(reasons) > 0, reasons
}

// ConsumerLagMessages returns max(0, streamLastSeq − deliveredStreamSeq).
func ConsumerLagMessages(streamLastSeq, deliveredStreamSeq uint64) uint64 {
	if streamLastSeq <= deliveredStreamSeq {
		return 0
	}
	return streamLastSeq - deliveredStreamSeq
}

// SlowConsumer watches a subscription for sustained JetStream backlog thresholds.
// goalign:ignore
type SlowConsumer struct {
	sub           Subscription
	streamLastSeq StreamLastSeqFunc
	events        chan SlowConsumerEvent
	stopCh        chan struct{}
	metrics       *clientMetrics
	cfg           SlowConsumerConfig
	stopOnce      sync.Once
	slow          atomic.Bool
}

// WatchSlowConsumer starts polling sub.ConsumerInfo for sustained threshold breaches.
// streamLastSeq may be nil; lag is then treated as 0 (pending / ack-pending still apply).
func WatchSlowConsumer(
	ctx context.Context,
	sub Subscription,
	streamLastSeq StreamLastSeqFunc,
	cfg SlowConsumerConfig,
	metrics *clientMetrics,
) (*SlowConsumer, error) {
	if sub == nil {
		return nil, fmt.Errorf("slow consumer: subscription is nil")
	}

	cfg = cfg.withDefaults()
	sc := &SlowConsumer{
		sub:           sub,
		streamLastSeq: streamLastSeq,
		cfg:           cfg,
		metrics:       metrics,
		events:        make(chan SlowConsumerEvent, slowConsumerEventBuffer),
		stopCh:        make(chan struct{}),
	}
	go sc.loop(ctx)
	return sc, nil
}

// Events returns slow-consumer notifications (buffered; overflows are dropped).
func (s *SlowConsumer) Events() <-chan SlowConsumerEvent { return s.events }

// Slow reports whether a slow condition has been detected (sticky until Stop).
func (s *SlowConsumer) Slow() bool { return s.slow.Load() }

// Stop ends the watch loop.
func (s *SlowConsumer) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
}

func (s *SlowConsumer) loop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	var (
		breachStart time.Time
		haveBreach  bool
	)

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			info, err := s.sub.ConsumerInfo()
			if err != nil || info == nil {
				haveBreach = false
				continue
			}

			var lastSeq uint64
			if s.streamLastSeq != nil && !bytesconv.IsEmpty(info.Stream) {
				if tip, tipErr := s.streamLastSeq(ctx, info.Stream); tipErr == nil {
					lastSeq = tip
				}
			}
			lag := ConsumerLagMessages(lastSeq, info.Delivered.Stream)
			slowNow, reasons := EvaluateSlowConsumer(
				info.NumPending,
				lag,
				info.NumAckPending,
				info.Config.MaxAckPending,
				s.cfg,
			)
			if !slowNow {
				haveBreach = false
				continue
			}

			now := time.Now()
			if !haveBreach {
				breachStart = now
				haveBreach = true
			}
			sustained := now.Sub(breachStart)
			if sustained < s.cfg.SustainFor {
				continue
			}
			if s.slow.Load() && !s.cfg.CircuitStop {
				// Already fired; avoid spamming every poll. Sticky Slow() remains.
				// Re-arm sustain after a clear (handled when !slowNow).
				continue
			}

			ev := SlowConsumerEvent{
				Stream:        info.Stream,
				Durable:       info.Name,
				Reasons:       reasons,
				Pending:       info.NumPending,
				Lag:           lag,
				AckPending:    info.NumAckPending,
				MaxAckPending: info.Config.MaxAckPending,
				SustainedFor:  sustained,
			}
			s.slow.Store(true)
			s.emit(ev)

			if s.metrics != nil && s.metrics.slowConsumerDetected != nil {
				s.metrics.slowConsumerDetected.Add(ctx, 1)
			}

			zerolog.Ctx(ctx).Warn().
				Str("stream", ev.Stream).
				Str("durable", ev.Durable).
				Strs("reasons", ev.Reasons).
				Uint64("num_pending", ev.Pending).
				Uint64("lag", ev.Lag).
				Int("ack_pending", ev.AckPending).
				Dur("sustained_for", ev.SustainedFor).
				Msg("slow consumer detected")

			if s.cfg.CircuitStop {
				return
			}
			// Prevent immediate re-fire; next fire requires clearing then re-breaching.
			haveBreach = false
			breachStart = time.Time{}
		}
	}
}

func (s *SlowConsumer) emit(ev SlowConsumerEvent) {
	if s.cfg.OnSlow != nil {
		s.cfg.OnSlow(ev)
	}
	trySend(s.events, ev)
}

// WatchSlowConsumer is a client helper that resolves stream tip via Streams().StreamInfo.
func (c *client) WatchSlowConsumer(
	ctx context.Context,
	sub Subscription,
	cfg SlowConsumerConfig,
) (*SlowConsumer, error) {
	var lastSeq StreamLastSeqFunc
	if c.streams != nil {
		lastSeq = func(ctx context.Context, stream string) (uint64, error) {
			info, err := c.streams.StreamInfo(ctx, stream)
			if err != nil || info == nil {
				return 0, err
			}
			return info.State.LastSeq, nil
		}
	}
	return WatchSlowConsumer(ctx, sub, lastSeq, cfg, c.metrics)
}
