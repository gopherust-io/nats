package nats

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEvaluateBehaviorFingerprint(t *testing.T) {
	t.Parallel()

	cfg := BehaviorFingerprintConfig{
		LatencyFactor: 3,
		RateTolerance: 0.3,
	}
	baseline := BehaviorSnapshot{MsgPerMin: 1000, Processing: 200 * time.Millisecond}

	assert.True(t, EvaluateBehaviorFingerprint(
		BehaviorSnapshot{MsgPerMin: 1000, Processing: 2400 * time.Millisecond},
		baseline,
		cfg,
	))
	assert.True(t, EvaluateBehaviorFingerprint(
		BehaviorSnapshot{MsgPerMin: 1100, Processing: 700 * time.Millisecond},
		baseline,
		cfg,
	))

	// Latency high but rate collapsed → not the fingerprint anomaly.
	assert.False(t, EvaluateBehaviorFingerprint(
		BehaviorSnapshot{MsgPerMin: 100, Processing: 2400 * time.Millisecond},
		baseline,
		cfg,
	))

	// Same rate, latency below factor.
	assert.False(t, EvaluateBehaviorFingerprint(
		BehaviorSnapshot{MsgPerMin: 1000, Processing: 400 * time.Millisecond},
		baseline,
		cfg,
	))

	assert.False(t, EvaluateBehaviorFingerprint(
		BehaviorSnapshot{MsgPerMin: 1000, Processing: 2 * time.Second},
		BehaviorSnapshot{},
		cfg,
	))
}

type behaviorInfoSub struct {
	info atomic.Pointer[natspkg.ConsumerInfo]
	fakeSub
}

func (s *behaviorInfoSub) ConsumerInfo() (*natspkg.ConsumerInfo, error) {
	return s.info.Load(), nil
}

func TestWatchBehaviorFingerprintDetectsAnomaly(t *testing.T) {
	// Timing-sensitive: keep serial so CI load does not stretch sleeps unevenly.
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	sub := &behaviorInfoSub{}
	sub.valid.Store(true)
	sub.info.Store(&natspkg.ConsumerInfo{Stream: "ORDERS", Name: "billing-worker"})

	const (
		poll     = 20 * time.Millisecond
		window   = 100 * time.Millisecond
		warmup   = time.Second // >> window so EWMA baseline can catch full-window rate
		sustain  = 50 * time.Millisecond
		interval = 3 * time.Millisecond
	)

	got := make(chan BehaviorAnomalyEvent, 1)
	started := time.Now()
	bf, err := WatchBehaviorFingerprint(ctx, sub, BehaviorFingerprintConfig{
		PollInterval:  poll,
		Window:        window,
		Warmup:        warmup,
		MinSamples:    8,
		LatencyFactor: 3,
		RateTolerance: 0.5,
		SustainFor:    sustain,
		CircuitStop:   true,
		OnAnomaly: func(ev BehaviorAnomalyEvent) {
			select {
			case got <- ev:
			default:
			}
		},
	}, nil)
	require.NoError(t, err)
	t.Cleanup(bf.Stop)

	// Learn only while still in warmup — do not inject slow samples yet or the
	// baseline EWMA absorbs the regression.
	learnUntil := started.Add(warmup + 50*time.Millisecond)
	for time.Now().Before(learnUntil) {
		bf.Observe(200 * time.Millisecond)
		time.Sleep(interval)
	}

	require.Eventually(t, func() bool {
		normal, current, ready := bf.Snapshot()
		if !ready || bf.Anomalous() || normal.Processing <= 0 || current.MsgPerMin <= 0 {
			return false
		}
		// Baseline rate must be near current full-window rate before we slow down.
		return nearBehaviorBaseline(current, normal, BehaviorFingerprintConfig{RateTolerance: 0.5})
	}, 2*time.Second, poll)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		bf.Observe(2400 * time.Millisecond)
		select {
		case ev := <-got:
			assert.Equal(t, "ORDERS", ev.Stream)
			assert.Equal(t, "billing-worker", ev.Durable)
			assert.InDelta(t, ev.Normal.MsgPerMin, ev.Current.MsgPerMin, ev.Normal.MsgPerMin*0.5+1)
			assert.GreaterOrEqual(t, ev.Current.Processing, 3*ev.Normal.Processing)
			assert.True(t, bf.Anomalous())
			return
		default:
			time.Sleep(interval)
		}
	}
	normal, current, ready := bf.Snapshot()
	t.Fatalf("expected behavior fingerprint anomaly; ready=%v anomalous=%v normal=%+v current=%+v",
		ready, bf.Anomalous(), normal, current)
}

func TestWatchBehaviorFingerprintNoAnomalyOnRateCollapse(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	sub := &behaviorInfoSub{}
	sub.valid.Store(true)
	sub.info.Store(&natspkg.ConsumerInfo{Stream: "ORDERS", Name: "billing-worker"})

	bf, err := WatchBehaviorFingerprint(ctx, sub, BehaviorFingerprintConfig{
		PollInterval:  15 * time.Millisecond,
		Window:        120 * time.Millisecond,
		Warmup:        150 * time.Millisecond,
		MinSamples:    8,
		LatencyFactor: 3,
		RateTolerance: 0.3,
		SustainFor:    30 * time.Millisecond,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(bf.Stop)

	deadline := time.Now().Add(220 * time.Millisecond)
	for time.Now().Before(deadline) {
		bf.Observe(200 * time.Millisecond)
		time.Sleep(2 * time.Millisecond)
	}
	require.Eventually(t, func() bool {
		_, _, ready := bf.Snapshot()
		return ready
	}, time.Second, 10*time.Millisecond)

	// High latency but much lower observe rate (rate collapse).
	slowUntil := time.Now().Add(120 * time.Millisecond)
	for time.Now().Before(slowUntil) {
		bf.Observe(2400 * time.Millisecond)
		time.Sleep(40 * time.Millisecond)
	}

	select {
	case <-bf.Events():
		t.Fatal("unexpected anomaly when rate collapsed")
	case <-time.After(150 * time.Millisecond):
		assert.False(t, bf.Anomalous())
	}
}

func TestWatchBehaviorFingerprintClearsBeforeSustain(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	sub := &behaviorInfoSub{}
	sub.valid.Store(true)
	sub.info.Store(&natspkg.ConsumerInfo{Stream: "ORDERS", Name: "worker"})

	bf, err := WatchBehaviorFingerprint(ctx, sub, BehaviorFingerprintConfig{
		PollInterval:  10 * time.Millisecond,
		Window:        100 * time.Millisecond,
		Warmup:        120 * time.Millisecond,
		MinSamples:    6,
		LatencyFactor: 3,
		RateTolerance: 0.5,
		SustainFor:    250 * time.Millisecond,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(bf.Stop)

	deadline := time.Now().Add(200 * time.Millisecond)
	for time.Now().Before(deadline) {
		bf.Observe(200 * time.Millisecond)
		time.Sleep(2 * time.Millisecond)
	}
	require.Eventually(t, func() bool {
		_, _, ready := bf.Snapshot()
		return ready
	}, time.Second, 10*time.Millisecond)

	// Brief latency spike, then recover before SustainFor.
	spikeUntil := time.Now().Add(40 * time.Millisecond)
	for time.Now().Before(spikeUntil) {
		bf.Observe(2400 * time.Millisecond)
		time.Sleep(2 * time.Millisecond)
	}
	recoverUntil := time.Now().Add(80 * time.Millisecond)
	for time.Now().Before(recoverUntil) {
		bf.Observe(200 * time.Millisecond)
		time.Sleep(2 * time.Millisecond)
	}

	select {
	case <-bf.Events():
		t.Fatal("unexpected anomaly when breach cleared before sustain")
	case <-time.After(100 * time.Millisecond):
		assert.False(t, bf.Anomalous())
	}
}

func TestWatchBehaviorFingerprintWarmupNoAnomaly(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	sub := &behaviorInfoSub{}
	sub.valid.Store(true)

	bf, err := WatchBehaviorFingerprint(ctx, sub, BehaviorFingerprintConfig{
		PollInterval:  10 * time.Millisecond,
		Window:        100 * time.Millisecond,
		Warmup:        300 * time.Millisecond,
		MinSamples:    1,
		LatencyFactor: 3,
		RateTolerance: 0.5,
		SustainFor:    20 * time.Millisecond,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(bf.Stop)

	// Seed a fast baseline window, then immediately go slow — still in warmup.
	for i := 0; i < 10; i++ {
		bf.Observe(50 * time.Millisecond)
	}
	time.Sleep(25 * time.Millisecond)
	until := time.Now().Add(80 * time.Millisecond)
	for time.Now().Before(until) {
		bf.Observe(2 * time.Second)
		time.Sleep(2 * time.Millisecond)
	}

	select {
	case <-bf.Events():
		t.Fatal("unexpected anomaly during warmup")
	case <-time.After(50 * time.Millisecond):
		assert.False(t, bf.Anomalous())
	}
}

func TestWatchBehaviorFingerprintNilSub(t *testing.T) {
	t.Parallel()
	_, err := WatchBehaviorFingerprint(context.Background(), nil, BehaviorFingerprintConfig{}, nil)
	require.Error(t, err)
}

func TestNotifyMessageHandled(t *testing.T) {
	t.Parallel()
	c := &consumer{}
	var got time.Duration
	c.OnMessageHandled(func(elapsed time.Duration) { got = elapsed })
	c.notifyMessageHandled(123 * time.Millisecond)
	assert.Equal(t, 123*time.Millisecond, got)
}
