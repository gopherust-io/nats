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

func TestEvaluateSlowConsumer(t *testing.T) {
	t.Parallel()

	cfg := SlowConsumerConfig{
		PendingThreshold: 100,
		LagThreshold:     50,
		AckPendingRatio:  0.9,
	}

	slow, reasons := EvaluateSlowConsumer(99, 0, 0, 0, cfg)
	assert.False(t, slow)
	assert.Empty(t, reasons)

	slow, reasons = EvaluateSlowConsumer(100, 0, 0, 0, cfg)
	assert.True(t, slow)
	assert.Equal(t, []string{SlowReasonPending}, reasons)

	slow, reasons = EvaluateSlowConsumer(0, 50, 0, 0, cfg)
	assert.True(t, slow)
	assert.Equal(t, []string{SlowReasonLag}, reasons)

	slow, reasons = EvaluateSlowConsumer(0, 0, 90, 100, cfg)
	assert.True(t, slow)
	assert.Equal(t, []string{SlowReasonAckPending}, reasons)

	slow, _ = EvaluateSlowConsumer(0, 0, 89, 100, cfg)
	assert.False(t, slow)

	// MaxAckPending <= 0 disables ack-pending check.
	slow, _ = EvaluateSlowConsumer(0, 0, 1000, 0, cfg)
	assert.False(t, slow)

	slow, reasons = EvaluateSlowConsumer(100, 50, 90, 100, cfg)
	assert.True(t, slow)
	assert.ElementsMatch(t, []string{SlowReasonPending, SlowReasonLag, SlowReasonAckPending}, reasons)
}

func TestConsumerLagMessages(t *testing.T) {
	t.Parallel()
	assert.Equal(t, uint64(0), ConsumerLagMessages(100, 100))
	assert.Equal(t, uint64(0), ConsumerLagMessages(50, 100))
	assert.Equal(t, uint64(42), ConsumerLagMessages(142, 100))
}

type slowInfoSub struct {
	info atomic.Pointer[natspkg.ConsumerInfo]
	fakeSub
}

func (s *slowInfoSub) ConsumerInfo() (*natspkg.ConsumerInfo, error) {
	return s.info.Load(), nil
}

func TestWatchSlowConsumerDetectsSustained(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	sub := &slowInfoSub{}
	sub.valid.Store(true)
	sub.info.Store(&natspkg.ConsumerInfo{
		Stream:     "ORDERS",
		Name:       "worker",
		NumPending: 2000,
		Delivered:  natspkg.SequenceInfo{Stream: 100},
		Config:     natspkg.ConsumerConfig{MaxAckPending: 1000},
	})

	detected := make(chan SlowConsumerEvent, 1)
	sc, err := WatchSlowConsumer(ctx, sub, func(context.Context, string) (uint64, error) {
		return 3100, nil // lag = 3000
	}, SlowConsumerConfig{
		PollInterval:     15 * time.Millisecond,
		SustainFor:       40 * time.Millisecond,
		PendingThreshold: 1000,
		LagThreshold:     1000,
		AckPendingRatio:  0.9,
		CircuitStop:      true,
		OnSlow: func(ev SlowConsumerEvent) {
			select {
			case detected <- ev:
			default:
			}
		},
	}, nil)
	require.NoError(t, err)
	t.Cleanup(sc.Stop)

	select {
	case ev := <-detected:
		assert.Equal(t, "ORDERS", ev.Stream)
		assert.Equal(t, "worker", ev.Durable)
		assert.Contains(t, ev.Reasons, SlowReasonPending)
		assert.Contains(t, ev.Reasons, SlowReasonLag)
		assert.True(t, sc.Slow())
		assert.GreaterOrEqual(t, ev.SustainedFor, 40*time.Millisecond)
	case <-time.After(2 * time.Second):
		t.Fatal("expected slow consumer detection")
	}
}

func TestWatchSlowConsumerClearsBeforeSustain(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	sub := &slowInfoSub{}
	sub.valid.Store(true)
	sub.info.Store(&natspkg.ConsumerInfo{
		Stream:     "ORDERS",
		Name:       "worker",
		NumPending: 2000,
	})

	sc, err := WatchSlowConsumer(ctx, sub, nil, SlowConsumerConfig{
		PollInterval:     20 * time.Millisecond,
		SustainFor:       200 * time.Millisecond,
		PendingThreshold: 1000,
		LagThreshold:     1000,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(sc.Stop)

	time.Sleep(50 * time.Millisecond)
	sub.info.Store(&natspkg.ConsumerInfo{
		Stream:     "ORDERS",
		Name:       "worker",
		NumPending: 10,
	})

	select {
	case <-sc.Events():
		t.Fatal("unexpected slow event when breach cleared before sustain")
	case <-time.After(250 * time.Millisecond):
		assert.False(t, sc.Slow())
	}
}

func TestWatchSlowConsumerNilSub(t *testing.T) {
	t.Parallel()
	_, err := WatchSlowConsumer(context.Background(), nil, nil, SlowConsumerConfig{}, nil)
	require.Error(t, err)
}
