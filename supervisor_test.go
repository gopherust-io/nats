package nats

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type fakeSub struct {
	subject string
	valid   atomic.Bool
}

func (f *fakeSub) Unsubscribe() error                           { return nil }
func (f *fakeSub) Drain() error                                 { return nil }
func (f *fakeSub) IsValid() bool                                { return f.valid.Load() }
func (f *fakeSub) Subject() string                              { return f.subject }
func (f *fakeSub) SetPendingLimits(_, _ int) error              { return nil }
func (f *fakeSub) ConsumerInfo() (*natspkg.ConsumerInfo, error) { return nil, nil }
func (f *fakeSub) Type() natspkg.SubscriptionType               { return natspkg.AsyncSubscription }

func TestSuperviseResubscribesWhenInvalid(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var calls atomic.Int32
	first := &fakeSub{subject: "orders.>"}
	first.valid.Store(true)
	second := &fakeSub{subject: "orders.>"}
	second.valid.Store(true)

	events := make(chan SupervisorEvent, 8)
	sub, err := Supervise(ctx, SupervisorConfig{
		CheckInterval:  20 * time.Millisecond,
		InitialBackoff: time.Millisecond,
		MaxRetries:     5,
		OnEvent: func(ev SupervisorEvent) {
			select {
			case events <- ev:
			default:
			}
		},
	}, nil, func(_ context.Context) (Subscription, error) {
		n := calls.Add(1)
		if n == 1 {
			return first, nil
		}

		return second, nil
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Stop() })

	first.valid.Store(false)

	require.Eventually(t, func() bool {
		return calls.Load() >= 2
	}, time.Second, 10*time.Millisecond)

	assert.True(t, sub.IsValid())
	assert.Equal(t, "orders.>", sub.Subject())

	ss, ok := sub.(*supervisedSubscription)
	require.True(t, ok)
	_ = ss.Events()
	require.NoError(t, ss.Drain())
	require.NoError(t, ss.SetPendingLimits(10, 1024))
	_, _ = ss.ConsumerInfo()
	assert.Equal(t, natspkg.AsyncSubscription, ss.Type())

	sawResub := false
	deadline := time.After(500 * time.Millisecond)
	for !sawResub {
		select {
		case ev := <-events:
			if ev.Kind == SupervisorResubscribed {
				sawResub = true
			}
		case <-deadline:
			t.Fatal("expected SupervisorResubscribed event")
		}
	}
}

func TestSuperviseGiveUp(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var calls atomic.Int32
	first := &fakeSub{subject: "x"}
	first.valid.Store(true)

	gaveUp := make(chan struct{})
	sub, err := Supervise(ctx, SupervisorConfig{
		CheckInterval:  10 * time.Millisecond,
		InitialBackoff: time.Millisecond,
		MaxRetries:     2,
		OnEvent: func(ev SupervisorEvent) {
			if ev.Kind == SupervisorGiveUp {
				close(gaveUp)
			}
		},
	}, nil, func(_ context.Context) (Subscription, error) {
		if calls.Add(1) == 1 {
			return first, nil
		}

		return nil, errors.New("resubscribe blocked")
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Stop() })

	first.valid.Store(false)

	select {
	case <-gaveUp:
	case <-time.After(2 * time.Second):
		t.Fatal("expected give up")
	}
}

func TestSupervisePullProcessRetries(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	var calls atomic.Int32
	events := make([]SupervisorEvent, 0, 4)
	var mu sync.Mutex

	err := SupervisePullProcess(ctx, SupervisorConfig{
		MaxRetries:     3,
		InitialBackoff: time.Millisecond,
		OnEvent: func(ev SupervisorEvent) {
			mu.Lock()
			events = append(events, ev)
			mu.Unlock()
		},
	}, nil, func(_ context.Context) error {
		if calls.Add(1) < 3 {
			return errors.New("transient")
		}
		cancel()

		return context.Canceled
	})
	require.ErrorIs(t, err, context.Canceled)
	assert.GreaterOrEqual(t, calls.Load(), int32(3))

	mu.Lock()
	defer mu.Unlock()
	require.NotEmpty(t, events)
	assert.Equal(t, SupervisorResubscribed, events[0].Kind)
}

func TestSuperviseNilSubscribe(t *testing.T) {
	t.Parallel()
	_, err := Supervise(context.Background(), SupervisorConfig{}, nil, nil)
	require.Error(t, err)
}

func TestSupervisePullNilRun(t *testing.T) {
	t.Parallel()
	err := SupervisePullProcess(context.Background(), SupervisorConfig{}, nil, nil)
	require.Error(t, err)
}
