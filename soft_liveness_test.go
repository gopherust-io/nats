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

type softInfoSub struct {
	fakeSub
	pending atomic.Uint64
}

func (s *softInfoSub) ConsumerInfo() (*natspkg.ConsumerInfo, error) {
	return &natspkg.ConsumerInfo{NumPending: s.pending.Load()}, nil
}

func TestSoftLivenessDetectsStall(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	sub := &softInfoSub{}
	sub.valid.Store(true)
	sub.pending.Store(1)

	activity := NewProcessActivity()
	// Backdate so StallAfter is already satisfied.
	activity.lastUnixNano.Store(time.Now().Add(-time.Minute).UnixNano())

	stalled := make(chan SoftLivenessEvent, 1)
	sl, err := WatchSoftLiveness(ctx, sub, activity, SoftLivenessConfig{
		PollInterval:  15 * time.Millisecond,
		StallAfter:    50 * time.Millisecond,
		RisingWindows: 2,
		CircuitStop:   true,
		OnStall: func(ev SoftLivenessEvent) {
			select {
			case stalled <- ev:
			default:
			}
		},
	}, nil)
	require.NoError(t, err)
	t.Cleanup(sl.Stop)

	// Rising pending across polls.
	go func() {
		time.Sleep(20 * time.Millisecond)
		sub.pending.Store(5)
		time.Sleep(20 * time.Millisecond)
		sub.pending.Store(12)
		time.Sleep(20 * time.Millisecond)
		sub.pending.Store(20)
	}()

	select {
	case ev := <-stalled:
		require.ErrorIs(t, ev.Err, ErrConsumerStall)
		assert.Positive(t, ev.NumPending)
		assert.True(t, sl.Stalled())
	case <-time.After(2 * time.Second):
		t.Fatal("expected soft liveness stall")
	}

	assert.NotNil(t, sl.Activity())
	select {
	case <-sl.Events():
	default:
	}
}

func TestSoftLivenessNoStallWhenActive(t *testing.T) {
	t.Parallel()

	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)

	sub := &softInfoSub{}
	sub.valid.Store(true)
	sub.pending.Store(10)

	activity := NewProcessActivity()
	sl, err := WatchSoftLiveness(ctx, sub, activity, SoftLivenessConfig{
		PollInterval:  20 * time.Millisecond,
		StallAfter:    200 * time.Millisecond,
		RisingWindows: 2,
	}, nil)
	require.NoError(t, err)
	t.Cleanup(sl.Stop)

	done := time.After(150 * time.Millisecond)
	for {
		select {
		case <-done:
			assert.False(t, sl.Stalled())

			return
		case <-sl.Events():
			t.Fatal("unexpected stall while activity is fresh")
		case <-time.After(25 * time.Millisecond):
			activity.Touch()
			sub.pending.Add(1)
		}
	}
}

func TestSoftLivenessNilSub(t *testing.T) {
	t.Parallel()
	_, err := WatchSoftLiveness(context.Background(), nil, nil, SoftLivenessConfig{}, nil)
	require.Error(t, err)
}

func TestProcessActivityTouch(t *testing.T) {
	t.Parallel()
	a := NewProcessActivity()
	before := a.LastSuccess()
	time.Sleep(2 * time.Millisecond)
	a.Touch()
	assert.True(t, a.LastSuccess().After(before) || a.LastSuccess().Equal(before))
}

func TestNotifyProcessSuccess(t *testing.T) {
	t.Parallel()
	c := &consumer{}
	called := false
	c.OnProcessSuccess(func() { called = true })
	c.notifyProcessSuccess()
	assert.True(t, called)
}
