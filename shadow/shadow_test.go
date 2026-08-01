package shadow

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

	"github.com/gopherust-io/nats/internal/bytesconv"
)

type memRecorder struct {
	mu     sync.Mutex
	events []struct{ detail, subject, err string }
}

func (r *memRecorder) RecordShadow(detail, subject, err string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.events = append(r.events, struct{ detail, subject, err string }{detail, subject, err})
}

func (r *memRecorder) snapshot() []struct{ detail, subject, err string } {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]struct{ detail, subject, err string }, len(r.events))
	copy(out, r.events)
	return out
}

func TestWithPrimaryDrivesFate(t *testing.T) {
	t.Parallel()
	want := errors.New("primary fail")
	var shadowCalled atomic.Bool
	h := With(Config{SampleRate: 1}, func(_ context.Context, _ *natspkg.Msg) error {
		return want
	}, func(_ context.Context, _ *natspkg.Msg) error {
		shadowCalled.Store(true)

		return nil
	})

	err := h(context.Background(), &natspkg.Msg{Subject: "orders.x", Data: bytesconv.StringToBytes("1")})
	require.ErrorIs(t, err, want)
	require.Eventually(t, shadowCalled.Load, time.Second, 5*time.Millisecond)
}

func TestWithMismatchRecorded(t *testing.T) {
	t.Parallel()
	rec := &memRecorder{}
	h := With(Config{Recorder: rec, SampleRate: 1}, func(_ context.Context, _ *natspkg.Msg) error {
		return nil
	}, func(_ context.Context, _ *natspkg.Msg) error {
		return errors.New("shadow diverged")
	})

	require.NoError(t, h(context.Background(), &natspkg.Msg{Subject: "a", Data: bytesconv.StringToBytes("x")}))
	require.Eventually(t, func() bool {
		for _, ev := range rec.snapshot() {
			if ev.detail == "shadow_mismatch" {
				return true
			}
		}
		return false
	}, time.Second, 5*time.Millisecond)
}

func TestWithPanicRecovered(t *testing.T) {
	t.Parallel()
	rec := &memRecorder{}
	h := With(Config{Recorder: rec, SampleRate: 1}, func(_ context.Context, _ *natspkg.Msg) error {
		return nil
	}, func(_ context.Context, _ *natspkg.Msg) error {
		panic("boom")
	})

	require.NoError(t, h(context.Background(), &natspkg.Msg{Subject: "a", Data: bytesconv.StringToBytes("x")}))
	require.Eventually(t, func() bool {
		evs := rec.snapshot()
		return len(evs) > 0 && evs[0].detail == "shadow_error"
	}, time.Second, 5*time.Millisecond)
}

func TestCloneMsgOmitsReply(t *testing.T) {
	t.Parallel()
	orig := &natspkg.Msg{
		Subject: "s",
		Data:    bytesconv.StringToBytes("hi"),
		Reply:   "_INBOX.ack",
		Header:  natspkg.Header{"K": []string{"v"}},
	}
	clone := cloneMsg(orig)
	require.NotNil(t, clone)
	assert.Empty(t, clone.Reply)
	assert.Equal(t, orig.Subject, clone.Subject)
	assert.Equal(t, orig.Data, clone.Data)
	assert.Equal(t, "v", clone.Header.Get("K"))
	clone.Data[0] = 'x'
	assert.Equal(t, byte('h'), orig.Data[0])
}

func TestWithNilShadowPassthrough(t *testing.T) {
	t.Parallel()
	called := false
	h := With(Config{}, func(_ context.Context, _ *natspkg.Msg) error {
		called = true

		return nil
	}, nil)
	require.NoError(t, h(context.Background(), &natspkg.Msg{}))
	assert.True(t, called)
}
