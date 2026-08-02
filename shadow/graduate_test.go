package shadow

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGraduateRampsAndPromoteReady(t *testing.T) {
	t.Parallel()

	var ready atomic.Bool
	g := NewGraduate(GraduateConfig{
		StartRate:       1,
		MaxRate:         1,
		Window:          2,
		MaxMismatchRate: 0.5,
		OnPromoteReady:  func() { ready.Store(true) },
	}, func(context.Context, *natspkg.Msg) error { return nil },
		func(context.Context, *natspkg.Msg) error { return nil },
	)

	msg := &natspkg.Msg{Subject: "t", Data: []byte("x")}
	require.NoError(t, g.Handler()(context.Background(), msg))
	require.NoError(t, g.Handler()(context.Background(), msg))

	require.Eventually(t, func() bool {
		return g.Status().Phase == PhasePromoteReady && ready.Load()
	}, time.Second, 5*time.Millisecond)
}

func TestGraduatePromote(t *testing.T) {
	t.Parallel()

	g := NewGraduate(GraduateConfig{
		StartRate: 1, MaxRate: 1, Window: 2, MaxMismatchRate: 0.5,
	}, func(context.Context, *natspkg.Msg) error { return nil },
		func(context.Context, *natspkg.Msg) error { return nil },
	)
	msg := &natspkg.Msg{Subject: "t", Data: []byte("x")}
	require.NoError(t, g.Handler()(context.Background(), msg))
	require.NoError(t, g.Handler()(context.Background(), msg))
	require.Eventually(t, func() bool { return g.Status().Phase == PhasePromoteReady }, time.Second, 5*time.Millisecond)

	g.Promote()
	assert.Equal(t, PhaseHolding, g.Status().Phase)
}

func TestGraduateAbortsOnMismatch(t *testing.T) {
	t.Parallel()

	var aborted atomic.Bool
	g := NewGraduate(GraduateConfig{
		StartRate:       1,
		MaxRate:         1,
		Window:          2,
		MaxMismatchRate: 0.01,
		OnAbort:         func(string) { aborted.Store(true) },
	}, func(context.Context, *natspkg.Msg) error { return nil },
		func(context.Context, *natspkg.Msg) error { return assert.AnError },
	)

	msg := &natspkg.Msg{Subject: "t", Data: []byte("x")}
	require.NoError(t, g.Handler()(context.Background(), msg))
	require.NoError(t, g.Handler()(context.Background(), msg))

	require.Eventually(t, func() bool {
		return g.Status().Phase == PhaseAborted && aborted.Load()
	}, time.Second, 5*time.Millisecond)
}
