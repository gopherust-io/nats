package workerpool

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const (
	waitShort = time.Second
	pollFast  = time.Millisecond
)

func TestWorkerPoolDispatchesAllMessages(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var processed atomic.Int64

	pool := New(ctx, 2, 4, func(_ context.Context, msg *nats.Msg) error {
		processed.Add(1)
		assert.NotEmpty(t, msg.Subject)
		return nil
	})
	pool.Consume()

	for range 20 {
		pool.Publish(ctx, &nats.Msg{Subject: "test"}, false, nil)
	}

	waitFor(t, func() bool {
		return processed.Load() == 20
	})

	pool.GracefulStop()
	assert.Equal(t, int64(20), processed.Load())
}

func TestWorkerPoolBackpressure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	block := make(chan struct{})

	pool := New(ctx, 1, 1, func(_ context.Context, _ *nats.Msg) error {
		<-block
		return nil
	})
	pool.Consume()

	pool.Publish(ctx, &nats.Msg{Subject: "one"}, false, nil)
	pool.Publish(ctx, &nats.Msg{Subject: "two"}, false, nil)

	waitFor(t, func() bool {
		return pool.QueueDepth() == 1
	})

	close(block)
	waitFor(t, func() bool {
		return pool.QueueDepth() == 0
	})

	pool.GracefulStop()
}

func TestWorkerPoolGracefulStopDrainsInFlight(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var processed atomic.Int64
	var entered atomic.Int64
	release := make(chan struct{})

	pool := New(ctx, 2, 8, func(_ context.Context, _ *nats.Msg) error {
		entered.Add(1)
		<-release
		processed.Add(1)
		return nil
	})
	pool.Consume()

	for range 5 {
		pool.Publish(ctx, &nats.Msg{Subject: "test"}, false, nil)
	}

	waitFor(t, func() bool {
		return entered.Load() >= 2
	})
	close(release)

	pool.GracefulStop()
	assert.Equal(t, int64(5), processed.Load())
	assert.Equal(t, stateStopped, pool.Stats().State)
}

func TestWorkerPoolPublishDuringShutdownNoPanic(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var startedOnce sync.Once
	started := make(chan struct{})
	block := make(chan struct{})

	pool := New(ctx, 1, 2, func(_ context.Context, _ *nats.Msg) error {
		startedOnce.Do(func() { close(started) })
		<-block
		return nil
	})
	pool.Consume()

	pool.Publish(ctx, &nats.Msg{Subject: "test"}, false, nil)
	<-started

	done := make(chan struct{})
	go func() {
		pool.GracefulStop()
		close(done)
	}()

	assert.NotPanics(t, func() {
		pool.Publish(ctx, &nats.Msg{Subject: "late"}, false, nil)
	})

	close(block)
	<-done
}

func TestWorkerPoolInvalidSizeDefaultsToOne(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := New(ctx, 0, 2, func(_ context.Context, _ *nats.Msg) error { return nil })
	require.NotNil(t, pool)
	assert.Equal(t, 1, pool.Stats().Workers)
}

func TestWorkerPoolQueueDepth(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	block := make(chan struct{})

	pool := New(ctx, 1, 4, func(_ context.Context, _ *nats.Msg) error {
		<-block
		return nil
	})
	pool.Consume()

	pool.Publish(ctx, &nats.Msg{Subject: "one"}, false, nil)
	pool.Publish(ctx, &nats.Msg{Subject: "two"}, false, nil)

	waitFor(t, func() bool {
		return pool.QueueDepth() >= 1
	})

	close(block)
	waitFor(t, func() bool {
		return pool.QueueDepth() == 0
	})

	pool.GracefulStop()
}

func TestWorkerPoolApplyFnPath(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var custom atomic.Int64

	pool := New(ctx, 1, 1, func(_ context.Context, _ *nats.Msg) error {
		t.Fatal("register fn should not be called")
		return nil
	})
	pool.Consume()

	customFn := func(_ context.Context, _ *nats.Msg) error {
		custom.Add(1)
		return nil
	}

	pool.Publish(ctx, &nats.Msg{Subject: "custom"}, true, customFn)

	waitFor(t, func() bool {
		return custom.Load() == 1
	})

	pool.GracefulStop()
}

func TestWorkerPoolTryPublishStopped(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := New(ctx, 1, 1, func(_ context.Context, _ *nats.Msg) error { return nil })
	pool.Consume()
	pool.GracefulStop()

	err := pool.TryPublish(ctx, &nats.Msg{Subject: "late"}, false, nil)
	assert.ErrorIs(t, err, ErrPoolStopped)
}

func waitFor(t *testing.T, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(waitShort)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(pollFast)
	}
	t.Fatal("condition not met before timeout")
}

func TestWorkerPoolConcurrentPublish(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	var processed atomic.Int64
	var wg sync.WaitGroup

	pool := New(ctx, 4, 32, func(_ context.Context, _ *nats.Msg) error {
		processed.Add(1)
		return nil
	})
	pool.Consume()

	for range 100 {
		wg.Go(func() {
			pool.Publish(ctx, &nats.Msg{Subject: "parallel"}, false, nil)
		})
	}
	wg.Wait()

	waitFor(t, func() bool {
		return processed.Load() == 100
	})

	pool.GracefulStop()
}

func TestWorkerPoolNegativeBufferDefaultsToZero(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := New(ctx, 1, -1, func(_ context.Context, _ *nats.Msg) error { return nil })
	require.NotNil(t, pool)
	pool.Consume()
	pool.GracefulStop()
}

func TestWorkerPoolGracefulStopIdempotent(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := New(ctx, 1, 1, func(_ context.Context, _ *nats.Msg) error { return nil })
	pool.Consume()
	pool.GracefulStop()
	pool.GracefulStop()
	assert.Equal(t, stateStopped, pool.Stats().State)
}

func TestWorkerPoolStats(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	pool := New(ctx, 2, 4, func(_ context.Context, _ *nats.Msg) error { return nil })
	pool.Consume()
	stats := pool.Stats()
	assert.Equal(t, 2, stats.Workers)
	assert.Equal(t, stateRunning, stats.State)
	pool.GracefulStop()
}
