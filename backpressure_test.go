package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gopherust-io/nats/workerpool"
)

func TestHandlePoolBackpressureNoPool(t *testing.T) {
	c := &consumer{}
	err := c.handlePoolBackpressure(context.Background(), &natspkg.Msg{Subject: "test"})
	assert.NoError(t, err)
}

func TestHandlePoolBackpressureBelowCapacity(t *testing.T) {
	ctx := context.Background()
	pool := workerpool.New(ctx, 1, 4, func(context.Context, *natspkg.Msg) error { return nil })
	pool.Consume()
	t.Cleanup(pool.GracefulStop)

	c := &consumer{
		workerPool:   pool,
		cfg:          RuntimeConsumerConfig{WorkerBufferSize: 4},
		backpressure: BackpressureConfig{Mode: BackpressureNak},
	}
	err := c.handlePoolBackpressure(ctx, &natspkg.Msg{Subject: "test"})
	assert.NoError(t, err)
}

func TestHandlePoolBackpressureBlockMode(t *testing.T) {
	ctx := context.Background()
	block := make(chan struct{})
	pool := workerpool.New(ctx, 1, 1, func(context.Context, *natspkg.Msg) error {
		<-block
		return nil
	})
	pool.Consume()
	t.Cleanup(func() {
		close(block)
		pool.GracefulStop()
	})

	pool.Publish(ctx, &natspkg.Msg{Subject: "fill"}, false, nil)

	c := &consumer{
		workerPool:   pool,
		cfg:          RuntimeConsumerConfig{WorkerBufferSize: 1},
		backpressure: BackpressureConfig{Mode: BackpressureBlock},
	}
	err := c.handlePoolBackpressure(ctx, &natspkg.Msg{Subject: "test"})
	assert.ErrorIs(t, err, ErrPoolFull)
}

func TestHandlePoolBackpressureDropMode(t *testing.T) {
	ctx := context.Background()
	block := make(chan struct{})
	pool := workerpool.New(ctx, 1, 1, func(context.Context, *natspkg.Msg) error {
		<-block
		return nil
	})
	pool.Consume()
	t.Cleanup(func() {
		close(block)
		pool.GracefulStop()
	})

	pool.Publish(ctx, &natspkg.Msg{Subject: "fill"}, false, nil)

	c := &consumer{
		workerPool:   pool,
		cfg:          RuntimeConsumerConfig{WorkerBufferSize: 1},
		backpressure: BackpressureConfig{Mode: BackpressureDrop},
	}
	err := c.handlePoolBackpressure(ctx, &natspkg.Msg{Subject: "test"})
	assert.ErrorIs(t, err, ErrBackpressureHandled)
}

func TestHandlePoolBackpressureNakAndTerm(t *testing.T) {
	ctx := context.Background()
	block := make(chan struct{})
	pool := workerpool.New(ctx, 1, 1, func(context.Context, *natspkg.Msg) error {
		<-block
		return nil
	})
	pool.Consume()
	t.Cleanup(func() {
		close(block)
		pool.GracefulStop()
	})
	// One in-flight (worker blocked) + one queued so QueueDepth >= capacity.
	pool.Publish(ctx, &natspkg.Msg{Subject: "fill-1"}, false, nil)
	pool.Publish(ctx, &natspkg.Msg{Subject: "fill-2"}, false, nil)
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) && pool.QueueDepth() < 1 {
		time.Sleep(time.Millisecond)
	}
	require.GreaterOrEqual(t, pool.QueueDepth(), 1)

	metrics := newClientMetrics(ctx, MetricsConfig{AllowMetrics: true, Prefix: "bp"})
	c := &consumer{
		workerPool:   pool,
		cfg:          RuntimeConsumerConfig{WorkerBufferSize: 1},
		backpressure: BackpressureConfig{Mode: BackpressureNak},
		metrics:      metrics,
	}
	err := c.handlePoolBackpressure(ctx, &natspkg.Msg{Subject: "nak"})
	// Without a JetStream reply, Nak fails after the mode branch records metrics.
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrPoolFull))

	c.backpressure.Mode = BackpressureTerm
	err = c.handlePoolBackpressure(ctx, &natspkg.Msg{Subject: "term"})
	require.Error(t, err)
	assert.False(t, errors.Is(err, ErrPoolFull))
}

func TestHandlePoolBackpressureDefaultMode(t *testing.T) {
	ctx := context.Background()
	block := make(chan struct{})
	pool := workerpool.New(ctx, 1, 1, func(context.Context, *natspkg.Msg) error {
		<-block
		return nil
	})
	pool.Consume()
	t.Cleanup(func() {
		close(block)
		pool.GracefulStop()
	})

	pool.Publish(ctx, &natspkg.Msg{Subject: "fill"}, false, nil)

	c := &consumer{
		workerPool:   pool,
		cfg:          RuntimeConsumerConfig{WorkerBufferSize: 1},
		backpressure: BackpressureConfig{},
	}
	err := c.handlePoolBackpressure(ctx, &natspkg.Msg{Subject: "test"})
	assert.ErrorIs(t, err, ErrPoolFull)
}
