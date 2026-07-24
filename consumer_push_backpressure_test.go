package nats

import (
	"context"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/gopherust-io/nats/workerpool"
)

func TestWrapHandlerBackpressureNakNonBlocking(t *testing.T) {
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

	// Fill the worker and its single buffer slot.
	pool.Publish(ctx, &natspkg.Msg{Subject: "active"}, false, nil)
	pool.Publish(ctx, &natspkg.Msg{Subject: "buffered"}, false, nil)

	c := &consumer{
		workerPool: pool,
		cfg: RuntimeConsumerConfig{
			WorkerPoolEnabled: true,
			WorkerBufferSize:  1,
		},
		backpressure: BackpressureConfig{Mode: BackpressureNak},
	}

	handler := c.wrapHandler(ctx, func(context.Context, *natspkg.Msg) error { return nil })

	done := make(chan error, 1)
	go func() {
		done <- handler(ctx, &natspkg.Msg{Subject: "overflow"})
	}()

	select {
	case err := <-done:
		require.NoError(t, err)
	case <-time.After(200 * time.Millisecond):
		t.Fatal("BackpressureNak blocked NATS callback thread")
	}
}
