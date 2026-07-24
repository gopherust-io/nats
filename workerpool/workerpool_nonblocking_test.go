package workerpool

import (
	"context"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTryPublishNonBlocking(t *testing.T) {
	ctx := context.Background()
	block := make(chan struct{})
	entered := make(chan struct{}, 1)
	pool := New(ctx, 1, 1, func(context.Context, *nats.Msg) error {
		select {
		case entered <- struct{}{}:
		default:
		}
		<-block
		return nil
	})
	pool.Consume()
	t.Cleanup(func() {
		close(block)
		pool.GracefulStop()
	})

	// Occupy the single worker.
	require.NoError(t, pool.TryPublish(ctx, &nats.Msg{Subject: "fill-worker"}, false, nil))
	select {
	case <-entered:
	case <-time.After(time.Second):
		t.Fatal("worker did not start")
	}

	// Fill the buffered channel (capacity 1).
	require.NoError(t, pool.TryPublish(ctx, &nats.Msg{Subject: "fill-buf"}, false, nil))

	accepted, err := pool.TryPublishNonBlocking(ctx, &nats.Msg{Subject: "overflow"}, false, nil)
	require.NoError(t, err)
	assert.False(t, accepted)
}
