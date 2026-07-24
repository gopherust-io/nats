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

func TestPullProcessConcurrency(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "PULLCONC")
	prefix := streamSubjectPrefix(stream)
	durable := uniqueDurable(t, "pullconc")

	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1, Retention: WorkQueuePolicy,
	})
	require.NoError(t, err)
	_, err = client.Consumers().CreateOrUpdateConsumer(ctx, stream, DurableConsumerConfig{
		Durable: durable, FilterSubject: prefix + ">", MaxAckPending: 100,
	})
	require.NoError(t, err)

	for i := range 8 {
		require.NoError(t, client.Publisher().PublishJSON(ctx, prefix+"events", map[string]int{"n": i}))
	}

	var processed atomic.Int32
	done := make(chan struct{})
	handler := func(_ context.Context, _ *natspkg.Msg) error {
		time.Sleep(20 * time.Millisecond)
		processed.Add(1)

		return nil
	}

	go func() {
		pull, err := client.Consumer().Pull(stream, durable)
		require.NoError(t, err)

		runCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
		defer cancel()

		err = pull.Process(runCtx, handler,
			WithFetchBatch(8),
			WithProcessMaxWait(500*time.Millisecond),
			WithProcessConcurrency(4),
		)
		if err != nil && runCtx.Err() == nil {
			t.Errorf("pull process: %v", err)
		}
		close(done)
	}()

	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("pull concurrency timed out")
	}

	assert.GreaterOrEqual(t, processed.Load(), int32(8))
}
