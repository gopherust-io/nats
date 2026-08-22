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

func TestPullV2FetchHonorsContextCancel(t *testing.T) {
	t.Parallel()
	client, base := testClient(t)
	stream := uniqueStream(t, "PULLCTX")
	prefix := streamSubjectPrefix(stream)
	durable := uniqueDurable(t, "pullctx")

	_, err := client.Streams().CreateOrUpdateStream(base, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage, Replicas: 1,
	})
	require.NoError(t, err)
	_, err = client.Consumers().CreateOrUpdateConsumer(base, stream, DurableConsumerConfig{
		Durable: durable, FilterSubject: prefix + ">",
	})
	require.NoError(t, err)

	pull, err := client.Consumer().Pull(stream, durable)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(base)
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	start := time.Now()
	_, err = pull.Fetch(ctx, 1, WithFetchMaxWait(5*time.Second))
	elapsed := time.Since(start)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled), "got %v", err)
	assert.Less(t, elapsed, 2*time.Second, "fetch should abort on cancel, not wait full maxWait")
}

func TestPullV2ProcessExitsOnCancel(t *testing.T) {
	t.Parallel()
	client, base := testClient(t)
	stream := uniqueStream(t, "PROCCX")
	prefix := streamSubjectPrefix(stream)
	durable := uniqueDurable(t, "proccx")

	_, err := client.Streams().CreateOrUpdateStream(base, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage, Replicas: 1,
	})
	require.NoError(t, err)
	_, err = client.Consumers().CreateOrUpdateConsumer(base, stream, DurableConsumerConfig{
		Durable: durable, FilterSubject: prefix + ">",
	})
	require.NoError(t, err)

	pull, err := client.Consumer().Pull(stream, durable)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(base)
	errCh := make(chan error, 1)
	go func() {
		errCh <- pull.Process(ctx, func(context.Context, *natspkg.Msg) error {
			return nil
		}, WithFetchBatch(1), WithProcessMaxWait(2*time.Second))
	}()

	time.Sleep(80 * time.Millisecond)
	cancel()

	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled), "got %v", err)
	case <-time.After(3 * time.Second):
		t.Fatal("Process did not return after cancel")
	}
}

func TestPublishHonorsCanceledContext(t *testing.T) {
	t.Parallel()
	client, base := testClient(t)
	stream := uniqueStream(t, "PUBCTX")
	prefix := streamSubjectPrefix(stream)

	_, err := client.Streams().CreateOrUpdateStream(base, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage, Replicas: 1,
	})
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(base)
	cancel()

	err = client.Publisher().PublishBytes(ctx, prefix+"x", []byte("nope"))
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled), "got %v", err)

	_, err = client.PublishRaw(ctx, prefix+"y", []byte("nope"), nil)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled), "got %v", err)
}

func TestProcessBatchWithPoolWaitHonorsCancel(t *testing.T) {
	t.Parallel()

	client, base := testClientWithOptions(t, func(cfg *Config) {
		cfg.RuntimeConsumer.WorkerPoolEnabled = true
		cfg.RuntimeConsumer.WorkerPoolSize = 1
		cfg.RuntimeConsumer.WorkerBufferSize = 4
		cfg.Backpressure.Mode = BackpressureBlock
	})
	stream := uniqueStream(t, "POOLCX")
	prefix := streamSubjectPrefix(stream)
	durable := uniqueDurable(t, "poolcx")

	_, err := client.Streams().CreateOrUpdateStream(base, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage, Replicas: 1,
	})
	require.NoError(t, err)
	_, err = client.Consumers().CreateOrUpdateConsumer(base, stream, DurableConsumerConfig{
		Durable: durable, FilterSubject: prefix + ">", MaxAckPending: 100,
	})
	require.NoError(t, err)

	require.NoError(t, client.Publisher().PublishBytes(base, prefix+"x", []byte("1")))
	require.NoError(t, client.Publisher().PublishBytes(base, prefix+"x", []byte("2")))

	pull, err := client.Consumer().Pull(stream, durable)
	require.NoError(t, err)

	gate := make(chan struct{})
	var started atomic.Int32
	ctx, cancel := context.WithCancel(base)

	var wg sync.WaitGroup
	wg.Add(1)
	errCh := make(chan error, 1)
	go func() {
		defer wg.Done()
		errCh <- pull.Process(ctx, func(context.Context, *natspkg.Msg) error {
			started.Add(1)
			<-gate
			return nil
		}, WithFetchBatch(2), WithProcessMaxWait(500*time.Millisecond))
	}()

	deadline := time.Now().Add(3 * time.Second)
	for started.Load() < 1 && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}
	require.GreaterOrEqual(t, started.Load(), int32(1))

	cancel()
	select {
	case err := <-errCh:
		require.Error(t, err)
		assert.True(t, errors.Is(err, context.Canceled), "got %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("Process hung on pool wait after cancel")
	}
	close(gate)
	wg.Wait()
}
