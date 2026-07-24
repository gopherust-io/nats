package nats

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueueSubscribeBound(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "BOUND_Q")
	prefix := streamSubjectPrefix(stream)
	subject := prefix + ">"
	publishSubject := prefix + "events"
	durable := uniqueDurable(t, "boundq-processor")

	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{subject}, Replicas: 1,
	})
	require.NoError(t, err)

	var received atomic.Int32
	done := make(chan struct{})
	var once sync.Once
	_, err = client.Consumer().QueueSubscribeBound(ctx, stream, durable, uniqueQueue(t, "boundq-workers"), subject,
		func(c context.Context, msg *natspkg.Msg) error {
			received.Add(1)
			once.Do(func() { close(done) })
			return nil
		})
	require.NoError(t, err)

	require.NoError(t, client.Publisher().PublishJSON(ctx, publishSubject, map[string]string{"id": "1"}))
	select {
	case <-done:
	case <-time.After(testWaitShort):
		t.Fatal("timeout")
	}
	assert.GreaterOrEqual(t, received.Load(), int32(1))
}

func TestSubscribeBound(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "BOUND")
	prefix := streamSubjectPrefix(stream)
	subscribeSubject := prefix + ">"
	publishSubject := prefix + "events"
	durable := uniqueDurable(t, "bound-processor")

	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{subscribeSubject}, Replicas: 1,
	})
	require.NoError(t, err)

	done := make(chan struct{})
	_, err = client.Consumer().SubscribeBound(ctx, stream, durable, subscribeSubject,
		func(c context.Context, msg *natspkg.Msg) error {
			close(done)
			return nil
		})
	require.NoError(t, err)

	require.NoError(t, client.Publisher().PublishJSON(ctx, publishSubject, map[string]string{"id": "1"}))
	select {
	case <-done:
	case <-time.After(testWaitShort):
		t.Fatal("timeout")
	}
}

func TestPublishWithMsgIDDedup(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "DEDUP")
	prefix := streamSubjectPrefix(stream)
	publishSubject := prefix + "events"
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1,
		DuplicateWindow: time.Minute,
	})
	require.NoError(t, err)

	msgID := uniqueDurable(t, "dedup-msg")
	for range 2 {
		require.NoError(t, client.Publisher().PublishWithMsgID(ctx, publishSubject, msgID, Message{
			Data: map[string]string{"id": "1"}, MessageType: JSON,
		}))
	}
	info, err := client.Streams().StreamInfo(ctx, stream)
	require.NoError(t, err)
	assert.Equal(t, uint64(1), info.State.Msgs)
}

func TestPullConsumerFilterSubjects(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "FILTER_SUB")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1,
	})
	require.NoError(t, err)

	durable := uniqueDurable(t, "filtersub-pull")
	subjectA := prefix + "a"
	subjectB := prefix + "b"
	_, err = client.Consumers().CreateOrUpdateConsumer(ctx, stream, DurableConsumerConfig{
		Durable:        durable,
		FilterSubjects: []string{subjectA, subjectB},
	})
	require.NoError(t, err)

	require.NoError(t, client.Publisher().PublishJSON(ctx, subjectA, map[string]string{"k": "a"}))

	pull, err := client.Consumer().Pull(stream, durable)
	require.NoError(t, err)
	msgs, err := pull.Fetch(ctx, 1, WithFetchMaxWait(200*time.Millisecond))
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, subjectA, msgs[0].Subject)
}

func TestConnectionHealthCheck(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	require.NoError(t, client.Connector().HealthCheck(ctx))
	assert.True(t, client.Connector().IsConnected())
}

func TestContextDrainOnCancel(t *testing.T) {
	t.Parallel()
	client, baseCtx := testClient(t)
	ctx, cancel := context.WithCancel(baseCtx)
	stream := uniqueStream(t, "DRAIN")
	prefix := streamSubjectPrefix(stream)
	subscribeSubject := prefix + ">"
	durable := uniqueDurable(t, "drain-processor")

	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{subscribeSubject}, Replicas: 1,
	})
	require.NoError(t, err)

	sub, err := client.Consumer().QueueSubscribeBound(ctx, stream, durable, uniqueQueue(t, "drain-workers"), subscribeSubject,
		func(c context.Context, msg *natspkg.Msg) error { return nil })
	require.NoError(t, err)

	cancel()
	require.Eventually(t, func() bool { return !sub.IsValid() }, testWaitShort, testPollFast)
}

func TestWorkerPoolPush(t *testing.T) {
	t.Parallel()
	client, ctx := testClientWithOptions(t, func(cfg *Config) {
		cfg.RuntimeConsumer.WorkerPoolEnabled = true
		cfg.RuntimeConsumer.WorkerPoolSize = 4
		cfg.RuntimeConsumer.WorkerBufferSize = 32
	})
	stream := uniqueStream(t, "POOL")
	prefix := streamSubjectPrefix(stream)
	subscribeSubject := prefix + ">"
	publishSubject := prefix + "events"
	durable := uniqueDurable(t, "pool-processor")

	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{subscribeSubject}, Replicas: 1,
	})
	require.NoError(t, err)

	var count atomic.Int32
	_, err = client.Consumer().QueueSubscribeBound(ctx, stream, durable, uniqueQueue(t, "pool-workers"), subscribeSubject,
		func(c context.Context, msg *natspkg.Msg) error {
			count.Add(1)
			return nil
		})
	require.NoError(t, err)

	for i := range 10 {
		require.NoError(t, client.Publisher().PublishJSON(ctx, publishSubject, map[string]int{"i": i}))
	}
	require.Eventually(t, func() bool { return count.Load() >= 10 }, testWaitShort, testPollFast)
}

func TestConcurrentQueueSubscribe(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "CONC")
	prefix := streamSubjectPrefix(stream)
	subscribeSubject := prefix + ">"
	publishSubject := prefix + "events"
	durable := uniqueDurable(t, "conc-processor")

	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{subscribeSubject}, Replicas: 1,
		Retention: WorkQueuePolicy,
	})
	require.NoError(t, err)

	var count atomic.Int32
	_, err = client.Consumer().QueueSubscribeBound(ctx, stream, durable, uniqueQueue(t, "conc-workers"), subscribeSubject,
		func(c context.Context, msg *natspkg.Msg) error {
			count.Add(1)
			return nil
		})
	require.NoError(t, err)

	const n = 20
	for i := range n {
		require.NoError(t, client.Publisher().PublishJSON(ctx, publishSubject, map[string]int{"i": i}))
	}
	require.Eventually(t, func() bool { return count.Load() >= int32(n) }, testWaitShort, testPollFast)
}
