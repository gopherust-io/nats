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

func TestStreamManagerCRUD(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "CRUD")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name:     stream,
		Subjects: []string{prefix + ">"},
		Replicas: 1,
	})
	require.NoError(t, err)

	info, err := client.Streams().StreamInfo(ctx, stream)
	require.NoError(t, err)
	assert.Equal(t, stream, info.Config.Name)

	streams, err := ListStreams(ctx, client.Streams())
	require.NoError(t, err)
	assert.NotEmpty(t, streams)

	require.NoError(t, client.Streams().DeleteStream(ctx, stream))
}

func TestConsumerManagerCRUD(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "CONS")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name:     stream,
		Subjects: []string{prefix + ">"},
	})
	require.NoError(t, err)

	durable := uniqueDurable(t, "cons")
	_, err = client.Consumers().CreateOrUpdateConsumer(ctx, stream, DurableConsumerConfig{
		Durable:       durable,
		FilterSubject: prefix + ">",
		MaxAckPending: 100,
		AckWait:       10 * time.Second,
	})
	require.NoError(t, err)

	info, err := client.Consumers().ConsumerInfo(ctx, stream, durable)
	require.NoError(t, err)
	assert.Equal(t, durable, info.Name)

	consumers, err := client.Consumers().ListConsumers(ctx, stream)
	require.NoError(t, err)
	assert.Len(t, consumers, 1)

	require.NoError(t, client.Consumers().DeleteConsumer(ctx, stream, durable))
}

func TestPublishSubscribePush(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "PSPUSH")
	prefix := streamSubjectPrefix(stream)
	subject := prefix + "events"
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name:     stream,
		Subjects: []string{prefix + ">"},
	})
	require.NoError(t, err)

	var received atomic.Int32
	done := make(chan struct{})
	var closeOnce sync.Once
	sub, err := client.Consumer().Subscribe(ctx, subject, func(c context.Context, msg *natspkg.Msg) error {
		received.Add(1)
		closeOnce.Do(func() { close(done) })
		return nil
	}, natspkg.BindStream(stream))
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Drain() })

	infoBefore, err := client.Streams().StreamInfo(ctx, stream)
	require.NoError(t, err)

	require.NoError(t, client.Publisher().PublishMessage(ctx, subject, Message{
		Data:        map[string]string{"id": "1"},
		MessageType: JSON,
	}))

	select {
	case <-done:
	case <-time.After(testWaitShort):
		t.Fatal("timeout waiting for message")
	}

	infoAfter, err := client.Streams().StreamInfo(ctx, stream)
	require.NoError(t, err)
	assert.Equal(t, infoBefore.State.Msgs+1, infoAfter.State.Msgs)
	assert.GreaterOrEqual(t, received.Load(), int32(1))
}

func TestPullConsumerFetch(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "PULL")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name:     stream,
		Subjects: []string{prefix + ">"},
	})
	require.NoError(t, err)

	durable := uniqueDurable(t, "pull")
	_, err = client.Consumers().CreateOrUpdateConsumer(ctx, stream, DurableConsumerConfig{
		Durable:       durable,
		FilterSubject: prefix + ">",
	})
	require.NoError(t, err)

	require.NoError(t, client.Publisher().PublishMessage(ctx, prefix+"events", Message{
		Data:        map[string]string{"id": "pull-1"},
		MessageType: JSON,
	}))

	pull, err := client.Consumer().Pull(stream, durable)
	require.NoError(t, err)

	var got bool
	deadline := time.Now().Add(testWaitShort)
	for time.Now().Before(deadline) {
		msgs, err := pull.Fetch(ctx, 1, WithFetchMaxWait(100*time.Millisecond))
		if err != nil {
			continue
		}
		if len(msgs) > 0 {
			got = true
			break
		}
	}
	assert.True(t, got)
}

func TestReplayResetConsumer(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "REPLAY")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name:     stream,
		Subjects: []string{prefix + ">"},
	})
	require.NoError(t, err)

	durable := uniqueDurable(t, "replay")
	_, err = client.Consumers().CreateOrUpdateConsumer(ctx, stream, DurableConsumerConfig{
		Durable:       durable,
		FilterSubject: prefix + ">",
		MaxAckPending: 777,
		AckWait:       40 * time.Second,
	})
	require.NoError(t, err)

	_, err = client.Replay().ResetConsumer(ctx, stream, durable,
		FromSeq(1), WithReplayPolicy(ReplayInstant))
	require.NoError(t, err)

	info, err := client.Consumers().ConsumerInfo(ctx, stream, durable)
	require.NoError(t, err)
	assert.Equal(t, 777, info.Config.MaxAckPending)
	assert.Equal(t, 40*time.Second, info.Config.AckWait)
	assert.Equal(t, prefix+">", info.Config.FilterSubject)
	assert.Equal(t, DeliverByStartSequence, info.Config.DeliverPolicy)
}

func TestReplayCreateReplayConsumerAndPeek(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "REPLAYPEEK")
	prefix := streamSubjectPrefix(stream)
	subject := prefix + "events"
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1, Storage: MemoryStorage,
	})
	require.NoError(t, err)

	source := uniqueDurable(t, "live")
	_, err = client.Consumers().CreateOrUpdateConsumer(ctx, stream, DurableConsumerConfig{
		Durable:          source,
		FilterSubject:    prefix + ">",
		MaxAckPending:    500,
		DeliverPolicy:    DeliverNew,
		HasDeliverPolicy: true,
	})
	require.NoError(t, err)

	require.NoError(t, client.Publisher().PublishJSON(ctx, subject, map[string]int{"n": 1}))
	require.NoError(t, client.Publisher().PublishJSON(ctx, subject, map[string]int{"n": 2}))

	last, err := client.Replay().GetLastMsgForSubject(ctx, stream, subject)
	require.NoError(t, err)
	require.NotNil(t, last)

	first, err := client.Replay().GetMsg(ctx, stream, 1)
	require.NoError(t, err)
	require.NotNil(t, first)

	next, err := client.Replay().GetNextMsgAfter(ctx, stream, 1)
	require.NoError(t, err)
	require.NotNil(t, next)

	side, err := client.Replay().CreateReplayConsumer(ctx, stream, source,
		FromSeq(1), WithReplayPolicy(ReplayInstant))
	require.NoError(t, err)
	assert.NotEqual(t, source, side.Durable)

	live, err := client.Consumers().ConsumerInfo(ctx, stream, source)
	require.NoError(t, err)
	assert.Equal(t, DeliverNew, live.Config.DeliverPolicy)
	assert.Equal(t, 500, live.Config.MaxAckPending)

	sideInfo, err := client.Consumers().ConsumerInfo(ctx, stream, side.Durable)
	require.NoError(t, err)
	assert.Equal(t, 500, sideInfo.Config.MaxAckPending)
	assert.Equal(t, DeliverByStartSequence, sideInfo.Config.DeliverPolicy)

	assert.Equal(t, uint64(1), first.Sequence)
	assert.Equal(t, uint64(2), next.Sequence)

	rangeMsgs, truncated, err := client.Replay().GetMsgRange(ctx, stream, 1, 2)
	require.NoError(t, err)
	assert.False(t, truncated)
	require.Len(t, rangeMsgs, 2)
}
