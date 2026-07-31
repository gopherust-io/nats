package nats

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

type dlqTestPublisher struct {
	subjects []string
	msgs     []Message
	mu       sync.Mutex
}

func (p *dlqTestPublisher) PublishMessage(_ context.Context, subject string, msg Message) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subjects = append(p.subjects, subject)
	p.msgs = append(p.msgs, msg)

	return nil
}

func (p *dlqTestPublisher) PublishProto(context.Context, string, proto.Message) error {
	return errors.New("unused")
}

func (p *dlqTestPublisher) PublishJSON(context.Context, string, any) error {
	return errors.New("unused")
}

func (p *dlqTestPublisher) PublishMsgPack(context.Context, string, any) error {
	return errors.New("unused")
}

func (p *dlqTestPublisher) PublishBytes(context.Context, string, []byte) error {
	return errors.New("unused")
}

func (p *dlqTestPublisher) PublishBytesWithMsgID(context.Context, string, string, []byte) error {
	return errors.New("unused")
}

func (p *dlqTestPublisher) PublishWithMsgID(context.Context, string, string, Message) error {
	return errors.New("unused")
}

func (p *dlqTestPublisher) PublishAsync(context.Context, string, Message) (PubAckFuture, error) {
	return nil, errors.New("unused")
}

func (p *dlqTestPublisher) PublishAsyncBytes(context.Context, string, []byte) (PubAckFuture, error) {
	return nil, errors.New("unused")
}

func (p *dlqTestPublisher) PublishAsyncComplete(context.Context) error {
	return errors.New("unused")
}

func TestProcessMessageDLQRoutedSkipsAckNak(t *testing.T) {
	t.Parallel()
	c := &consumer{}
	err := c.processMessage(context.Background(), &natspkg.Msg{Subject: "a", Data: bytesconv.StringToBytes("x")},
		func(_ context.Context, _ *natspkg.Msg) error {
			return ErrDLQRouted
		})
	require.NoError(t, err)
}

func TestDLQIntegrationRoutesAndTerms(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)

	stream := uniqueStream(t, "DLQ")
	dlqStream := uniqueStream(t, "DLQDEST")
	prefix := streamSubjectPrefix(stream)
	dlqPrefix := streamSubjectPrefix(dlqStream)
	dlqSubject := dlqPrefix + "poison"

	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage, Retention: WorkQueuePolicy,
	})
	require.NoError(t, err)
	_, err = client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: dlqStream, Subjects: []string{dlqPrefix + ">"}, Storage: MemoryStorage,
	})
	require.NoError(t, err)

	durable := uniqueDurable(t, "dlq")

	gotDLQ := make(chan *natspkg.Msg, 1)
	dlqSub, err := client.Consumer().SubscribeBound(ctx, dlqStream, uniqueDurable(t, "dlqread"), dlqPrefix+">",
		func(_ context.Context, msg *natspkg.Msg) error {
			gotDLQ <- msg

			return nil
		})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dlqSub.Unsubscribe() })

	handler := WithDLQ(DLQConfig{
		Publisher:  client.Publisher(),
		Subject:    dlqSubject,
		MaxDeliver: 0,
	}, func(_ context.Context, _ *natspkg.Msg) error {
		return ErrSendToDLQ
	})

	sub, err := client.Consumer().SubscribeBound(ctx, stream, durable, prefix+">", handler)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	require.NoError(t, client.Publisher().PublishJSON(ctx, prefix+"created", map[string]string{"id": "1"}))

	select {
	case msg := <-gotDLQ:
		assert.Equal(t, prefix+"created", msg.Header.Get(HeaderDLQOriginalSubject))
		assert.Equal(t, "handler_requested", msg.Header.Get(HeaderDLQReason))
		assert.Contains(t, bytesconv.BytesToString(msg.Data), `"id"`)
	case <-time.After(testWaitShort):
		t.Fatal("timeout waiting for dlq message")
	}
}

func TestSuperviseSubscribeBoundIntegration(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "SUP")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage,
	})
	require.NoError(t, err)

	durable := uniqueDurable(t, "sup")
	got := make(chan struct{}, 1)
	sub, err := client.SuperviseSubscribeBound(ctx, stream, durable, prefix+">",
		func(_ context.Context, msg *natspkg.Msg) error {
			select {
			case got <- struct{}{}:
			default:
			}

			return nil
		}, SupervisorConfig{CheckInterval: 50 * time.Millisecond, MaxRetries: 3})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Stop() })

	require.NoError(t, client.Publisher().PublishJSON(ctx, prefix+"e", map[string]int{"n": 1}))
	select {
	case <-got:
	case <-time.After(testWaitShort):
		t.Fatal("timeout waiting for supervised message")
	}
	assert.True(t, sub.IsValid())
}
