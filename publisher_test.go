package nats

import (
	"context"
	"testing"
	"time"

	"github.com/gopherust-io/nats/internal/bytesconv"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestPublisherPublishEmptySubject(t *testing.T) {
	p := &publisher{}
	err := p.publish(context.Background(), empty, Message{MessageType: JSON, Data: map[string]string{"id": "1"}})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEmptySubjectNotAllowed)
}

func TestPublisherPublishInvalidMessageType(t *testing.T) {
	p := &publisher{}
	err := p.publish(context.Background(), "orders.created", Message{MessageType: MessageType(99), Data: "x"})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidMessageType)
}

func TestPublisherPublishEncodeError(t *testing.T) {
	p := &publisher{}
	err := p.publish(context.Background(), "orders.created", Message{
		MessageType: Proto,
		Data:        "not-a-proto",
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "encode")
}

func TestPublisherMetricHelpers(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	metrics := newClientMetrics(ctx, MetricsConfig{AllowMetrics: true, Prefix: "pub"})
	require.NotNil(t, metrics)

	p := &publisher{allowMetrics: true, metrics: metrics}
	assert.Equal(t, "orders.created", p.metricSubject("orders.created"))
	p.metrics.fixedCardinality = true
	assert.Empty(t, p.metricSubject("orders.created"))
	p.metrics.fixedCardinality = false

	p.recordError(ctx, "orders.created")
	p.recordSuccess(ctx, "orders.created", 12, 5*time.Millisecond)
	p.recordAsyncAccepted(ctx, "orders.created", 12)

	p.allowMetrics = false
	p.recordError(ctx, "orders.created")
	p.recordSuccess(ctx, "orders.created", 1, time.Millisecond)
	p.recordAsyncAccepted(ctx, "orders.created", 1)
}

func TestPublishProtoAndAsyncIntegration(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "PROTO")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1,
	})
	require.NoError(t, err)

	require.NoError(t, client.Publisher().PublishProto(ctx, prefix+"events", wrapperspb.String("hi")))

	fut, err := client.Publisher().PublishAsync(ctx, prefix+"async", Message{
		MessageType: JSON,
		Data:        map[string]string{"id": "1"},
	})
	require.NoError(t, err)
	require.NotNil(t, fut)
	require.NoError(t, client.Publisher().PublishAsyncComplete(ctx))
}

func TestPublishJSONIntegration(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "JSON")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1,
	})
	require.NoError(t, err)
	require.NoError(t, client.Publisher().PublishJSON(ctx, prefix+"events", map[string]string{"id": "1"}))
}

func TestPublishMsgPackIntegration(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "MSGPACK")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1,
	})
	require.NoError(t, err)
	require.NoError(t, client.Publisher().PublishMsgPack(ctx, prefix+"events", map[string]string{"id": "1"}))
}

func TestPublishBytesIntegration(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "BYTES")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1,
	})
	require.NoError(t, err)
	require.NoError(t, client.Publisher().PublishBytes(ctx, prefix+"events", bytesconv.StringToBytes(`{"id":"1"}`)))
}

func TestPublishBytesWithMsgIDIntegration(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "BYTESID")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1,
		DuplicateWindow: 2 * time.Minute,
	})
	require.NoError(t, err)
	require.NoError(t, client.Publisher().PublishBytesWithMsgID(ctx, prefix+"events", uniqueDurable(t, "bytes"), bytesconv.StringToBytes("raw")))
}

func TestPublishAsyncIntegration(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "ASYNC")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1,
	})
	require.NoError(t, err)

	future, err := client.Publisher().PublishAsyncBytes(ctx, prefix+"events", bytesconv.StringToBytes("async"))
	require.NoError(t, err)

	select {
	case ack := <-future.Ok():
		require.NotNil(t, ack)
	case err := <-future.Err():
		require.NoError(t, err)
	case <-time.After(5 * time.Second):
		t.Fatal("async publish ack timeout")
	}
}

func TestPublisherSkipSubjectValidation(t *testing.T) {
	p := &publisher{skipSubjectValidation: true}
	err := p.validateSubject("bad subject with spaces")
	require.NoError(t, err)
}

func TestPublishWithMsgIDSetsHeader(t *testing.T) {
	msg := Message{Data: map[string]string{"id": "1"}, MessageType: JSON}
	withID := msg.WithMsgID("dedup-1")
	require.Equal(t, []string{"dedup-1"}, withID.Header[HeaderMsgID])
}

func TestPublishWithMsgIDIntegration(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "MSGID")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1,
		DuplicateWindow: 2 * time.Minute,
	})
	require.NoError(t, err)
	err = client.Publisher().PublishWithMsgID(ctx, prefix+"events", uniqueDurable(t, "msgid"), Message{
		Data: map[string]string{"id": "1"}, MessageType: JSON,
	})
	require.NoError(t, err)
}

// ExampleMessage_WithMsgID demonstrates publish deduplication header.
func ExampleMessage_WithMsgID() {
	_ = Message{Data: map[string]string{"id": "1"}, MessageType: JSON}.WithMsgID("pay-123")
}

func TestNewClientValidation(t *testing.T) {
	_, err := NewClient(context.Background(), nil)
	require.Error(t, err)
	require.ErrorIs(t, err, ErrEmptyConfigNotAllowed)

	cfg := DefaultConfig()
	cfg.Conn.Address = empty
	_, err = NewClient(context.Background(), &cfg)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEmptyAddressNotAllowed)
}
