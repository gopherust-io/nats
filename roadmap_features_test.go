package nats

import (
	"context"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPullMultiFilterNoCommonPrefix(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "MFILTER")
	prefix := streamSubjectPrefix(stream)
	subjectA := prefix + "orders"
	subjectB := prefix + "payments"
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1,
	})
	require.NoError(t, err)

	durable := uniqueDurable(t, "mfilter-pull")
	_, err = client.Consumers().CreateOrUpdateConsumer(ctx, stream, DurableConsumerConfig{
		Durable:        durable,
		FilterSubjects: []string{subjectA, subjectB},
	})
	require.NoError(t, err)

	require.NoError(t, client.Publisher().PublishJSON(ctx, subjectB, map[string]string{"k": "b"}))

	pull, err := client.Consumer().Pull(stream, durable)
	require.NoError(t, err)
	defer pull.Close()

	msgs, err := pull.Fetch(ctx, 1, WithFetchMaxWait(500*time.Millisecond))
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, subjectB, msgs[0].Subject)
}

func TestSetupWorkerFilterSubjects(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "SETUPMF")
	prefix := streamSubjectPrefix(stream)
	subjectA := prefix + "a"
	subjectB := prefix + "b"
	durable := uniqueDurable(t, "setup-mf")
	received := make(chan string, 2)

	_, err := client.SetupWorker(ctx, WorkerSetup{
		Stream: StreamConfig{Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1},
		Consumer: DurableConsumerConfig{
			Durable:        durable,
			FilterSubjects: []string{subjectA, subjectB},
		},
		Queue: uniqueQueue(t, "setup-mf-q"),
	}, func(_ context.Context, msg *natspkg.Msg) error {
		received <- msg.Subject
		return nil
	})
	require.NoError(t, err)

	info, err := client.Consumers().ConsumerInfo(ctx, stream, durable)
	require.NoError(t, err)
	assert.Equal(t, []string{subjectA, subjectB}, info.Config.FilterSubjects)

	require.NoError(t, client.Publisher().PublishJSON(ctx, subjectB, map[string]string{"id": "1"}))
	select {
	case got := <-received:
		assert.Equal(t, subjectB, got)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for multi-filter setup worker")
	}
}

func TestOrderedSubscribe(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "ORDERED")
	prefix := streamSubjectPrefix(stream)
	subject := prefix + "events"
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1,
	})
	require.NoError(t, err)

	got := make(chan struct{}, 1)
	sub, err := client.OrderedSubscribe(ctx, stream, OrderedConsumerConfig{
		FilterSubject: subject,
	}, func(_ context.Context, msg *natspkg.Msg) error {
		got <- struct{}{}
		return nil
	})
	require.NoError(t, err)
	defer sub.Stop()

	require.NoError(t, client.Publisher().PublishJSON(ctx, subject, map[string]string{"ok": "1"}))
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ordered message")
	}
}

func TestSchemaHelpers(t *testing.T) {
	t.Parallel()
	m := Message{Data: map[string]string{"a": "b"}, MessageType: JSON}
	ApplySchemaHeaders(&m, "orders.v1", "1", "json")
	require.Equal(t, "orders.v1", m.Header[HeaderSchema][0])

	msg := &natspkg.Msg{Header: natspkg.Header{}}
	msg.Header.Set(HeaderSchema, "orders.v1")
	msg.Header.Set(HeaderSchemaVersion, "1")
	require.NoError(t, CheckSchema(msg, SchemaExpectation{Name: "orders.v1", Version: "1"}))
	require.ErrorIs(t, CheckSchema(msg, SchemaExpectation{Name: "other", Required: true}), ErrSchemaMismatch)
}

func TestCircuitBreaker(t *testing.T) {
	t.Parallel()
	b := NewCircuitBreaker(CircuitConfig{Enabled: true, OpenOnLevel: PressureCritical})
	require.NotNil(t, b)
	assert.False(t, b.ShouldPause(AdaptivePressureDecision{Level: PressureWarm}))
	assert.True(t, b.ShouldPause(AdaptivePressureDecision{Level: PressureCritical}))
	assert.Equal(t, int64(1), b.Opens())
	assert.True(t, b.IsOpen())
}

func TestCRUDContextCanceled(t *testing.T) {
	t.Parallel()
	client, _ := testClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{Name: "X", Subjects: []string{"x.>"}})
	require.ErrorIs(t, err, context.Canceled)
}
