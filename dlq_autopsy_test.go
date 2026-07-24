package nats

import (
	"context"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestRecordDLQAutopsy(t *testing.T) {
	t.Parallel()
	r := NewFlightRecorder(4)
	r.RecordDLQAutopsy("orders.x", "ORDERS", "worker", "handler_requested", "boom", 3)
	snap := r.Snapshot()
	require.Len(t, snap, 1)
	assert.Equal(t, "autopsy", snap[0].Detail)
	assert.Equal(t, "boom", snap[0].Err)
	assert.Equal(t, IncidentDLQ, snap[0].Kind)
}

func TestDLQIntegrationAutopsy(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)

	stream := uniqueStream(t, "AUT")
	dlqStream := uniqueStream(t, "AUTDLQ")
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

	rec := NewFlightRecorder(8)
	gotDLQ := make(chan *natspkg.Msg, 1)
	dlqSub, err := client.Consumer().SubscribeBound(ctx, dlqStream, uniqueDurable(t, "autread"), dlqPrefix+">",
		func(_ context.Context, msg *natspkg.Msg) error {
			gotDLQ <- msg

			return nil
		})
	require.NoError(t, err)
	t.Cleanup(func() { _ = dlqSub.Unsubscribe() })

	handler := WithDLQ(DLQConfig{
		Publisher: client.Publisher(),
		Subject:   dlqSubject,
		Recorder:  rec,
		Autopsy:   AutopsyConfig{Enabled: true},
	}, func(_ context.Context, _ *natspkg.Msg) error {
		return ErrSendToDLQ
	})

	sub, err := client.Consumer().SubscribeBound(ctx, stream, uniqueDurable(t, "aut"), prefix+">", handler)
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	require.NoError(t, client.Publisher().PublishJSON(ctx, prefix+"created", map[string]string{"id": "1"}))

	select {
	case msg := <-gotDLQ:
		assert.NotEmpty(t, msg.Header.Get(HeaderAutopsyHash))
		assert.Contains(t, msg.Header.Get(HeaderAutopsyError), "send message to dlq")
		assert.Equal(t, "handler_requested", msg.Header.Get(HeaderDLQReason))
	case <-time.After(testWaitShort):
		t.Fatal("timeout waiting for autopsy dlq message")
	}

	require.Eventually(t, func() bool {
		for _, ev := range rec.Snapshot() {
			if ev.Detail == "autopsy" {
				return true
			}
		}

		return false
	}, testWaitShort, testPollFast)
}
