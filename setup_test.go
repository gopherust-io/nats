package nats

import (
	"context"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"
)

func TestSetupWorkerValidation(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	handler := func(context.Context, *natspkg.Msg) error { return nil }

	tests := []struct {
		name  string
		setup WorkerSetup
	}{
		{name: "empty stream", setup: WorkerSetup{Consumer: DurableConsumerConfig{Durable: "d"}, Queue: "q", Subject: "s.>"}},
		{name: "empty durable", setup: WorkerSetup{Stream: StreamConfig{Name: "S"}, Queue: "q", Subject: "s.>"}},
		{name: "empty subject", setup: WorkerSetup{Stream: StreamConfig{Name: "S"}, Consumer: DurableConsumerConfig{Durable: "d"}, Queue: "q"}},
		{name: "empty queue", setup: WorkerSetup{Stream: StreamConfig{Name: "S"}, Consumer: DurableConsumerConfig{Durable: "d"}, Subject: "s.>"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			_, err := client.SetupWorker(ctx, tt.setup, handler)
			require.Error(t, err)
		})
	}
}

func TestSetupWorkerIntegration(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "SETUP")
	prefix := streamSubjectPrefix(stream)
	subscribeSubject := prefix + ">"
	publishSubject := prefix + "events"
	received := make(chan struct{}, 1)

	_, err := client.SetupWorker(ctx, WorkerSetup{
		Stream: StreamConfig{Name: stream, Subjects: []string{subscribeSubject}, Replicas: 1},
		Consumer: DurableConsumerConfig{
			Durable:       uniqueDurable(t, "setup-worker"),
			FilterSubject: subscribeSubject,
		},
		Queue:   uniqueQueue(t, "setup-workers"),
		Subject: subscribeSubject,
	}, func(c context.Context, msg *natspkg.Msg) error {
		select {
		case received <- struct{}{}:
		default:
		}
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, client.Publisher().PublishJSON(ctx, publishSubject, map[string]string{"id": "1"}))

	select {
	case <-received:
	case <-time.After(testWaitShort):
		t.Fatal("timeout waiting for setup worker message")
	}
}

// ExampleWorkerSetup documents one-shot worker setup fields.
func ExampleWorkerSetup() {
	_ = WorkerSetup{
		Stream:   StreamConfig{Name: "ORDERS", Subjects: []string{"orders.>"}},
		Consumer: DurableConsumerConfig{Durable: "orders-processor", FilterSubject: "orders.>"},
		Queue:    "orders-workers",
		Subject:  "orders.>",
	}
}
