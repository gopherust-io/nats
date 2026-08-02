package nats

import (
	"context"
	"testing"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIncidentCapsuleCaptureReplay(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "CAPSULE")
	prefix := streamSubjectPrefix(stream)
	subject := prefix + "evt"

	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage, Replicas: 1,
	})
	require.NoError(t, err)

	for i := 0; i < 5; i++ {
		require.NoError(t, client.Publisher().PublishJSON(ctx, subject, map[string]int{"n": i}))
	}

	last, err := client.Replay().GetLastMsgForSubject(ctx, stream, subject)
	require.NoError(t, err)

	rec := NewFlightRecorder(16)
	rec.Record(IncidentEvent{Kind: IncidentDLQ, Detail: "test", Stream: stream, Sequence: last.Sequence})

	store := uniqueName(t, "capstore")
	index := uniqueName(t, "capidx")

	capsule, err := client.Incidents().Capture(ctx, IncidentCapture{
		Stream:         stream,
		Consumer:       "orders-processor",
		Trigger:        TriggerDLQ,
		FailingSeq:     last.Sequence,
		Window:         10,
		Subject:        subject,
		Reason:         "test_poison",
		StoreBucket:    store,
		IndexBucket:    index,
		FlightRecorder: rec,
		Redact: func(msg *CapsuleMessage) {
			if msg != nil && len(msg.Data) > 0 {
				msg.Data = []byte(`{"redacted":true}`)
			}
		},
	})
	require.NoError(t, err)
	require.NotEmpty(t, capsule.ID)
	assert.Equal(t, TriggerDLQ, capsule.Trigger)
	assert.NotEmpty(t, capsule.Messages)
	assert.NotEmpty(t, capsule.FlightTimeline)

	loaded, err := client.Incidents().Load(ctx, store, capsule.ID)
	require.NoError(t, err)
	assert.Equal(t, capsule.ID, loaded.ID)

	var seen int
	err = client.Incidents().ReplayLocal(ctx, loaded, func(_ context.Context, msg *natspkg.Msg) error {
		seen++
		assert.JSONEq(t, `{"redacted":true}`, string(msg.Data))
		return nil
	})
	require.NoError(t, err)
	assert.Equal(t, len(loaded.Messages), seen)
}

func TestIncidentCapsuleListDefaultIndex(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "CAPLIST")
	prefix := streamSubjectPrefix(stream)
	subject := prefix + "evt"
	durable := "list-worker"

	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage, Replicas: 1,
	})
	require.NoError(t, err)
	require.NoError(t, client.Publisher().PublishJSON(ctx, subject, map[string]string{"k": "v"}))
	last, err := client.Replay().GetLastMsgForSubject(ctx, stream, subject)
	require.NoError(t, err)

	capsule, err := client.Incidents().Capture(ctx, IncidentCapture{
		Stream:     stream,
		Consumer:   durable,
		Trigger:    TriggerManual,
		FailingSeq: last.Sequence,
		Window:     5,
	})
	require.NoError(t, err)

	ids, err := client.Incidents().List(ctx, stream, durable, "")
	require.NoError(t, err)
	assert.Contains(t, ids, capsule.ID)
}

func TestCapsuleAutoRecorders(t *testing.T) {
	t.Parallel()
	client, _ := testClient(t)
	var got IncidentTrigger
	auto := NewCapsuleAuto(client, "ORDERS", "worker", CapsuleAutoConfig{
		Enabled: true,
		OnReady: func(c *Capsule) {
			got = c.Trigger
		},
	})
	// Shadow path does not need JetStream seq; Capture still writes an empty-message capsule.
	auto.ShadowRecorder().RecordShadow("shadow_mismatch", "orders.x", "diff")
	assert.Equal(t, TriggerShadowMismatch, got)

	auto.OnAnomaly()(BehaviorAnomalyEvent{Stream: "ORDERS", Durable: "worker"})
	assert.Equal(t, TriggerAnomaly, got)
}

func TestIncidentCapsuleReplayLocalEmpty(t *testing.T) {
	t.Parallel()
	client, _ := testClient(t)
	err := client.Incidents().ReplayLocal(context.Background(), &Capsule{ID: "x"}, nil)
	require.Error(t, err)
}
