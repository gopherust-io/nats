package nats

import (
	"context"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/trace"
)

func TestTracePropagationPublishToConsume(t *testing.T) {
	client, ctx, sr := testClientWithTracing(t) // not parallel: mutates global otel providers
	stream := uniqueStream(t, "TRACE")
	prefix := streamSubjectPrefix(stream)
	subscribeSubject := prefix + ">"
	publishSubject := prefix + "events"
	durable := uniqueDurable(t, "trace-processor")

	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{subscribeSubject}, Replicas: 1,
	})
	require.NoError(t, err)

	done := make(chan trace.TraceID, 1)
	_, err = client.Consumer().QueueSubscribeBound(ctx, stream, durable, uniqueQueue(t, "trace-workers"), subscribeSubject,
		func(c context.Context, msg *natspkg.Msg) error {
			span := trace.SpanFromContext(c)
			if span.SpanContext().IsValid() {
				done <- span.SpanContext().TraceID()
			}
			return nil
		})
	require.NoError(t, err)

	require.NoError(t, client.Publisher().PublishJSON(ctx, publishSubject, map[string]string{"id": "1"}))

	var consumeTraceID trace.TraceID
	select {
	case consumeTraceID = <-done:
	case <-time.After(testWaitShort):
		t.Fatal("timeout waiting for traced message")
	}

	require.Eventually(t, func() bool {
		var publishID, processID trace.TraceID
		for _, s := range sr.Ended() {
			switch s.Name() {
			case "nats.publish":
				publishID = s.SpanContext().TraceID()
			case "nats.process":
				processID = s.SpanContext().TraceID()
			}
		}
		return publishID.IsValid() && publishID == processID && processID == consumeTraceID
	}, testWaitShort, testPollFast)
	assert.True(t, consumeTraceID.IsValid())
}
