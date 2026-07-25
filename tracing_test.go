package nats

import (
	"context"
	"testing"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
)

func TestInjectTraceContext(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sr),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}))

	ctx, span := otel.Tracer("test").Start(context.Background(), "publish")
	msg := Message{Data: map[string]string{"id": "1"}, MessageType: JSON}
	injectTraceContext(ctx, &msg)
	wantTraceID := span.SpanContext().TraceID().String()
	span.End()

	require.NotEmpty(t, msg.Header)
	require.Equal(t, wantTraceID, msg.Header[HeaderTraceID][0])
	require.NotEmpty(t, msg.Header["traceparent"])
	assert.Equal(t, wantTraceID, TraceIDFromHeader(msg.Header))
}

func TestTraceIDFromHeader(t *testing.T) {
	assert.Empty(t, TraceIDFromHeader(nil))
	assert.Empty(t, TraceIDFromHeader(natspkg.Header{}))
	assert.Equal(t, "abc123", TraceIDFromHeader(natspkg.Header{
		HeaderTraceID: []string{"abc123"},
	}))
}

func TestStartProcessSpanDisabled(t *testing.T) {
	ctx, span := startProcessSpan(context.Background(), nil, false, nil)
	assert.Equal(t, context.Background(), ctx)
	assert.False(t, span.IsRecording())
}

func TestStartPublishSpanDisabledDoesNotEndParent(t *testing.T) {
	sr := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sr),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(provider)

	parentCtx, parent := otel.Tracer("test").Start(context.Background(), "parent")
	_, span := startPublishSpan(parentCtx, "orders.created", false)
	endSpan(span, nil)

	assert.False(t, span.IsRecording())
	assert.True(t, parent.IsRecording())
	parent.End()
}

func TestJetStreamMetadataSkipsEmptyReply(t *testing.T) {
	assert.Nil(t, jetStreamMetadata(nil))
	assert.Nil(t, jetStreamMetadata(&natspkg.Msg{Subject: "orders.created"}))
}
