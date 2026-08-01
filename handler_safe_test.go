package nats

import (
	"context"
	"testing"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

func TestProcessMessageHandlerPanicReturnsError(t *testing.T) {
	t.Parallel()
	c := &consumer{}
	err := c.processMessage(context.Background(), &natspkg.Msg{
		Subject: "orders.panic",
		Data:    bytesconv.StringToBytes("x"),
	}, func(_ context.Context, _ *natspkg.Msg) error {
		panic("boom")
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "handler panic: boom")
}

func TestResponderWrapHandlerPanicRecordsError(t *testing.T) {
	t.Parallel()
	metrics := newClientMetrics(context.Background(), MetricsConfig{
		AllowMetrics: true,
		Prefix:       "test-reply-panic",
	})
	require.NotNil(t, metrics)

	r := &responder{metrics: metrics, allowMetrics: true}
	wrapped := r.wrap(context.Background(), func(_ context.Context, _ *natspkg.Msg) error {
		panic("reply boom")
	})
	require.NotPanics(t, func() {
		wrapped(&natspkg.Msg{Subject: "reply.panic", Data: bytesconv.StringToBytes("x")})
	})
}

func TestInvokeMsgHandlerPanic(t *testing.T) {
	t.Parallel()
	err := invokeMsgHandler(context.Background(), &natspkg.Msg{Subject: "s"}, func(_ context.Context, _ *natspkg.Msg) error {
		panic("direct")
	})
	require.Error(t, err)
	assert.Equal(t, "handler panic: direct", err.Error())
}

func TestInvokeMsgHandlerPassThrough(t *testing.T) {
	t.Parallel()
	want := assert.AnError
	err := invokeMsgHandler(context.Background(), &natspkg.Msg{Subject: "s"}, func(_ context.Context, _ *natspkg.Msg) error {
		return want
	})
	require.ErrorIs(t, err, want)
}
