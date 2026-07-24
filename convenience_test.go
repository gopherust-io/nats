package nats

import (
	"context"
	"testing"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMessageWithMsgID(t *testing.T) {
	msg := Message{Data: map[string]string{"id": "1"}, MessageType: JSON}.WithMsgID("pay-123")
	require.NotNil(t, msg.Header)
	assert.Equal(t, []string{"pay-123"}, msg.Header[HeaderMsgID])
}

func TestMessageWithMsgIDPreservesExistingHeader(t *testing.T) {
	msg := Message{
		Data:        map[string]string{"id": "1"},
		MessageType: JSON,
		Header:      map[string][]string{"X-Custom": {"v"}},
	}.WithMsgID("pay-456")
	assert.Equal(t, []string{"v"}, msg.Header["X-Custom"])
	assert.Equal(t, []string{"pay-456"}, msg.Header[HeaderMsgID])
}

func TestDevConfig(t *testing.T) {
	cfg := DevConfig()
	assert.Equal(t, MemoryStorage, cfg.Stream.Storage)
	assert.False(t, cfg.Metrics.AllowMetrics)
	assert.False(t, cfg.Conn.AllowReconnect)
}

func TestProdWorkerConfig(t *testing.T) {
	cfg := ProdWorkerConfig()
	assert.Equal(t, WorkQueuePolicy, cfg.Stream.Retention)
	assert.True(t, cfg.RuntimeConsumer.WorkerPoolEnabled)
	assert.Equal(t, BackpressureNak, cfg.Backpressure.Mode)
}

func TestProdFanOutConfig(t *testing.T) {
	cfg := ProdFanOutConfig()
	assert.Equal(t, LimitsPolicy, cfg.Stream.Retention)
	assert.Equal(t, BackpressureBlock, cfg.Backpressure.Mode)
}

func TestHandlerTyped(t *testing.T) {
	type payload struct {
		ID string `json:"id"`
	}
	called := false
	handler := HandlerTyped[payload](JSON, func(_ context.Context, subject string, p payload) error {
		called = true
		assert.Equal(t, "orders.created", subject)
		assert.Equal(t, "99", p.ID)
		return nil
	})
	require.NotNil(t, handler)

	data, err := Encode(Message{Data: payload{ID: "99"}, MessageType: JSON})
	require.NoError(t, err)

	err = handler(context.Background(), &natspkg.Msg{
		Subject: "orders.created",
		Data:    data,
		Header:  natspkg.Header{HeaderContentType: []string{ContentTypeJSON}},
	})
	require.NoError(t, err)
	assert.True(t, called)
}
