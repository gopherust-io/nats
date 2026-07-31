package nats

import (
	"context"
	"testing"
	"time"

	"github.com/gopherust-io/nats/internal/bytesconv"
	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestNewConsumerDefaults(t *testing.T) {
	c := newConsumer(context.Background(), RuntimeConsumerConfig{}, BackpressureConfig{}, nil, nil, false)
	require.NotNil(t, c)
	assert.Equal(t, 30*time.Second, c.cfg.AckWait)
}

func TestAppendConsumerOpts(t *testing.T) {
	c := newConsumer(context.Background(), RuntimeConsumerConfig{
		AckWait:       10 * time.Second,
		IdleHeartbeat: time.Second,
		FlowControl:   true,
	}, BackpressureConfig{}, nil, nil, false)

	opts := c.appendConsumerOpts(nil, false)
	require.NotEmpty(t, opts)

	queueOpts := c.appendConsumerOpts(nil, true)
	require.NotEmpty(t, queueOpts)
	// Queue path must not add IdleHeartbeat/FlowControl (would error at subscribe).
	assert.Less(t, len(queueOpts), len(opts))
}

func TestAppendConsumerOptsSkipsHeartbeatWhenDisabled(t *testing.T) {
	c := newConsumer(context.Background(), RuntimeConsumerConfig{
		AckWait:       10 * time.Second,
		IdleHeartbeat: 0,
	}, BackpressureConfig{}, nil, nil, false)
	opts := c.appendConsumerOpts(nil, false)
	queueOpts := c.appendConsumerOpts(nil, true)
	assert.Equal(t, len(opts), len(queueOpts))
}

func TestRecordMessageMetricsNil(t *testing.T) {
	c := &consumer{metrics: nil}
	msg := &natspkg.Msg{Subject: "test.subject", Data: bytesconv.StringToBytes("data")}
	assert.Equal(t, int64(0), c.recordMessageMetrics(context.Background(), msg, nil))
}

func TestRecordMessageMetricsWithMetrics(t *testing.T) {
	metrics := newClientMetrics(context.Background(), MetricsConfig{
		AllowMetrics: true,
		Prefix:       "test",
	})
	require.NotNil(t, metrics)

	c := &consumer{metrics: metrics}
	msg := &natspkg.Msg{Subject: "bench.subject", Data: bytesconv.StringToBytes(`{"id":"1"}`)}
	start := c.recordMessageMetrics(context.Background(), msg, nil)
	assert.Positive(t, start)
}

func TestRecordProcessError(t *testing.T) {
	metrics := newClientMetrics(context.Background(), MetricsConfig{
		AllowMetrics: true,
		Prefix:       "test-err",
	})
	require.NotNil(t, metrics)

	c := &consumer{metrics: metrics}
	msg := &natspkg.Msg{Subject: "orders.fail", Data: bytesconv.StringToBytes("x")}
	c.recordProcessError(context.Background(), msg.Subject, msg, assert.AnError)
	assert.Equal(t, "orders.fail", c.metricSubject(msg.Subject))

	c.metrics.fixedCardinality = true
	assert.Empty(t, c.metricSubject(msg.Subject))
}

func TestToNatsConsumerConfigAckDefault(t *testing.T) {
	cc := toNatsConsumerConfig(DurableConsumerConfig{Durable: "worker"})
	assert.Equal(t, AckExplicit, cc.AckPolicy)
}

func TestWrapHandlerDirectPath(t *testing.T) {
	c := newConsumer(context.Background(), RuntimeConsumerConfig{WorkerPoolEnabled: false}, BackpressureConfig{}, nil, nil, false)
	wrapped := c.wrapHandler(context.Background(), func(_ context.Context, _ *natspkg.Msg) error {
		return nil
	})
	require.NotNil(t, wrapped)
}

func TestSubscriptionNilGuards(t *testing.T) {
	sub := &subscription{}
	assert.False(t, sub.IsValid())
	assert.Equal(t, empty, sub.Subject())
	require.ErrorIs(t, sub.Unsubscribe(), ErrInvalidSubscription)
	require.ErrorIs(t, sub.Drain(), ErrInvalidSubscription)
	require.ErrorIs(t, sub.SetPendingLimits(1, 1), ErrInvalidSubscription)
	_, err := sub.ConsumerInfo()
	require.ErrorIs(t, err, ErrInvalidSubscription)
	assert.Equal(t, natspkg.SubscriptionType(-1), sub.Type())
}

func TestDecodeTyped(t *testing.T) {
	type payload struct {
		ID string `json:"id"`
	}
	data, err := Encode(Message{Data: payload{ID: "42"}, MessageType: JSON})
	require.NoError(t, err)

	msg := &natspkg.Msg{
		Subject: "typed.subject",
		Data:    data,
		Header:  natspkg.Header{HeaderContentType: []string{ContentTypeJSON}},
	}

	decoded, err := DecodeTyped[payload](msg, 0)
	require.NoError(t, err)
	assert.Equal(t, "42", decoded.ID)
}

func TestDecodeTypedError(t *testing.T) {
	msg := &natspkg.Msg{Subject: "bad", Data: bytesconv.StringToBytes("not-json")}
	_, err := DecodeTyped[map[string]string](msg, JSON)
	require.Error(t, err)
}

// ExampleConsumer_QueueSubscribeBound documents durable queue subscribe with stream binding.
func ExampleConsumer_QueueSubscribeBound() {
	stream := "ORDERS"
	durable := "orders-processor"
	queue := "orders-workers"
	subject := "orders.>"
	_ = stream
	_ = durable
	_ = queue
	_ = subject
}
