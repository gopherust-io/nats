package nats

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestDefaultConfig(t *testing.T) {
	cfg := DefaultConfig()

	assert.Equal(t, defaultNATSAddress, cfg.Conn.Address)
	assert.True(t, cfg.Conn.AllowReconnect)
	assert.Equal(t, defaultMaxReconnect, cfg.Conn.MaxReconnect)
	assert.Equal(t, defaultReconnectWait, cfg.Conn.ReconnectWait)
	assert.Equal(t, defaultReconnectJitter, cfg.Conn.ReconnectJitter)
	assert.Equal(t, defaultReconnectJitterTLS, cfg.Conn.ReconnectJitterTLS)
	assert.Equal(t, defaultReconnectBufSize, cfg.Conn.ReconnectBufSize)
	assert.Equal(t, defaultPingInterval, cfg.Conn.PingInterval)
	assert.Equal(t, defaultMaxPingsOut, cfg.Conn.MaxPingsOut)
	assert.True(t, cfg.Conn.RetryOnFailedConnect)
	assert.Equal(t, defaultInitialRetryAttempts, cfg.Conn.InitialRetryAttempts)
	assert.Equal(t, defaultConnectTimeout, cfg.Conn.ConnectTimeout)
	assert.Equal(t, defaultDrainTimeout, cfg.Conn.DrainTimeout)
	assert.True(t, cfg.Conn.AllowMetrics)
	assert.True(t, cfg.PublisherConfig.AllowMetrics)
	assert.Equal(t, defaultAckWait, cfg.RuntimeConsumer.AckWait)
	assert.Equal(t, defaultIdleHeartbeat, cfg.RuntimeConsumer.IdleHeartbeat)
	assert.True(t, cfg.RuntimeConsumer.FlowControl)
	assert.Equal(t, defaultWorkerPoolSize, cfg.RuntimeConsumer.WorkerPoolSize)
	assert.Equal(t, defaultWorkerBufferSize, cfg.RuntimeConsumer.WorkerBufferSize)
	assert.Equal(t, BackpressureNak, cfg.Backpressure.Mode)
	assert.Equal(t, defaultMaxAckPending, cfg.Backpressure.MaxAckPending)
	assert.Equal(t, defaultQueueDepthSampleInterval, cfg.Backpressure.QueueDepthSampleInterval)
	assert.True(t, cfg.Metrics.AllowMetrics)
	assert.True(t, cfg.Metrics.AllowTracing)
	assert.Equal(t, defaultMetricsCollectInterval, cfg.Metrics.CollectInterval)
}

// ExampleDevConfig shows a minimal local development NATS configuration.
func ExampleDevConfig() {
	cfg := DevConfig()
	_ = cfg.Stream.Name
}
