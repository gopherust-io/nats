package nats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestThroughputConfig(t *testing.T) {
	t.Parallel()
	cfg := ThroughputConfig()
	assert.False(t, cfg.PublisherConfig.AllowMetrics)
	assert.False(t, cfg.PublisherConfig.AllowTracing)
	assert.True(t, cfg.PublisherConfig.SkipSubjectValidation)
	assert.False(t, cfg.RuntimeConsumer.AllowMetrics)
	assert.False(t, cfg.RuntimeConsumer.AllowTracing)
	assert.True(t, cfg.RuntimeConsumer.WorkerPoolEnabled)
	assert.True(t, cfg.Metrics.Lite)
	assert.True(t, cfg.Metrics.FixedCardinality)
	assert.Equal(t, 60*time.Second, cfg.Metrics.CollectInterval)
	assert.Equal(t, BackpressureNak, cfg.Backpressure.Mode)
}
