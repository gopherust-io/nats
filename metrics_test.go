package nats

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gopherust-io/tel"
)

func TestNewClientMetricsDisabled(t *testing.T) {
	metrics := newClientMetrics(context.Background(), MetricsConfig{AllowMetrics: false})
	assert.Nil(t, metrics)
}

func TestNewMetricsCollectorDisabled(t *testing.T) {
	collector := newMetricsCollector(context.Background(), nil, nil, MetricsConfig{AllowMetrics: false}, nil)
	assert.Nil(t, collector)
}

func TestClientMetricsCreation(t *testing.T) {
	telem := tel.NewWithConfig(tel.DefaultDebugConfig())
	ctx := tel.WrapContext(context.Background(), telem)

	metrics := newClientMetrics(ctx, MetricsConfig{
		AllowMetrics: true,
		Prefix:       "test",
	})
	require.NotNil(t, metrics)
	assert.NotPanics(t, func() {
		metrics.TrackStream("orders")
	})
}

func TestMetricsCollectorRecordByteDelta(t *testing.T) {
	telem := tel.NewWithConfig(tel.DefaultDebugConfig())
	ctx := tel.WrapContext(context.Background(), telem)
	metrics := newClientMetrics(ctx, MetricsConfig{AllowMetrics: true, Prefix: "bytes"})
	require.NotNil(t, metrics)

	c := &metricsCollector{metrics: metrics}
	var last uint64
	c.recordByteDelta(ctx, metrics.connectionInBytes, 10, &last)
	assert.Equal(t, uint64(10), last)
	c.recordByteDelta(ctx, metrics.connectionInBytes, 15, &last)
	assert.Equal(t, uint64(15), last)
	c.recordByteDelta(ctx, metrics.connectionInBytes, 5, &last) // reset on decrease
	assert.Equal(t, uint64(5), last)
	c.recordByteDelta(ctx, nil, 1, &last)

	(*metricsCollector)(nil).stop()
}
