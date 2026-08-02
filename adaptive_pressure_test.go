package nats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestDecideAdaptivePressureLevels(t *testing.T) {
	t.Parallel()
	cfg := AdaptivePressureConfig{Enabled: true, MinFetchBatch: 1, MaxFetchBatch: 100, MinAckPending: 10}

	idle := DecideAdaptivePressure(cfg, AdaptivePressureInput{
		PoolDepth: 0, PoolCapacity: 100, CurrentBatch: 10, MaxAckPending: 1000,
	})
	assert.Equal(t, PressureIdle, idle.Level)
	assert.Greater(t, idle.FetchBatch, 10)

	hard := DecideAdaptivePressure(cfg, AdaptivePressureInput{
		PoolDepth: 80, PoolCapacity: 100, CurrentBatch: 50, MaxAckPending: 1000,
		BaselineLatency: time.Millisecond, HandlerLatency: 4 * time.Millisecond,
	})
	assert.Equal(t, PressureHard, hard.Level)
	assert.Equal(t, 1, hard.FetchBatch)
	assert.Greater(t, hard.NakDelay, time.Duration(0))

	crit := DecideAdaptivePressure(cfg, AdaptivePressureInput{
		PoolDepth: 99, PoolCapacity: 100, Lag: 20_000, CurrentBatch: 50,
	})
	assert.Equal(t, PressureCritical, crit.Level)
	assert.Equal(t, cfg.MinAckPending, crit.AckPending)
}

func TestAdaptivePressureFetchBatchOr(t *testing.T) {
	t.Parallel()
	a := NewAdaptivePressure(AdaptivePressureConfig{Enabled: true, MaxFetchBatch: 32})
	assert.Equal(t, 32, a.FetchBatchOr(10))
	a.Observe(AdaptivePressureInput{PoolDepth: 90, PoolCapacity: 100, CurrentBatch: 32})
	assert.Equal(t, 1, a.FetchBatchOr(10))
}

func TestDecideAdaptivePressureElevated(t *testing.T) {
	t.Parallel()
	cfg := AdaptivePressureConfig{Enabled: true, MinFetchBatch: 2, MaxFetchBatch: 64}
	d := DecideAdaptivePressure(cfg, AdaptivePressureInput{
		PoolDepth: 45, PoolCapacity: 100, CurrentBatch: 40, MaxAckPending: 500,
	})
	assert.Equal(t, PressureElevated, d.Level)
	assert.Equal(t, 20, d.FetchBatch)
	assert.Greater(t, d.NakDelay, time.Duration(0))
}

func BenchmarkDecideAdaptivePressure(b *testing.B) {
	cfg := AdaptivePressureConfig{Enabled: true, MinFetchBatch: 1, MaxFetchBatch: 256}
	in := AdaptivePressureInput{
		PoolDepth: 70, PoolCapacity: 100, CurrentBatch: 64, MaxAckPending: 1000,
		BaselineLatency: time.Millisecond, HandlerLatency: 3 * time.Millisecond,
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = DecideAdaptivePressure(cfg, in)
	}
}
