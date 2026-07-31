package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/gopherust-io/nats/internal/bytesconv"
	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestShardReexports(t *testing.T) {
	t.Parallel()
	assert.Equal(t, ShardIndex("user-42", 8), ShardIndex("user-42", 8))
	assert.Equal(t, "orders.3.created", ShardSubject("orders", "user-42", 8, "created"))
}

func TestWithShadowReexport(t *testing.T) {
	t.Parallel()
	rec := NewFlightRecorder(8)
	primaryCalls := 0
	shadowCalls := 0

	h := WithShadow(ShadowConfig{SampleRate: 1, Recorder: rec},
		func(_ context.Context, _ *natspkg.Msg) error {
			primaryCalls++

			return nil
		},
		func(_ context.Context, _ *natspkg.Msg) error {
			shadowCalls++

			return errors.New("shadow diverged")
		},
	)

	require.NoError(t, h(context.Background(), &natspkg.Msg{Subject: "orders.x", Data: bytesconv.StringToBytes("1")}))
	assert.Equal(t, 1, primaryCalls)
	assert.Equal(t, 1, shadowCalls)

	// Allow async shadow recorder path to land.
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if len(rec.Snapshot()) > 0 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	metrics := newClientMetrics(context.Background(), MetricsConfig{AllowMetrics: true, Prefix: "shadow"})
	adapter := shadowMetricsAdapter{m: metrics}
	adapter.ShadowError(context.Background())
	adapter.ShadowMismatch(context.Background())

	c := &client{metrics: metrics}
	wrapped := c.WithShadow(ShadowConfig{SampleRate: 1},
		func(_ context.Context, _ *natspkg.Msg) error { return nil },
		func(_ context.Context, _ *natspkg.Msg) error { return nil },
	)
	require.NotNil(t, wrapped)
	require.NoError(t, wrapped(context.Background(), &natspkg.Msg{Subject: "a"}))
}

func TestShadowRecorderAdapter(t *testing.T) {
	t.Parallel()
	rec := NewFlightRecorder(4)
	shadowRecorderAdapter{r: rec}.RecordShadow("mismatch", "orders.x", "boom")
	snap := rec.Snapshot()
	require.Len(t, snap, 1)
	assert.Equal(t, IncidentShadow, snap[0].Kind)
	assert.Equal(t, "mismatch", snap[0].Detail)
	assert.Equal(t, "orders.x", snap[0].Subject)
	assert.Equal(t, "boom", snap[0].Err)
}
