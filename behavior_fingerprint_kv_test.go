package nats

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMarshalBehaviorFingerprintKV(t *testing.T) {
	t.Parallel()

	raw, err := MarshalBehaviorFingerprintKV(
		"ORDERS",
		"billing-worker",
		true,
		BehaviorSnapshot{MsgPerMin: 1000, Processing: 200 * time.Millisecond},
		BehaviorSnapshot{MsgPerMin: 1000, Processing: 2400 * time.Millisecond},
		30*time.Second,
		time.Date(2026, 7, 29, 6, 0, 0, 0, time.UTC),
	)
	require.NoError(t, err)

	var payload BehaviorFingerprintKVPayload
	require.NoError(t, json.Unmarshal(raw, &payload))
	assert.Equal(t, "ORDERS", payload.Stream)
	assert.Equal(t, "billing-worker", payload.Durable)
	assert.True(t, payload.Anomaly)
	assert.Equal(t, float64(1000), payload.Normal.MsgPerMin)
	assert.InDelta(t, 200, payload.Normal.ProcessingMs, 0.01)
	assert.InDelta(t, 2400, payload.Current.ProcessingMs, 0.01)
	assert.Equal(t, int64(30000), payload.SustainedForMs)
	assert.Equal(t, "ORDERS/billing-worker", BehaviorFingerprintKVKey("ORDERS", "billing-worker"))
}
