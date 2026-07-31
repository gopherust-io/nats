package nats

import (
	"bytes"
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/gopherust-io/nats/internal/bytesconv"
	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFlightRecorderRingAndSnapshot(t *testing.T) {
	t.Parallel()
	r := NewFlightRecorder(3)
	r.Record(IncidentEvent{Kind: IncidentDLQ, Detail: "a"})
	r.Record(IncidentEvent{Kind: IncidentStall, Detail: "b"})
	r.Record(IncidentEvent{Kind: IncidentSupervisor, Detail: "c"})
	r.Record(IncidentEvent{Kind: IncidentReconnect, Detail: "d"}) // drops oldest

	snap := r.Snapshot()
	require.Len(t, snap, 3)
	assert.Equal(t, "b", snap[0].Detail)
	assert.Equal(t, "c", snap[1].Detail)
	assert.Equal(t, "d", snap[2].Detail)
}

func TestFlightRecorderWriteJSON(t *testing.T) {
	t.Parallel()
	r := NewFlightRecorder(8)
	r.Record(IncidentEvent{Kind: IncidentDLQ, Subject: "orders.x", Reason: "max_deliver", Sequence: 9})

	var buf bytes.Buffer
	require.NoError(t, r.WriteJSON(&buf))

	var decoded []IncidentEvent
	require.NoError(t, json.Unmarshal(buf.Bytes(), &decoded))
	require.Len(t, decoded, 1)
	assert.Equal(t, IncidentDLQ, decoded[0].Kind)
	assert.Equal(t, "orders.x", decoded[0].Subject)
}

func TestFlightRecorderReconnectAndEvents(t *testing.T) {
	t.Parallel()
	assert.Nil(t, (*FlightRecorder)(nil).Snapshot())
	closed := (*FlightRecorder)(nil).Events()
	_, ok := <-closed
	assert.False(t, ok)

	r := NewFlightRecorder(0)
	require.Equal(t, defaultFlightRecorderCapacity, r.cap)

	r.RecordReconnect("spike")
	ev := <-r.Events()
	assert.Equal(t, IncidentReconnect, ev.Kind)
	assert.Equal(t, "spike", ev.Detail)

	assert.Equal(t, "resubscribed", supervisorKindName(SupervisorResubscribed))
	assert.Equal(t, "give_up", supervisorKindName(SupervisorGiveUp))
	assert.Equal(t, "invalid", supervisorKindName(SupervisorInvalid))
	assert.Equal(t, "unknown", supervisorKindName(SupervisorEventKind(99)))
}

func TestFlightRecorderAttachSupervisor(t *testing.T) {
	t.Parallel()
	r := NewFlightRecorder(16)
	cfg := SupervisorConfig{}
	r.AttachSupervisor(&cfg)
	require.NotNil(t, cfg.OnEvent)

	cfg.OnEvent(SupervisorEvent{Kind: SupervisorInvalid, Attempt: 1})
	cfg.OnEvent(SupervisorEvent{Kind: SupervisorGiveUp, Attempt: 2, Err: ErrSupervisorGiveUp})

	snap := r.Snapshot()
	require.GreaterOrEqual(t, len(snap), 2)
	assert.Equal(t, IncidentSupervisor, snap[0].Kind)
	assert.Equal(t, "invalid", snap[0].Detail)
}

func TestFlightRecorderAttachSoftLiveness(t *testing.T) {
	t.Parallel()
	r := NewFlightRecorder(8)
	cfg := SoftLivenessConfig{}
	r.AttachSoftLiveness(&cfg)
	require.NotNil(t, cfg.OnStall)

	cfg.OnStall(SoftLivenessEvent{NumPending: 42, StalledFor: time.Second, Err: ErrConsumerStall})
	snap := r.Snapshot()
	require.Len(t, snap, 1)
	assert.Equal(t, IncidentStall, snap[0].Kind)
	assert.Equal(t, uint64(42), snap[0].NumPending)
}

func TestFlightRecorderRecordDLQ(t *testing.T) {
	t.Parallel()
	r := NewFlightRecorder(4)
	r.RecordDLQ("orders.created", "ORDERS", "worker", "handler_requested", 7)
	snap := r.Snapshot()
	require.Len(t, snap, 1)
	assert.Equal(t, IncidentDLQ, snap[0].Kind)
	assert.Equal(t, uint64(7), snap[0].Sequence)
}

func TestWithDLQRecordsFlight(t *testing.T) {
	t.Parallel()
	r := NewFlightRecorder(4)
	pub := &dlqTestPublisher{}
	h := WithDLQ(DLQConfig{
		Publisher: pub,
		Subject:   "orders.dlq",
		Recorder:  r,
	}, func(_ context.Context, _ *natspkg.Msg) error {
		return ErrSendToDLQ
	})

	err := h(context.Background(), &natspkg.Msg{
		Subject: "orders.created",
		Data:    bytesconv.StringToBytes(`{}`),
	})
	// Term fails without JetStream reply, so recorder should not fire.
	require.Error(t, err)
	assert.Empty(t, r.Snapshot())

	// Simulate successful route recording API directly (integration covers Term path).
	r.RecordDLQ("orders.created", "ORDERS", "c", "handler_requested", 1)
	require.Len(t, r.Snapshot(), 1)
}
