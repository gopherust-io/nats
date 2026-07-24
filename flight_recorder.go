package nats

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"sync"
	"time"
)

const (
	defaultFlightRecorderCapacity = 128
	flightRecorderEventBuffer     = 32
)

// IncidentKind classifies flight-recorder events.
type IncidentKind uint8

const (
	IncidentSupervisor IncidentKind = iota + 1
	IncidentStall
	IncidentDLQ
	IncidentReconnect
	IncidentShadow
)

// IncidentEvent is one entry in the flight-recorder timeline.
type IncidentEvent struct {
	Time       time.Time    `json:"time"`
	Detail     string       `json:"detail,omitempty"`
	Subject    string       `json:"subject,omitempty"`
	Stream     string       `json:"stream,omitempty"`
	Consumer   string       `json:"consumer,omitempty"`
	Reason     string       `json:"reason,omitempty"`
	Err        string       `json:"err,omitempty"`
	Sequence   uint64       `json:"sequence,omitempty"`
	NumPending uint64       `json:"num_pending,omitempty"`
	Attempt    int          `json:"attempt,omitempty"`
	Kind       IncidentKind `json:"kind"`
}

// FlightRecorder is a ring buffer of incident events for ops storytelling.
type FlightRecorder struct {
	events chan IncidentEvent
	buf    []IncidentEvent
	cap    int
	next   int
	mu     sync.Mutex
	full   bool
}

// NewFlightRecorder creates a recorder with the given capacity (default 128).
func NewFlightRecorder(capacity int) *FlightRecorder {
	if capacity <= 0 {
		capacity = defaultFlightRecorderCapacity
	}

	return &FlightRecorder{
		buf:    make([]IncidentEvent, capacity),
		cap:    capacity,
		events: make(chan IncidentEvent, flightRecorderEventBuffer),
	}
}

// Record appends an incident (drops oldest when full).
func (r *FlightRecorder) Record(ev IncidentEvent) {
	if r == nil {
		return
	}

	if ev.Time.IsZero() {
		ev.Time = time.Now().UTC()
	}

	r.mu.Lock()
	r.buf[r.next] = ev
	r.next = (r.next + 1) % r.cap
	if r.next == 0 {
		r.full = true
	}
	r.mu.Unlock()

	select {
	case r.events <- ev:
	default:
	}
}

// Events is a live stream of newly recorded incidents (buffered; overflows dropped).
func (r *FlightRecorder) Events() <-chan IncidentEvent {
	if r == nil {
		ch := make(chan IncidentEvent)
		close(ch)

		return ch
	}

	return r.events
}

// Snapshot returns incidents in chronological order (oldest first).
func (r *FlightRecorder) Snapshot() []IncidentEvent {
	if r == nil {
		return nil
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	if !r.full && r.next == 0 {
		return nil
	}

	if !r.full {
		out := make([]IncidentEvent, r.next)
		copy(out, r.buf[:r.next])

		return out
	}

	out := make([]IncidentEvent, r.cap)
	copy(out, r.buf[r.next:])
	copy(out[r.cap-r.next:], r.buf[:r.next])

	return out
}

// WriteJSON writes a Snapshot as JSON to w.
func (r *FlightRecorder) WriteJSON(w io.Writer) error {
	enc := json.NewEncoder(w)
	enc.SetIndent("", "  ")

	return enc.Encode(r.Snapshot())
}

// LogSnapshot writes a compact slog dump of the current snapshot.
func (r *FlightRecorder) LogSnapshot(ctx context.Context, msg string) {
	if r == nil {
		return
	}

	snap := r.Snapshot()
	slog.WarnContext(ctx, msg, slog.Int("incident_count", len(snap)))
	for _, ev := range snap {
		slog.WarnContext(ctx, "flight_recorder_incident",
			slog.Time("time", ev.Time),
			slog.Int("kind", int(ev.Kind)),
			slog.String("detail", ev.Detail),
			slog.String("subject", ev.Subject),
			slog.String("reason", ev.Reason),
			slog.Uint64("num_pending", ev.NumPending),
			slog.Int("attempt", ev.Attempt),
			slog.String("err", ev.Err))
	}
}

// AttachSupervisor wraps cfg.OnEvent to record supervisor lifecycle events.
// On SupervisorGiveUp it auto-logs a compact dump.
func (r *FlightRecorder) AttachSupervisor(cfg *SupervisorConfig) {
	if r == nil || cfg == nil {
		return
	}

	prev := cfg.OnEvent
	cfg.OnEvent = func(ev SupervisorEvent) {
		detail := supervisorKindName(ev.Kind)
		errStr := empty
		if ev.Err != nil {
			errStr = ev.Err.Error()
		}

		r.Record(IncidentEvent{
			Kind:    IncidentSupervisor,
			Detail:  detail,
			Attempt: ev.Attempt,
			Err:     errStr,
		})

		if ev.Kind == SupervisorGiveUp {
			r.LogSnapshot(context.Background(), "flight recorder dump on supervisor give-up")
		}

		if prev != nil {
			prev(ev)
		}
	}
}

// AttachSoftLiveness wraps cfg.OnStall to record stall incidents.
func (r *FlightRecorder) AttachSoftLiveness(cfg *SoftLivenessConfig) {
	if r == nil || cfg == nil {
		return
	}

	prev := cfg.OnStall
	cfg.OnStall = func(ev SoftLivenessEvent) {
		errStr := empty
		if ev.Err != nil {
			errStr = ev.Err.Error()
		}

		r.Record(IncidentEvent{
			Kind:       IncidentStall,
			Detail:     "consumer_stall",
			NumPending: ev.NumPending,
			Err:        errStr,
		})

		if prev != nil {
			prev(ev)
		}
	}
}

// RecordDLQ records a dead-letter route.
func (r *FlightRecorder) RecordDLQ(subject, stream, consumer, reason string, seq uint64) {
	r.Record(IncidentEvent{
		Kind:     IncidentDLQ,
		Detail:   "dlq_route",
		Subject:  subject,
		Stream:   stream,
		Consumer: consumer,
		Reason:   reason,
		Sequence: seq,
	})
}

// RecordDLQAutopsy records a dead-letter route with forensic error detail.
func (r *FlightRecorder) RecordDLQAutopsy(subject, stream, consumer, reason, errStr string, seq uint64) {
	r.Record(IncidentEvent{
		Kind:     IncidentDLQ,
		Detail:   "autopsy",
		Subject:  subject,
		Stream:   stream,
		Consumer: consumer,
		Reason:   reason,
		Err:      errStr,
		Sequence: seq,
	})
}

// RecordReconnect records a reconnect spike for storytelling.
func (r *FlightRecorder) RecordReconnect(detail string) {
	r.Record(IncidentEvent{
		Kind:   IncidentReconnect,
		Detail: detail,
	})
}

func supervisorKindName(k SupervisorEventKind) string {
	switch k {
	case SupervisorResubscribed:
		return "resubscribed"
	case SupervisorGiveUp:
		return "give_up"
	case SupervisorInvalid:
		return "invalid"
	default:
		return "unknown"
	}
}
