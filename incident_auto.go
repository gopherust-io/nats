package nats

import (
	"context"
	"sync"

	"github.com/gopherust-io/nats/dlq"
	"github.com/gopherust-io/nats/shadow"
)

// CapsuleAutoConfig wires automatic Incident Capsule capture from ops hooks.
//
// goalign:ignore
type CapsuleAutoConfig struct {
	StoreBucket    string
	IndexBucket    string
	Window         int
	FlightRecorder *FlightRecorder
	OnReady        func(*Capsule)
	Redact         func(*CapsuleMessage)
	Enabled        bool
}

// CapsuleAuto captures capsules from DLQ / shadow / anomaly hooks.
//
// goalign:ignore
type CapsuleAuto struct {
	client  Client
	cfg     CapsuleAutoConfig
	mu      sync.Mutex
	stream  string
	durable string
}

func NewCapsuleAuto(client Client, stream, durable string, cfg CapsuleAutoConfig) *CapsuleAuto {
	if cfg.Window <= 0 {
		cfg.Window = defaultCapsuleWindow
	}
	if cfg.StoreBucket == "" {
		cfg.StoreBucket = DefaultIncidentCapsuleBucket
	}
	if cfg.IndexBucket == "" {
		cfg.IndexBucket = DefaultIncidentIndexBucket
	}
	if cfg.Redact == nil {
		cfg.Redact = defaultCapsuleRedact
	}

	return &CapsuleAuto{client: client, cfg: cfg, stream: stream, durable: durable}
}

func defaultCapsuleRedact(msg *CapsuleMessage) {
	if msg == nil || msg.Header == nil {
		return
	}
	for _, key := range []string{"Authorization", "Nats-Api-Token", "Cookie", "X-Api-Key"} {
		if _, ok := msg.Header[key]; ok {
			msg.Header[key] = []string{"[redacted]"}
		}
	}
}

func (a *CapsuleAuto) capture(ctx context.Context, trigger IncidentTrigger, seq uint64, subject, reason string) {
	if a == nil || !a.cfg.Enabled || a.client == nil {
		return
	}
	a.mu.Lock()
	stream, durable := a.stream, a.durable
	a.mu.Unlock()

	capsule, err := a.client.Incidents().Capture(ctx, IncidentCapture{
		Stream:         stream,
		Consumer:       durable,
		Trigger:        trigger,
		FailingSeq:     seq,
		Window:         a.cfg.Window,
		Subject:        subject,
		Reason:         reason,
		StoreBucket:    a.cfg.StoreBucket,
		IndexBucket:    a.cfg.IndexBucket,
		FlightRecorder: a.cfg.FlightRecorder,
		Redact:         a.cfg.Redact,
	})
	if err != nil || capsule == nil {
		return
	}
	if a.cfg.OnReady != nil {
		a.cfg.OnReady(capsule)
	}
}

// DLQRecorder implements dlq.Recorder and captures capsules.
func (a *CapsuleAuto) DLQRecorder() dlq.Recorder {
	return capsuleDLQRecorder{auto: a}
}

type capsuleDLQRecorder struct{ auto *CapsuleAuto }

func (r capsuleDLQRecorder) RecordDLQ(subject, stream, consumer, reason string, seq uint64) {
	if r.auto == nil {
		return
	}
	r.auto.mu.Lock()
	if stream != "" {
		r.auto.stream = stream
	}
	if consumer != "" {
		r.auto.durable = consumer
	}
	r.auto.mu.Unlock()
	r.auto.capture(context.Background(), TriggerDLQ, seq, subject, reason)
}

func (r capsuleDLQRecorder) RecordDLQAutopsy(subject, stream, consumer, reason, _ string, seq uint64) {
	r.RecordDLQ(subject, stream, consumer, reason, seq)
}

// ShadowRecorder implements shadow.Recorder and captures on mismatch.
func (a *CapsuleAuto) ShadowRecorder() shadow.Recorder {
	return capsuleShadowRecorder{auto: a}
}

type capsuleShadowRecorder struct{ auto *CapsuleAuto }

func (r capsuleShadowRecorder) RecordShadow(detail, subject, err string) {
	if r.auto == nil {
		return
	}
	reason := detail
	if err != "" {
		reason = detail + ": " + err
	}
	r.auto.capture(context.Background(), TriggerShadowMismatch, 0, subject, reason)
}

// OnAnomaly returns a BehaviorFingerprint OnAnomaly callback.
func (a *CapsuleAuto) OnAnomaly() func(BehaviorAnomalyEvent) {
	return func(ev BehaviorAnomalyEvent) {
		if a == nil {
			return
		}
		a.mu.Lock()
		if ev.Stream != "" {
			a.stream = ev.Stream
		}
		if ev.Durable != "" {
			a.durable = ev.Durable
		}
		a.mu.Unlock()
		a.capture(context.Background(), TriggerAnomaly, 0, "", "behavior_fingerprint_anomaly")
	}
}
