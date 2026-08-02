package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

const (
	DefaultIncidentCapsuleBucket = "nats_incident_capsules"
	DefaultIncidentIndexBucket   = "nats_incident_index"
)

const (
	defaultCapsuleWindow = 50
	capsuleSchemaVersion = 1
)

// IncidentTrigger classifies why a capsule was captured.
type IncidentTrigger string

const (
	TriggerManual         IncidentTrigger = "manual"
	TriggerDLQ            IncidentTrigger = "dlq"
	TriggerAnomaly        IncidentTrigger = "anomaly"
	TriggerShadowMismatch IncidentTrigger = "shadow_mismatch"
	TriggerStall          IncidentTrigger = "stall"
)

// CapsuleMessage is one stored message inside an incident capsule.
type CapsuleMessage struct {
	Time     time.Time           `json:"time"`
	Header   map[string][]string `json:"header,omitempty"`
	Subject  string              `json:"subject"`
	Data     []byte              `json:"data"`
	Sequence uint64              `json:"sequence"`
}

// Capsule is a portable forensic pack for offline ReplayLocal.
//
// goalign:ignore
type Capsule struct {
	CreatedAt      time.Time                     `json:"createdAt"`
	Fingerprint    *BehaviorFingerprintKVPayload `json:"fingerprint,omitempty"`
	ID             string                        `json:"id"`
	Stream         string                        `json:"stream"`
	Consumer       string                        `json:"consumer"`
	Trigger        IncidentTrigger               `json:"trigger"`
	Subject        string                        `json:"subject,omitempty"`
	Reason         string                        `json:"reason,omitempty"`
	Messages       []CapsuleMessage              `json:"messages"`
	FlightTimeline []IncidentEvent               `json:"flightTimeline,omitempty"`
	FailingSeq     uint64                        `json:"failingSeq,omitempty"`
	SchemaVersion  int                           `json:"schemaVersion"`
}

// IncidentCapture configures Capsule assembly.
//
// goalign:ignore
type IncidentCapture struct {
	Redact         func(*CapsuleMessage)
	FlightRecorder *FlightRecorder
	Fingerprint    *BehaviorFingerprintKVPayload
	Stream         string
	Consumer       string
	Subject        string
	Reason         string
	StoreBucket    string
	IndexBucket    string
	Trigger        IncidentTrigger
	FailingSeq     uint64
	Window         int
}

// Incidents captures and replays incident capsules.
type Incidents interface {
	Capture(ctx context.Context, cfg IncidentCapture) (*Capsule, error)
	Load(ctx context.Context, bucket, id string) (*Capsule, error)
	// List returns capsule IDs for stream/consumer. Empty indexBucket uses DefaultIncidentIndexBucket.
	List(ctx context.Context, stream, consumer, indexBucket string) ([]string, error)
	ReplayLocal(ctx context.Context, capsule *Capsule, handler MsgHandler) error
}

type incidents struct {
	objects ObjectStoreManager
	kv      KeyValueManager
	keys    KeyValueKeys
	replay  Replay
	streams StreamManager
}

func newIncidents(objects ObjectStoreManager, kv KeyValueManager, keys KeyValueKeys, replay Replay, streams StreamManager) *incidents {
	return &incidents{objects: objects, kv: kv, keys: keys, replay: replay, streams: streams}
}

func (c *client) Incidents() Incidents {
	return newIncidents(c.objects, c.kv, c.kv, c.replay, c.streams)
}

func (i *incidents) Capture(ctx context.Context, cfg IncidentCapture) (*Capsule, error) {
	if err := ValidateStreamName(cfg.Stream); err != nil {
		return nil, fmt.Errorf("incident capture: %w", err)
	}
	if bytesconv.IsEmpty(cfg.Consumer) {
		return nil, fmt.Errorf("incident capture: consumer required")
	}
	if cfg.Trigger == "" {
		cfg.Trigger = TriggerManual
	}
	window := cfg.Window
	if window <= 0 {
		window = defaultCapsuleWindow
	}
	store := cfg.StoreBucket
	if bytesconv.IsEmpty(store) {
		store = DefaultIncidentCapsuleBucket
	}
	index := cfg.IndexBucket
	if bytesconv.IsEmpty(index) {
		index = DefaultIncidentIndexBucket
	}

	capsule := &Capsule{
		ID:            nuid.Next(),
		CreatedAt:     time.Now().UTC(),
		Stream:        cfg.Stream,
		Consumer:      cfg.Consumer,
		Trigger:       cfg.Trigger,
		FailingSeq:    cfg.FailingSeq,
		Subject:       cfg.Subject,
		Reason:        cfg.Reason,
		Fingerprint:   cfg.Fingerprint,
		SchemaVersion: capsuleSchemaVersion,
	}
	if cfg.FlightRecorder != nil {
		capsule.FlightTimeline = cfg.FlightRecorder.Snapshot()
	}

	failingSeq := cfg.FailingSeq
	if failingSeq == 0 && i.streams != nil {
		if info, infoErr := i.streams.StreamInfo(ctx, cfg.Stream); infoErr == nil && info != nil && info.State.LastSeq > 0 {
			failingSeq = info.State.LastSeq
			capsule.FailingSeq = failingSeq
		}
	}

	if failingSeq > 0 {
		start := failingSeq
		half := uint64(window / 2)
		if start > half {
			start -= half
		} else {
			start = 1
		}
		end := failingSeq + half
		msgs, _, err := i.replay.GetMsgRange(ctx, cfg.Stream, start, end, WithMaxMessages(window))
		if err != nil {
			return nil, fmt.Errorf("incident capture range: %w", err)
		}
		capsule.Messages = make([]CapsuleMessage, 0, len(msgs))
		for _, replayMsg := range msgs {
			if replayMsg == nil {
				continue
			}
			cm := CapsuleMessage{
				Sequence: replayMsg.Sequence,
				Subject:  replayMsg.Subject,
				Time:     replayMsg.Time,
				Data:     append([]byte(nil), replayMsg.Data...),
				Header:   cloneHeaderMap(replayMsg.Header),
			}
			if cfg.Redact != nil {
				cfg.Redact(&cm)
			}
			capsule.Messages = append(capsule.Messages, cm)
		}
	}

	raw, err := json.Marshal(capsule)
	if err != nil {
		return nil, fmt.Errorf("incident capture marshal: %w", err)
	}

	if _, err := i.objects.Create(ctx, ObjectStoreConfig{Bucket: store, Storage: FileStorage}); err != nil {
		// Bucket may already exist.
		_ = err
	}
	if _, err := i.objects.Put(ctx, store, capsule.ID, raw); err != nil {
		return nil, fmt.Errorf("incident capture put object: %w", err)
	}

	if _, err := i.kv.CreateOrUpdate(ctx, KeyValueConfig{Bucket: index, History: 1}); err != nil {
		_ = err
	}
	key := incidentIndexKey(cfg.Stream, cfg.Consumer, capsule.ID)
	if _, err := i.keys.Put(ctx, index, key, []byte(capsule.ID)); err != nil {
		// Index is best-effort; object store already holds the capsule.
		_ = err
	}

	return capsule, nil
}

func (i *incidents) Load(ctx context.Context, bucket, id string) (*Capsule, error) {
	if bytesconv.IsEmpty(bucket) {
		bucket = DefaultIncidentCapsuleBucket
	}
	entry, err := i.objects.Get(ctx, bucket, id)
	if err != nil {
		return nil, fmt.Errorf("incident load: %w", err)
	}
	var loaded Capsule
	if err := json.Unmarshal(entry.Data, &loaded); err != nil {
		return nil, fmt.Errorf("incident load decode: %w", err)
	}

	return &loaded, nil
}

func (i *incidents) List(ctx context.Context, stream, consumer, indexBucket string) ([]string, error) {
	if bytesconv.IsEmpty(indexBucket) {
		indexBucket = DefaultIncidentIndexBucket
	}
	prefix := stream + "/" + consumer + "/"
	all, _, err := i.keys.ListKeys(ctx, indexBucket, 0, -1)
	if err != nil {
		return nil, fmt.Errorf("incident list keys: %w", err)
	}
	out := make([]string, 0)
	for _, k := range all {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, k[len(prefix):])
		}
	}

	return out, nil
}

func (i *incidents) ReplayLocal(ctx context.Context, capsule *Capsule, handler MsgHandler) error {
	if capsule == nil {
		return fmt.Errorf("incident replay: nil capsule")
	}
	if handler == nil {
		return fmt.Errorf("incident replay: nil handler")
	}
	for _, cm := range capsule.Messages {
		msg := &natspkg.Msg{
			Subject: cm.Subject,
			Data:    append([]byte(nil), cm.Data...),
			Header:  natspkg.Header(cloneHeaderMap(cm.Header)),
		}
		if err := invokeMsgHandler(ctx, msg, handler); err != nil {
			return fmt.Errorf("incident replay seq=%d subject=%q: %w", cm.Sequence, cm.Subject, err)
		}
	}

	return nil
}

func incidentIndexKey(stream, consumer, id string) string {
	return stream + "/" + consumer + "/" + id
}

func cloneHeaderMap(h map[string][]string) map[string][]string {
	if len(h) == 0 {
		return nil
	}
	out := make(map[string][]string, len(h))
	for k, vs := range h {
		out[k] = append([]string(nil), vs...)
	}

	return out
}
