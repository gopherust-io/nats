package nats

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

// DefaultBehaviorFingerprintKVBucket is the JetStream KV bucket nats-consol reads
// for Consumer Detail behavior fingerprints.
const DefaultBehaviorFingerprintKVBucket = "nats_consol_fingerprints"

// BehaviorFingerprintKVSnapshot is the JSON shape published for consol UI.
type BehaviorFingerprintKVSnapshot struct {
	MsgPerMin    float64 `json:"msgPerMin"`
	ProcessingMs float64 `json:"processingMs"`
}

// BehaviorFingerprintKVPayload is stored at key stream/durable in the fingerprint bucket.
// goalign:ignore
type BehaviorFingerprintKVPayload struct {
	UpdatedAt      time.Time                     `json:"updatedAt"`
	Stream         string                        `json:"stream"`
	Durable        string                        `json:"durable"`
	Normal         BehaviorFingerprintKVSnapshot `json:"normal"`
	Current        BehaviorFingerprintKVSnapshot `json:"current"`
	SustainedForMs int64                         `json:"sustainedForMs,omitempty"`
	Anomaly        bool                          `json:"anomaly"`
}

// BehaviorFingerprintKVKey builds the KV key consol reads for a consumer.
func BehaviorFingerprintKVKey(stream, durable string) string {
	return strings.TrimSpace(stream) + "/" + strings.TrimSpace(durable)
}

// SnapshotToKV converts library snapshots to the consol JSON shape.
func SnapshotToKV(s BehaviorSnapshot) BehaviorFingerprintKVSnapshot {
	return BehaviorFingerprintKVSnapshot{
		MsgPerMin:    s.MsgPerMin,
		ProcessingMs: float64(s.Processing) / float64(time.Millisecond),
	}
}

// MarshalBehaviorFingerprintKV encodes a consol-compatible fingerprint payload.
func MarshalBehaviorFingerprintKV(
	stream, durable string,
	anomaly bool,
	normal, current BehaviorSnapshot,
	sustained time.Duration,
	at time.Time,
) ([]byte, error) {
	if at.IsZero() {
		at = time.Now().UTC()
	}
	payload := BehaviorFingerprintKVPayload{
		Stream:         strings.TrimSpace(stream),
		Durable:        strings.TrimSpace(durable),
		Anomaly:        anomaly,
		Normal:         SnapshotToKV(normal),
		Current:        SnapshotToKV(current),
		SustainedForMs: sustained.Milliseconds(),
		UpdatedAt:      at.UTC(),
	}

	return json.Marshal(payload)
}

// ReportBehaviorFingerprintKV publishes a fingerprint snapshot for nats-consol.
func ReportBehaviorFingerprintKV(
	ctx context.Context,
	keys KeyValueKeys,
	bucket, stream, durable string,
	anomaly bool,
	normal, current BehaviorSnapshot,
	sustained time.Duration,
) error {
	if keys == nil {
		return fmt.Errorf("behavior fingerprint report: kv keys is nil")
	}
	bucket = strings.TrimSpace(bucket)
	if bytesconv.IsEmpty(bucket) {
		bucket = DefaultBehaviorFingerprintKVBucket
	}
	raw, err := MarshalBehaviorFingerprintKV(stream, durable, anomaly, normal, current, sustained, time.Now())
	if err != nil {
		return err
	}
	_, err = keys.Put(ctx, bucket, BehaviorFingerprintKVKey(stream, durable), raw)

	return err
}
