# NATS Consol integration contracts

Source of truth for wire formats shared between [`gopherust-io/nats`](https://github.com/gopherust-io/nats) and [`gopherust-io/nats-console`](https://github.com/gopherust-io/nats-console).

Consol **consumes** these contracts; this library **produces** them.

## Behavior fingerprints (KV)

| Field | Value |
|-------|--------|
| Bucket | `nats_consol_fingerprints` (`DefaultBehaviorFingerprintKVBucket`) |
| Key | `{stream}/{durable}` via `BehaviorFingerprintKVKey` |
| Codec | JSON (`BehaviorFingerprintKVPayload`) |

```json
{
  "updatedAt": "2026-08-21T12:00:00Z",
  "stream": "ORDERS",
  "durable": "orders-processor",
  "normal": { "msgPerMin": 120.0, "processingMs": 8.5 },
  "current": { "msgPerMin": 118.0, "processingMs": 42.0 },
  "sustainedForMs": 15000,
  "anomaly": true
}
```

Helpers: `MarshalBehaviorFingerprintKV`, `ReportBehaviorFingerprintKV`.

## Incident capsules (Object Store + KV index)

| Field | Value |
|-------|--------|
| Object bucket | `nats_incident_capsules` (`DefaultIncidentCapsuleBucket`) |
| Index KV bucket | `nats_incident_index` (`DefaultIncidentIndexBucket`) |
| Schema version | `Capsule.SchemaVersion` (currently `1`) |

Capsule JSON shape (`Capsule`):

| Field | Type | Notes |
|-------|------|--------|
| `schemaVersion` | int | Bump on breaking changes |
| `id` | string | Capsule ID / object name |
| `createdAt` | RFC3339 | UTC |
| `stream` / `consumer` | string | Source durable |
| `trigger` | string | `manual` \| `dlq` \| `anomaly` \| `shadow_mismatch` \| `stall` |
| `failingSeq` | uint64 | Optional |
| `subject` / `reason` | string | Optional |
| `messages` | array | `sequence`, `subject`, `time`, `data`, `header` |
| `flightTimeline` | array | Optional flight recorder events |
| `fingerprint` | object | Optional `BehaviorFingerprintKVPayload` |

Capture / load / list / local replay: `Client.Incidents()`. One-line handler wiring: `Incidents().AttachToHandler(...)`.

## Slow-consumer / adaptive pressure signals

Library metrics (when tel metrics enabled) that consol alert rules / dashboards should align with:

| Signal | Library surface |
|--------|-----------------|
| Slow consumer | `WatchSlowConsumer`, metric `slow_consumer_detected` |
| Behavior anomaly | `WatchBehaviorFingerprint`, metric `behavior_fingerprint_anomaly` |
| Circuit open | `CircuitConfig` + `CircuitBreaker.Opens()` (pull Process pauses) |

## Versioning

- Additive JSON fields are backward compatible.
- Renames / type changes require a `schemaVersion` bump and a consol migration note in both changelogs.
