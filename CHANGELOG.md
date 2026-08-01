# Changelog

## v0.4.0

- Depend on [tel](https://github.com/gopherust-io/tel) **v0.3.0** (pulls [env](https://github.com/gopherust-io/env) **v0.5.0**).
- goalign **v1.3.0** via `go install` (Makefile).
- Panic-safe message handlers (`invokeMsgHandler`) for push/pull paths.
- `ARCHITECTURE.md`; JetStream Compose labs moved to [nats-console](https://github.com/gopherust-io/nats-console) (`docker/nats` removed from this repo).
- Worker pool, shadow, replay, and consumer hardening; docs updated for nats-console labs.

## v0.3.0

- Replay peek returns `StoredMessage` (sequence, subject, time, data).
- `GetMsgRange` / `GetMsgRangeByTime` and time→seq helpers for export without moving consumers.
- Seek bounds: `UntilSeq`, `UntilTime`, `Limit`, `OneMessage`; returned on `ReplayConsumerResult` and stored in consumer metadata.
- `DurableConsumerConfig.Metadata` passed through to JetStream.
- **Slow consumer detection** — `EvaluateSlowConsumer`, `WatchSlowConsumer` / `Client.WatchSlowConsumer` for sustained JetStream backlog (pending / lag / ack-pending ratio); metric `slow_consumer_detected` (distinct from backpressure `slow_consumer_events`).
- **Consumer behavior fingerprinting** — `EvaluateBehaviorFingerprint`, `WatchBehaviorFingerprint` / `Client.WatchBehaviorFingerprint` learns normal msg/min + handling latency, then emits anomalies when throughput stays near baseline while processing regresses; metric `behavior_fingerprint_anomaly`. Snapshots publish to KV `nats_consol_fingerprints` for nats-consol Consumer Detail.

## v0.2.1

- Fix zero-value JetStream enum merges: `FromBeginning()`, `WithReplayPolicy(ReplayInstant)`,
  and `AckNone` now apply correctly via explicit set flags.
- `ResetConsumer` preserves `OptStartSeq` / `OptStartTime` unless seek opts change them.
- `CreateOrUpdateConsumer` treats any deliver-policy change (including to `DeliverAll`) as recreate-required.
- End OpenTelemetry spans on successful `PublishAsync`.
- Label stream/consumer JetStream gauges with stream / `stream.durable` via `RecordWith`.
- `pageSlice` / KV / Object `limit < 0` returns all items from offset (aligned with streams/consumers).
- Soft-skip `ErrConsumerNotFound` during `ListConsumersPage`.

## v0.2.0

- Admin APIs for console/ops: `AddStream`/`UpdateStream`, `ListStreamsPage`,
  `AddConsumer`/`UpdateConsumer`, `ConsumerNames`, `ListConsumersPage`.
- Package helpers `StreamNames` / `ListStreams` over `ListStreamsPage`.
- KV admin helpers: `ListBuckets`, `BucketInfo`, `CreateRaw`; key ops via `Client.KVKeys()`.
- New `Objects()` Object Store manager and `Monitoring().Fetch` for `/jsz`/`/varz`.
- `Connector`: `Conn()`, `JetStream()`, `AccountInfo()`; optional `OnDisconnect`/`OnReconnect`/`OnClosed` hooks.
- `Client.PublishRaw` returns JetStream `PubAck`.

## v0.1.1

- Depend on `github.com/gopherust-io/tel` v0.1.1 (tracing/metrics hot-path optimizations).
- Reduce span attribute slice allocations on publish/process paths.

## v0.1.0

- Initial release: split from `github.com/gopherust-io/libs/nats` as module `github.com/gopherust-io/nats`.
- Depends on `github.com/gopherust-io/tel` for metrics and tracing.
