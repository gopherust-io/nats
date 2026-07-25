# Changelog

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
