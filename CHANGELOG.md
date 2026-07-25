# Changelog

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
