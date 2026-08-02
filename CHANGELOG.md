# Changelog

## v0.6.0

- **Request/reply payload compression** — `RequesterConfig` / `ResponderConfig` `PayloadCompression` + `PayloadDecompression` (same >32 KiB br/gzip rules as publish). Reply compress via `Responder().Respond*`; package-level `Respond*` unchanged. Bench: `BenchmarkRequestReplyPayload`. See [performance.md](docs/performance.md).
- **Breaking:** removed named config presets (`DevConfig`, `ProdWorkerConfig`, `ProdFanOutConfig`, `ThroughputConfig`, `ProdWorkerStreamConfig`, `ProdFanOutStreamConfig`). Use `DefaultConfig()` plus documented recipes in [consumer tuning](docs/nats/consumer-tuning-guide.md) / [performance](docs/performance.md) / [devops](docs/nats/devops.md).
- **Pub/sub payload benches** — `BenchmarkPubSubPayload` matrix JSON/MsgPack/Proto × off/gzip/br (encode→publish→consume→decode); `PayloadCompressionGzip` / `PayloadCompressionBrotli` forced modes. See [performance.md](docs/performance.md).
- **Docs / benches:** refreshed payload + pub/sub + request/reply sample numbers; `make bench-payload`; baseline includes `BenchmarkRequestReplyPayload`; README / API / tuning updated for Adaptive Pressure, Capsule, Shadow Graduation, and config recipes.
- **Payload compression** (nats-consol parity): `PublisherConfig.PayloadCompression = PayloadCompressionAuto` compresses bodies strictly larger than 32 KiB with **br → gzip** (best-speed, shrink-only) and sets `Content-Encoding`. `RuntimeConsumer.PayloadDecompression` (default on) expands before handlers. Helpers: `MaybeCompressPayload`, `DecompressPayload`, `DecodeMsg` (no auto-decompress) / `DecodeMsgWithDecompress` (size-capped). Cap: `MaxPayloadDecompressBytes` (64 MiB). Benches: `BenchmarkPayloadCompression` / `BenchmarkPayloadDecompression` (see [performance.md](docs/performance.md)).
- **`SetupWorker` v2** — applies `MaxAckPending` / `AckWait` (subscribe opts + update) and MaxDeliver / Metadata / RateLimit / Replicas / MemStorage via `UpdateConsumer` after bound queue subscribe.
- **Incident Capsule** — `Client.Incidents().Capture` / `Load` / `List` / `ReplayLocal` packs JetStream messages + flight recorder (+ optional fingerprint) into Object Store; `CapsuleAuto` hooks for DLQ / shadow / fingerprint anomaly. `List(ctx, stream, consumer, indexBucket)` — empty `indexBucket` uses `DefaultIncidentIndexBucket`.
- **Adaptive Pressure** — `Config.AdaptivePressure` closed-loop adjusts pull `FetchBatch` and graduated `NakWithDelay` from pool depth / lag sensors (`DecideAdaptivePressure`).
- **Shadow Graduation** — `shadow.NewGraduate` ramps canary `SampleRate` on healthy windows, aborts on mismatch rate, signals `PromoteReady`.
- **DLQ autopsy** — `IncludeStack` now captures `runtime.Stack` into `X-NATS-Autopsy-Stack`.

### Reliability / correctness

- Shutdown stops health-check before clearing `conn`/`js`; drains async publishes (`PublishAsyncComplete`) then waits for connection drain (`IsClosed`) instead of treating `Drain()` as synchronous.
- Idempotency `ClaimStore`: pending vs done claims; in-flight returns `ErrClaimInFlight` (Nak, do not Ack); `Release` on handler or `Mark` failure so redelivery can reclaim.
- Shared worker-pool handlers no longer race across push subscriptions; pull `Process` WaitGroup / caller ctx cancel and continue after per-message Nak.
- Ack/Nak treat `ErrMsgAlreadyAckd` as settled; `BackpressureDrop` Terms (and logs) so messages leave Ack-pending.
- `DurableConsumerConfig.DeliverSubject` / `DeliverGroup` preserved across `ResetConsumer` / manager updates (push consumers).
- `CreateOrUpdateStream` preserves existing replica count when `Replicas` is unset on update.
- Decompress size cap (`MaxPayloadDecompressBytes`); URL auth conflict detection; monitoring HTTP deny-list includes IMDS ranges.
- Raw encode copies caller buffers for async publish safety.

## v0.5.0

- Depend on [tel](https://github.com/gopherust-io/tel) **v0.4.0** (pulls [env](https://github.com/gopherust-io/env) **v0.6.0**); goalign **v1.4.0**.
- Monitoring HTTP client: SSRF-safe dial/redirect checks, response body size cap (`ErrMonitoringBodyTooLarge`), cloned transport.
- Loadtest / bench tooling updates; competitive benches under `benchcmp/`; docs for performance baselines.

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
