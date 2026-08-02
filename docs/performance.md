# Performance Guide

How to minimize CPU and allocations on the **publish** and **consume** hot paths (`tel` + `nats`).

For how messages move through JetStream (store → durable → Ack), see [NATS JetStream Guide](nats/README.md). This page is only about reducing cost on those paths.

```mermaid
flowchart LR
  pub[Publish_encode] --> js[JetStream_store]
  js --> del[Push_or_Pull]
  del --> decode[Decode_handler]
  decode --> ack[Ack]
```

| Path | Cost drivers | Library knobs |
|------|--------------|---------------|
| Publish | Codec, headers, subject validation, sync vs async, optional compress | Max-QPS recipe, Proto/`PublishBytes`, `SkipSubjectValidation`, `PublishAsync`, `PayloadCompression` |
| Consume | Decode, decompress, pool/backpressure, metrics/trace, fetch size | Worker pool / `WithProcessConcurrency`, `AdaptivePressure`, Lite metrics, batch size |

## Decision tree

```
Need max throughput on a hot subject?
├── Yes → DefaultConfig + job-worker knobs + metrics/tracing off + SkipSubjectValidation
│         + Proto or PublishBytes (see recipe below)
│         └── Dedup required? → PublishBytesWithMsgID / PublishWithMsgID (accept header path)
└── Need full observability?
    ├── Yes → DefaultConfig (+ job-worker knobs if competing workers)
    └── Dev / local → DefaultConfig + no reconnect + metrics off
```

## Recommended configurations

Always start from `DefaultConfig()`. Apply the fields below for your case.

### Job queue client (monitoring on)

```go
cfg := libnats.DefaultConfig()
cfg.RuntimeConsumer.WorkerPoolEnabled = true
cfg.RuntimeConsumer.WorkerPoolSize = 8
cfg.RuntimeConsumer.WorkerBufferSize = 256
cfg.RuntimeConsumer.AckWait = 45 * time.Second
cfg.RuntimeConsumer.PendingMsgLimit = 1000
cfg.RuntimeConsumer.PendingMsgBuffer = 10 << 20 // 10 MiB
cfg.Backpressure.Mode = libnats.BackpressureNak
cfg.Backpressure.MaxAckPending = 1000
```

### Max QPS / load tests (observability minimized)

```go
cfg := libnats.DefaultConfig()
// …same job-worker knobs as above…
cfg.PublisherConfig.AllowMetrics = false
cfg.PublisherConfig.AllowTracing = false
cfg.PublisherConfig.SkipSubjectValidation = true
cfg.RequesterConfig.AllowMetrics = false
cfg.RequesterConfig.AllowTracing = false
cfg.RequesterConfig.SkipSubjectValidation = true
cfg.ResponderConfig.AllowMetrics = false
cfg.ResponderConfig.AllowTracing = false
cfg.RuntimeConsumer.AllowMetrics = false
cfg.RuntimeConsumer.AllowTracing = false
cfg.Conn.AllowMetrics = false
cfg.Metrics.AllowMetrics = false
cfg.Metrics.AllowTracing = false
cfg.Metrics.CollectInterval = 60 * time.Second
cfg.Metrics.Lite = true
cfg.Metrics.FixedCardinality = true
cfg.Conn.Address = os.Getenv("NATS_URL")
// Optional fail-fast while disconnected:
// cfg.Conn.ReconnectBufSize = -1
```

### Local / CI

```go
cfg := libnats.DefaultConfig()
cfg.Conn.AllowReconnect = false
cfg.Conn.RetryOnFailedConnect = false
cfg.Conn.InitialRetryAttempts = 1
// turn AllowMetrics / AllowTracing off on Conn, Publisher, Requester, Responder, RuntimeConsumer, Metrics
cfg.RuntimeConsumer.WorkerPoolEnabled = true
cfg.RuntimeConsumer.WorkerPoolSize = 2
cfg.RuntimeConsumer.WorkerBufferSize = 32
```

## Codec selection

See [README codec benchmarks](../README.md#codec-comparison-benchmarks) and [benchmarks methodology](benchmarks.md).

| Codec | When |
|-------|------|
| **Protobuf** / `PublishProto` | Highest throughput, smallest payload |
| **Raw** / `PublishBytes` | Pre-encoded payloads (zero encode cost) |
| **MessagePack** | Binary structs without `.proto` |
| **JSON** | Debugging / interoperability |

## AttrCache warmup

Avoid cold-path CoW insert + heap on first message per subject:

```go
for _, s := range []string{"orders.created", "orders.>"} {
    tel.Registry().AttrCache().SubjectOpts(s)
}
```

Keep subject cardinality bounded (`METRICS_CARDINALITY_DETECTOR_MAX_CARDINALITY`, default 100). Prefer `MetricsConfig.FixedCardinality` when subject labels are not needed.

## Background poll budget

| Component | Default | Throughput mode |
|-----------|---------|-----------------|
| `MetricsConfig.CollectInterval` | 15s | 60s (max-QPS recipe) |
| Soft liveness `PollInterval` | 2s | 5–10s |
| Supervisor | 1s | `HealthyBackoffInterval` after healthy streak |
| OTLP export | `TEL_METRIC_PERIODIC_INTERVAL_SEC` (15) | Raise or set `TEL_EXPORT_INTERVAL_SEC` |

## Hot-path library features

- Trace headers injected only when the span is recording
- `SkipSubjectValidation` for trusted static subjects
- `PublishAsync` + `PublishAsyncComplete` for batched publish
- Pull `WithProcessConcurrency(n)` uses a fixed worker set (no per-message goroutine churn)
- Worker pool `TryPublishNonBlocking` for `BackpressureNak`
- Idempotency: `idempotency.NewBloomStore` for in-memory probabilistic Seen
- Payload compression (opt-in) — see below

## Payload compression (nats-consol parity)

Compresses wire payloads **only when strictly larger than 32 KiB**, prefers **brotli (`br`)** then **gzip**, and only if the compressed body is smaller. Sets `Content-Encoding`.

- **JetStream:** `PublisherConfig.PayloadCompression` + `RuntimeConsumer.PayloadDecompression` (default on)
- **Request/reply:** `RequesterConfig` / `ResponderConfig` each have `PayloadCompression` (default Off) and `PayloadDecompression` (default on). Reply compression applies only via `client.Responder().Respond*` methods; package-level `Respond*` leave bodies uncompressed.

```go
cfg := libnats.DefaultConfig()
cfg.PublisherConfig.PayloadCompression = libnats.PayloadCompressionAuto // br→gzip
// cfg.PublisherConfig.PayloadCompression = libnats.PayloadCompressionGzip // or Brotli
cfg.RequesterConfig.PayloadCompression = libnats.PayloadCompressionAuto
cfg.ResponderConfig.PayloadCompression = libnats.PayloadCompressionAuto
// PayloadDecompression defaults true on RuntimeConsumer / Requester / Responder
```

Skip double-compress: if `Content-Encoding` is already set (e.g. DLQ passthrough), publish/request leave bytes alone.

### Sample benches — pure compress helpers (darwin/arm64, Apple M4 Pro; `benchtime=30x`, directional)

```bash
make bench-payload
# or: go test -bench='BenchmarkPayload|BenchmarkPubSubPayload|BenchmarkRequestReplyPayload' -benchmem -count=1 -run='^$' .
```

| Bench | ns/op (approx) | B/op · allocs | bytes_out / ratio |
|-------|----------------|---------------|-------------------|
| `PayloadCompression/gzip/40KiB` | ~154µs | ~815KiB · 16 | 205 / 0.005 |
| `PayloadCompression/br/40KiB` | ~103µs | ~363KiB · 4 | 134 / 0.003 |
| `PayloadCompression/gzip/64KiB` | ~118µs | ~815KiB · 15 | 280 / 0.004 |
| `PayloadCompression/br/64KiB` | ~88µs | ~412KiB · 4 | 141 / 0.002 |
| `PayloadCompression/gzip/512KiB` | ~210µs | ~816KiB · 15 | 1647 / 0.003 |
| `PayloadCompression/br/512KiB` | ~202µs | ~1.3MiB · 4 | 214 / 0.0004 |
| `PayloadDecompression/gzip` (64KiB plain) | ~41µs | ~177KiB · 24 | — |
| `PayloadDecompression/br` (64KiB plain) | ~78µs | ~274KiB · 30 | — |

### Sample benches — pub/sub end-to-end (64 KiB body; `benchtime=30x`, directional)

Full path: encode → publish (optional compress) → JetStream deliver → decompress → `DecodeMsg`.

| Bench | ns/op (approx) | B/op · allocs | bytes_out / ratio |
|-------|----------------|---------------|-------------------|
| `PubSubPayload/json/off` | ~220µs | ~1.4MiB · 90 | 65547 / 1.00 |
| `PubSubPayload/json/gzip` | ~270µs | ~2.2MiB · 152 | 290 / 0.004 |
| `PubSubPayload/json/br` | ~275µs | ~1.9MiB · 153 | 142 / 0.002 |
| `PubSubPayload/msgpack/off` | ~156µs | ~609KiB · 97 | 65547 / 1.00 |
| `PubSubPayload/msgpack/gzip` | ~198µs | ~1.1MiB · 150 | 295 / 0.005 |
| `PubSubPayload/msgpack/br` | ~232µs | ~936KiB · 139 | 144 / 0.002 |
| `PubSubPayload/proto/off` | ~152µs | ~577KiB · 95 | 65540 / 1.00 |
| `PubSubPayload/proto/gzip` | ~180µs | ~1.1MiB · 142 | 279 / 0.004 |
| `PubSubPayload/proto/br` | ~200µs | ~889KiB · 135 | 139 / 0.002 |

Best-speed levels; compressible fixture (same threshold semantics as nats-consol). Prefer `br` on publish for large JSON; keep compression **off** on tiny payloads. Proto/MsgPack often win on CPU even before compress; compress mainly cuts wire size.

### Sample benches — request/reply end-to-end (64 KiB body; `benchtime=30x`, directional)

Full path: encode → compress request → core NATS round-trip → decompress → decode → compress reply → decompress reply. Same mode on requester and responder.

| Bench | ns/op (approx) | B/op · allocs | bytes_out / ratio |
|-------|----------------|---------------|-------------------|
| `RequestReplyPayload/json/off` | ~299µs | ~1.4MiB · 67 | 65547 / 1.00 |
| `RequestReplyPayload/json/gzip` | ~488µs | ~5.0MiB · 177 | 290 / 0.004 |
| `RequestReplyPayload/json/br` | ~437µs | ~3.3MiB · 160 | 142 / 0.002 |
| `RequestReplyPayload/msgpack/off` | ~219µs | ~681KiB · 77 | 65547 / 1.00 |
| `RequestReplyPayload/msgpack/gzip` | ~390µs | ~2.4MiB · 173 | 295 / 0.005 |
| `RequestReplyPayload/msgpack/br` | ~463µs | ~1.9MiB · 159 | 144 / 0.002 |
| `RequestReplyPayload/proto/off` | ~213µs | ~607KiB · 66 | 65540 / 1.00 |
| `RequestReplyPayload/proto/gzip` | ~386µs | ~2.2MiB · 158 | 279 / 0.004 |
| `RequestReplyPayload/proto/br` | ~443µs | ~1.7MiB · 142 | 139 / 0.002 |

RR pays compress+decompress on both directions; off is often faster on CPU for local loopback, while gzip/br cut wire size the same as pub/sub.

### Adaptive pressure (CPU only)

`DecideAdaptivePressure` is a pure function — no I/O. Bench: `BenchmarkDecideAdaptivePressure`.

## Adaptive Pressure

Opt-in closed loop for pull fetch batch + graduated Nak delay when the worker pool fills:

```go
cfg := libnats.DefaultConfig()
cfg.RuntimeConsumer.WorkerPoolEnabled = true
cfg.AdaptivePressure = libnats.AdaptivePressureConfig{
    Enabled:       true,
    MinFetchBatch: 1,
    MaxFetchBatch: 256,
}
// Explicit WithFetchBatch(N) still wins for that Process call.
```

Sensors: pool depth, optional latency/lag inputs. Actuators: dynamic fetch batch on `Pull.Process`, `NakWithDelay` under `BackpressureNak`. See [consumer tuning](nats/consumer-tuning-guide.md#adaptive-pressure).

## Measuring

```bash
./scripts/bench-baseline.sh
./scripts/bench-baseline.sh --out bench/after.txt
benchstat bench/baseline.txt bench/after.txt
make bench-compete
make bench-payload
```

Details: [benchmarks.md](benchmarks.md) · [bench/README.md](../bench/README.md) · [Consumer tuning guide](nats/consumer-tuning-guide.md) · [Load test](../tools/loadtest/) · [Production operations](nats/devops.md) (client + server HA / security)
