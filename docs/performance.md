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
| Publish | Codec, headers, subject validation, sync vs async | `ThroughputConfig`, Proto/`PublishBytes`, `SkipSubjectValidation`, `PublishAsync` |
| Consume | Decode, pool/backpressure, metrics/trace, fetch size | Worker pool / `WithProcessConcurrency`, Lite metrics, batch size |

## Decision tree

```
Need max throughput on a hot subject?
├── Yes → ThroughputConfig() + Proto or PublishBytes
│         └── Dedup required? → PublishBytesWithMsgID / PublishWithMsgID (accept header path)
└── Need full observability?
    ├── Yes → DefaultConfig / ProdWorkerConfig (metrics + tracing on)
    └── Dev / local → DevConfig()
```

## Presets

| Preset | Observability | Best for |
|--------|---------------|----------|
| `DefaultConfig()` | On | General services |
| `ProdWorkerConfig()` | On + worker pool | Job queues with monitoring |
| `ThroughputConfig()` | Off / lite | Inner loops, load tests |
| `DevConfig()` | Off | Local demos |

```go
cfg := libnats.ThroughputConfig()
cfg.Conn.Address = os.Getenv("NATS_URL")
// Optional fail-fast while disconnected:
// cfg.Conn.ReconnectBufSize = -1
```

## Codec selection

See [README codec benchmarks](../README.md#codec-comparison-benchmarks).

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
| `MetricsConfig.CollectInterval` | 15s | 60s (`ThroughputConfig`) |
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

## Measuring

```bash
./scripts/bench-baseline.sh
benchstat bench/baseline.txt <after.txt>
```

Details: [bench/README.md](../bench/README.md) · [Consumer tuning guide](nats/consumer-tuning-guide.md) · [Load test](../tools/loadtest/) · [Production operations](nats/devops.md) (client + server HA / security)
