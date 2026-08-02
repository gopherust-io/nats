# Load test harness

Publishes and consumes against a local JetStream server for CPU/memory comparison.

## Prerequisites

```bash
# nats-console: make nats-up
# or: nats-server -js
```

## Run

```bash
# Throughput profile (metrics off) + raw bytes
go run ./tools/loadtest -duration 30s -codec bytes -mode push -workers 8 -rate 1000

# Side-by-side vs legacy nats.go
go run ./tools/loadtest -impl gopherust -codec bytes -mode push -duration 30s
go run ./tools/loadtest -impl natsgo   -codec bytes -mode push -duration 30s

# Compare observability tax (gopherust only)
go run ./tools/loadtest -duration 30s -codec json -metrics=false
go run ./tools/loadtest -duration 30s -codec json -metrics=true

# Pull batch
go run ./tools/loadtest -mode pull -batch 100 -workers 8 -codec bytes -duration 30s
```

## Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-nats` | `nats://127.0.0.1:4222` | NATS URL |
| `-impl` | `gopherust` | `gopherust` or `natsgo` |
| `-duration` | `15s` | Run length |
| `-codec` | `json` | `json` or `bytes` |
| `-mode` | `push` | `push` or `pull` |
| `-workers` | `4` | Pool / pull concurrency |
| `-batch` | `50` | Pull fetch batch |
| `-metrics` | `false` | Job-worker knobs with metrics on vs max-QPS (metrics off) for gopherust |
| `-rate` | `500` | Target publish rate |

See also [docs/performance.md](../../docs/performance.md).
