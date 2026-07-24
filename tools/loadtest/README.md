# Load test harness

Publishes and consumes against a local JetStream server for CPU/memory comparison.

## Prerequisites

```bash
docker compose -f docker/nats/single/docker-compose.yml up -d
```

## Run

```bash
# Throughput profile (metrics off) + raw bytes
go run ./tools/loadtest -duration 30s -codec bytes -mode push -workers 8 -rate 1000

# Compare observability tax
go run ./tools/loadtest -duration 30s -codec json -metrics=false
go run ./tools/loadtest -duration 30s -codec json -metrics=true

# Pull batch
go run ./tools/loadtest -mode pull -batch 100 -workers 8 -codec bytes -duration 30s
```

## Flags

| Flag | Default | Meaning |
|------|---------|---------|
| `-nats` | `nats://127.0.0.1:4222` | NATS URL |
| `-duration` | `15s` | Run length |
| `-codec` | `json` | `json` or `bytes` |
| `-mode` | `push` | `push` or `pull` |
| `-workers` | `4` | Pool / pull concurrency |
| `-batch` | `50` | Pull fetch batch |
| `-metrics` | `false` | Use ProdWorkerConfig vs ThroughputConfig |
| `-rate` | `500` | Target publish rate |

See also [docs/performance.md](../../docs/performance.md).
