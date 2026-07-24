# nats

JetStream client for Go: streams, consumers (push/pull), codecs, worker pool, backpressure, metrics, and distributed tracing.

**Import:** `github.com/gopherust-io/nats`

Depends on [`github.com/gopherust-io/tel`](https://github.com/gopherust-io/tel) for optional OpenTelemetry metrics and traces.

## Packages

| Package | Import | Purpose |
|---------|--------|---------|
| `nats` | `github.com/gopherust-io/nats` | JetStream client: streams, consumers, publish/consume, replay, metrics, traces |
| `nats/workerpool` | `github.com/gopherust-io/nats/workerpool` | Fixed-size goroutine pool used by the push consumer |
| `nats/idempotency` | `github.com/gopherust-io/nats/idempotency` | Consume-side dedup (KV / Bloom) middleware |
| `nats/dlq` | `github.com/gopherust-io/nats/dlq` | Dead-letter + autopsy middleware |
| `nats/shadow` | `github.com/gopherust-io/nats/shadow` | Shadow/canary dual-run middleware |
| `nats/shard` | `github.com/gopherust-io/nats/shard` | Stable subject sharding helpers |

## Quick start

```bash
go get github.com/gopherust-io/nats@latest
make nats-up && make demo-nats
```

```go
import (
	"context"

	libnats "github.com/gopherust-io/nats"
	"github.com/gopherust-io/tel"
)

cfg := libnats.DefaultConfig()
telem := tel.NewWithConfig(tel.DefaultDebugConfig())
ctx := tel.WrapContext(context.Background(), telem)
client, err := libnats.NewClient(ctx, &cfg)
```

## Documentation

| Guide | Read when you need… |
|-------|---------------------|
| [Getting started](docs/getting-started.md) | Setup path (deps → Docker NATS → examples → tests) |
| [NATS JetStream Guide](docs/nats/README.md) | Publish, consume, retention, durables |
| [Performance](docs/performance.md) | ThroughputConfig, codecs, AttrCache |
| [Recipes](docs/nats/recipes.md) | Copy-paste production configs |
| [Local Docker](docs/nats/local-docker.md) | Single / cluster / supercluster |
| [API reference](docs/nats/api-reference.md) | Bound subscribe, presets, middleware |

Also: [Contributing](CONTRIBUTING.md) · [Changelog](CHANGELOG.md).

## Development

```bash
make nats-up
make test
make test-race
make lint
make demo-nats
```
