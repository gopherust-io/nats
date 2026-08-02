# nats

JetStream client for Go. Telemetry is optional via [tel](https://github.com/gopherust-io/tel).

Designed for teams that want explicit JetStream topology ownership and predictable runtime behavior in production services.

Module: [`github.com/gopherust-io/nats`](https://github.com/gopherust-io/nats)

[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/gopherust-io/nats/badge)](https://scorecard.dev/viewer/?uri=github.com/gopherust-io/nats)

Latest stable release: see [GitHub Releases](https://github.com/gopherust-io/nats/releases).

Quick links: [Architecture](ARCHITECTURE.md) · [Getting started](docs/getting-started.md) · [API reference](docs/nats/api-reference.md) · [Compatibility](docs/compatibility.md)

---

**Mental model:** the app **connects and binds**. Ops **owns streams** (CLI/platform). If no stream captures the subject, the message is **not retained**—that is topology, not a client bug.

```bash
go get github.com/gopherust-io/nats@latest
# local broker: nats-server -js  — or labs in nats-console (make nats-up)
```

```bash
# production topology — not NewClient
nats stream add ORDERS --subjects 'orders.>' --storage file --retention workqueue
```

Labs may call `Streams()` / `Consumers()` / `SetupWorker` explicitly. See [API reference](docs/nats/api-reference.md). `cfg.Stream` on `NewClient` does not create anything.

## Worker sketch

```go
package main

import (
	"context"
	"log"

	libnats "github.com/gopherust-io/nats"
	"github.com/gopherust-io/tel"
	natspkg "github.com/nats-io/nats.go"
)

func main() {
	ctx := context.Background()

	telem := tel.NewWithConfig(tel.DefaultDebugConfig()) // prod: DefaultConfig + collector
	_ = telem.Start(ctx)
	defer telem.Shutdown(ctx)
	ctx = tel.WrapContext(ctx, telem)

	cfg := libnats.DefaultConfig()
	cfg.Conn.Address = "nats://127.0.0.1:4222"

	client, err := libnats.NewClient(ctx, &cfg)
	if err != nil {
		log.Fatal(err)
	}
	defer client.Connector().Shutdown()

	// Stream must already exist (CLI above). Then:
	if err := client.Publisher().PublishJSON(ctx, "orders.created", map[string]any{"id": "42"}); err != nil {
		log.Fatal(err)
	}

	handler := func(ctx context.Context, msg *natspkg.Msg) error {
		// nil → Ack; non-nil → Nak / retry
		return nil
	}

	_, err = client.Consumer().QueueSubscribeBound(
		ctx,
		"ORDERS",           // stream
		"orders-processor", // durable
		"orders-workers",   // queue
		"orders.>",         // filter
		handler,
	)
	if err != nil {
		log.Fatal(err)
	}

	select {}
}
```

Publish variants: `PublishBytes`, `PublishWithMsgID` (requires stream `DuplicateWindow` + stable IDs). Pick one codec per stream and stay consistent.

## Pull

Create the pull durable first (CLI or `Consumers().CreateOrUpdateConsumer`), then:

```go
pull, err := client.Consumer().Pull("ORDERS", "orders-puller")
if err != nil {
	return err
}
return pull.Process(ctx, handler)
```

## Request / Reply (core NATS)

JetStream is not involved. `Requester` calls `Request*`; `Responder` uses core subscribe (no auto Ack/Nak). Handler must `Respond*`. Codecs match Publisher: Bytes, JSON, MessagePack, Proto.

```go
_, err = client.Responder().Subscribe(ctx, "echo", func(_ context.Context, msg *natspkg.Msg) error {
	return libnats.RespondJSON(msg, map[string]any{"ok": true})
})

var resp map[string]any
err = client.Requester().RequestJSONInto(ctx, "echo", map[string]any{"ping": 1}, &resp)
```

Also: `RequestBytes` / `RequestMsgPackInto` / `RequestProtoInto`, and `RespondBytes` / `RespondMsgPack` / `RespondProto`. Queue groups: `Responder().QueueSubscribe`. Default request timeout is 2s when `ctx` has no deadline (`RequesterConfig.Timeout`).

Do **not** request on subjects captured by a JetStream stream—the server PubAck is delivered to the reply inbox. Auth that blocks `_INBOX.>` also breaks requests — see [devops](docs/nats/devops.md).

## Recommended configurations

Start from `DefaultConfig()`, then apply knobs for your workload. Stream topology is always explicit (`CreateOrUpdateStream` / ops CLI) — the client factory does not invent it.

```go
cfg := libnats.DefaultConfig()

// Job queue client (competing workers):
cfg.RuntimeConsumer.WorkerPoolEnabled = true
cfg.RuntimeConsumer.WorkerPoolSize = 8
cfg.RuntimeConsumer.WorkerBufferSize = 256
cfg.RuntimeConsumer.AckWait = 45 * time.Second
cfg.RuntimeConsumer.PendingMsgLimit = 1000
cfg.RuntimeConsumer.PendingMsgBuffer = 10 << 20 // 10 MiB
cfg.Backpressure.Mode = libnats.BackpressureNak
cfg.Backpressure.MaxAckPending = 1000

// Job stream (HA cluster):
stream := libnats.StreamConfig{
    Name: "ORDERS", Subjects: []string{"orders.>"},
    Storage: libnats.FileStorage, Retention: libnats.WorkQueuePolicy,
    Replicas: 3, Discard: libnats.DiscardOld,
}
```

More recipes (fan-out, local/dev, max QPS): [consumer tuning](docs/nats/consumer-tuning-guide.md) · [performance](docs/performance.md) · [devops](docs/nats/devops.md).

## vs nats.go

This module is an **opinionated façade over legacy `nats.go` JetStream** (`JetStreamContext`): bind-first consume, codecs, optional [tel](https://github.com/gopherust-io/tel) observability, and ops helpers (`idempotency`, `dlq`, `shadow`, …). Escape hatches: concrete client `Conn()` / `JetStream()`.

It is **not** a faster protocol client. Competitive benches measure **wrapper tax** on the max-throughput path against equivalent legacy JetStream calls—not broker throughput.

## Benchmarks

<a id="codec-comparison-benchmarks"></a>

### Codec comparison benchmarks

Internal codec matrix (`make bench-codec`): prefer **Protobuf** / `PublishProto` or raw **`PublishBytes`** on hot paths; JSON is for interoperability. See [docs/performance.md](docs/performance.md).

### Payload compression

Large bodies (`>32 KiB`): `PayloadCompressionAuto` (br→gzip), or forced `PayloadCompressionGzip` / `PayloadCompressionBrotli` on **publish**, **request**, and **`Responder().Respond*`** (package-level `Respond*` stay uncompressed). Pure + pub/sub + request/reply benches: `make bench-payload` — [docs/performance.md § Payload compression](docs/performance.md#payload-compression-nats-consol-parity).

### Wrapper tax vs legacy nats.go

Same embedded JetStream broker, bytes payload, metrics/tracing off (max-throughput recipe), worker pool off. Sample (darwin/arm64, Apple M4 Pro; directional):

| Path | gopherust | nats.go | note |
|------|----------:|--------:|------|
| Sync Publish + PubAck | ~71 µs, 36 allocs | ~55 µs, 34 allocs | small wrapper tax |
| Async Publish + wait | ~51 µs, 42 allocs | ~56 µs, 42 allocs | ≈ parity |
| Push consume + Ack | ~62 µs, 67 allocs | ~65 µs, 64 allocs | ≈ parity |
| Pull fetch + Ack | ~112 µs, 105 allocs | ~104 µs, 91 allocs | small tax |

```bash
make bench-compete
make bench-payload
# system load (external broker):
go run ./tools/loadtest -impl gopherust -codec bytes -mode push -duration 30s
go run ./tools/loadtest -impl natsgo   -codec bytes -mode push -duration 30s
```

Methodology: [docs/benchmarks.md](docs/benchmarks.md) · sample: [benchcmp/results.sample.txt](benchcmp/results.sample.txt) · baselines: [./scripts/bench-baseline.sh](scripts/bench-baseline.sh).

## Extensions

| Need | Package / API |
|------|----------------|
| Consume dedup | `nats/idempotency` |
| Dead letter | `nats/dlq` (+ autopsy `IncludeStack`) |
| Shadow / canary | `nats/shadow` · `shadow.NewGraduate` for ramp/abort |
| Sharding | `nats/shard` |
| Worker pool | `nats/workerpool` |
| Replay | `client.Replay()` |
| Incident capsule | `client.Incidents()` · `CapsuleAuto` |
| Adaptive backpressure | `Config.AdaptivePressure` |
| Payload compress | `PublisherConfig` / `RequesterConfig` / `ResponderConfig` `.PayloadCompression` |
| Supervised sub | `SuperviseQueueSubscribeBound`, `SupervisePullProcess` |

## Footguns

| Symptom | Cause | Fix |
|---------|-------|-----|
| Publish “works”, nothing stored | Subject not captured by any stream | Fix stream `Subjects` / CLI |
| Wrong or missing deliveries | Stream / durable / filter mismatch | Use `*SubscribeBound` |
| Poison keeps disappearing | Handler returns nil after failure | Return error to Nak |
| Stream missing after deploy | Relied on `cfg.Stream` in `NewClient` | CLI or `Streams().CreateOrUpdateStream` |
| Request gets `{"stream":...}` JSON | Subject captured by a JetStream stream | Use core-only subjects for R/R |
| Consumer update rejected | Deliver policy change on existing durable | Recreate durable; see consumer docs |

## Development

```bash
# Start JetStream (nats-console: make nats-up — or nats-server -js)
make demo-nats    # ROLE=all|publisher|worker|puller
make test
make bench-compete
make loadtest
```

## Docs

| Guide | When |
|-------|------|
| [Getting started](docs/getting-started.md) | Clone → Docker → demos |
| [JetStream guide](docs/nats/README.md) | Publish / retain / consume |
| [Recipes](docs/nats/recipes.md) | Production configs |
| [Push vs pull](docs/nats/push-vs-pull.md) | Delivery model |
| [Performance](docs/performance.md) | Codecs, AttrCache, throughput |
| [Benchmarks](docs/benchmarks.md) | Methodology + compete vs nats.go |
| [Local Docker](docs/nats/local-docker.md) | Compose labs (in nats-console) |
| [API reference](docs/nats/api-reference.md) | Binding, middleware, DefaultConfig |

[Contributing](CONTRIBUTING.md) · [Changelog](CHANGELOG.md)

## Compatibility and support

- Supported Go/NATS versions are tracked in [docs/compatibility.md](docs/compatibility.md).
- `main` is active development; use tagged releases for production.
- Stream topology is intentionally externalized (CLI/platform) for operational control.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
