# nats

JetStream client for Go. Telemetry is optional via [tel](https://github.com/gopherust-io/tel).

Module: [`github.com/gopherust-io/nats`](https://github.com/gopherust-io/nats)

[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/gopherust-io/nats/badge)](https://scorecard.dev/viewer/?uri=github.com/gopherust-io/nats)

**Mental model:** the app **connects and binds**. Ops **owns streams** (CLI/platform). If no stream captures the subject, the message is **not retained**—that is topology, not a client bug.

```bash
go get github.com/gopherust-io/nats@latest
make nats-up   # local broker
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

## Presets

| Preset | When |
|--------|------|
| `DefaultConfig()` | Starting point; resilient reconnect |
| `DevConfig()` | Local; quieter reconnect, metrics off |
| `ProdWorkerConfig()` | Job queue; worker pool + Nak backpressure |
| `ProdFanOutConfig()` | Event bus; block backpressure |
| `ThroughputConfig()` | Max throughput; minimal observability |

Presets never invent stream topology.

## Extensions

| Need | Package / API |
|------|----------------|
| Consume dedup | `nats/idempotency` |
| Dead letter | `nats/dlq` |
| Shadow / canary | `nats/shadow` |
| Sharding | `nats/shard` |
| Worker pool | `nats/workerpool` |
| Replay | `client.Replay()` |
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
make nats-up
make demo-nats    # ROLE=all|publisher|worker|puller
make test
```

## Docs

| Guide | When |
|-------|------|
| [Getting started](docs/getting-started.md) | Clone → Docker → demos |
| [JetStream guide](docs/nats/README.md) | Publish / retain / consume |
| [Recipes](docs/nats/recipes.md) | Production configs |
| [Push vs pull](docs/nats/push-vs-pull.md) | Delivery model |
| [Performance](docs/performance.md) | Codecs, AttrCache, throughput |
| [Local Docker](docs/nats/local-docker.md) | Single / cluster / supercluster |
| [API reference](docs/nats/api-reference.md) | Binding, middleware, presets |

[Contributing](CONTRIBUTING.md) · [Changelog](CHANGELOG.md)

## License

Apache License 2.0 — see [LICENSE](LICENSE).
