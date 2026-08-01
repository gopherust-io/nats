# nats — Architecture

Opinionated JetStream client library: applications connect and bind; operations owns stream topology.

## Overview

`github.com/gopherust-io/nats` wraps `nats.go` with a `Client` façade over publishers, push/pull consumers, request/reply, KV, object store, replay, and supervisors. Presets (`Default` / `Dev` / `ProdWorker` / `ProdFanOut` / `Throughput`) encode common production shapes. Optional **tel** hooks provide OTLP metrics/traces. Leaf packages add idempotency, DLQ, shadow traffic, sharding helpers, and worker pools.

Ecosystem: [gopherust-io](https://github.com/gopherust-io/gopherust-io/blob/main/ARCHITECTURE.md) · Telemetry: [tel](https://github.com/gopherust-io/tel/blob/main/ARCHITECTURE.md)

## Layer / package overview

```
┌─────────────────────────────────────────────────────────────┐
│  Application                                                │
│  NewClient → Consumer / Publisher / KV / SetupWorker / …    │
└──────────────────────────┬──────────────────────────────────┘
                           │
┌──────────────────────────▼──────────────────────────────────┐
│  Client façade (package nats)                               │
│  connection, config presets, managers, ack helpers          │
└──────────┬───────────────────────────────┬──────────────────┘
           │                               │
┌──────────▼──────────┐         ┌──────────▼──────────────────┐
│  Core managers      │         │  Resilience / ops           │
│  publish, push/pull │         │  supervisor, soft,         │
│  KV, objects, R/R   │         │  soft-consumer, soft_liveness │
└──────────┬──────────┘         └──────────┬──────────────────┘
           │                               │
┌──────────▼───────────────────────────────▼──────────────────┐
│  nats.go / JetStream                                        │
└─────────────────────────────────────────────────────────────┘

Leaf packages (compose around handlers / subjects):
  idempotency/  dlq/  shadow/  shard/  workerpool/
```

## Packages

| Path | Responsibility |
|------|----------------|
| `nats` (root) | `Client`, config presets, connection, publish/consume, KV/objects, R/R, monitoring, replay, supervisor |
| `idempotency/` | Dedup / claim stores and handler wrappers |
| `dlq/` | Dead-letter wrappers and autopsy helpers |
| `shadow/` | Shadow-traffic middleware around handlers |
| `shard/` | Subject / index sharding helpers |
| `workerpool/` | Bounded worker pool for handler fan-out |
| `internal/bytesconv` | Shared zero-copy string/bytes helpers |
| `docs/` | Getting started, API reference, recipes, scaling |
| `examples/`, `bench/`, `tools/`, `docker/` | Demos, benches, loadtest, local NATS |

## Key design rules

- **Ops owns streams; apps bind:** library assumes streams/consumers are declared out-of-band (CLI/platform); clients attach with bind-oriented APIs.
- **Façade over managers:** prefer `Client` accessors (`Publisher`, `Consumer`, `KV`, …) rather than wiring raw JetStream handles in every call site.
- **Handler Ack contract:** handlers own Ack/Nak/Term; helpers (`InProgress`, `NakWithDelay`, `TermWithReason`) keep semantics explicit.
- **Presets over ad-hoc knobs:** start from `DefaultConfig` / `ProdWorkerConfig` / etc., then override.
- **Leaf packages compose:** idempotency, DLQ, and shadow wrap handlers; they do not fork the core client.
- **Telemetry optional:** metrics/tracing integrate with **tel** when present; core messaging must work without a collector.

## Core APIs / interfaces

```go
func NewClient(cfg Config) (*Client, error)

type Client struct { /* managers + connection */ }

func (c *Client) Publisher() *Publisher
func (c *Client) Consumer(…) (*Consumer, error)
func (c *Client) PullConsumer(…) (*PullConsumer, error)
func (c *Client) KV(…) (KeyValueManager, error)
func (c *Client) Objects(…) (ObjectStoreManager, error)
func (c *Client) SetupWorker(…) (*WorkerSetup, error)
func (c *Client) Supervise(…) (*SupervisedSubscription, error)

type MsgHandler func(ctx context.Context, msg Msg) error
```

## Request / call flow

Example: publish then consume on a bound consumer

1. `cfg := nats.ProdWorkerConfig(); cfg.URL = …` → `NewClient(cfg)`.
2. Dial/reconnect via connector; managers lazy-init against JetStream.
3. `Publisher().Publish…` encodes payload (JSON/MsgPack/Proto/bytes) and publishes to subject/stream.
4. `Consumer(…)` / `PullConsumer(…)` binds to an existing consumer; handler runs with explicit Ack/Nak.
5. Optional: wrap handler with `idempotency.WithHandler` or `dlq.With`; watch soft-liveness / slow-consumer for ops signals.
6. `Shutdown` / close connection when the process exits.

## Bootstrap / lifecycle

1. Build `Config` from a preset (or explicit fields).
2. `NewClient` establishes the connection and exposes managers.
3. Create publishers/consumers/workers; run until context cancel.
4. Close client / supervised subscriptions on shutdown; flush as required by config.

No standalone daemon—library lifecycle follows the host process.

## Adding a feature

1. Prefer extending the root `Client` / managers when the capability is core messaging.
2. Put optional cross-cutting behavior in a leaf package (`idempotency`, `dlq`, `shadow`, …) that wraps handlers or subjects.
3. Keep stream provisioning out of app hot paths; document ops steps under `docs/nats/`.
4. Add examples and update `docs/nats/api-reference.md` when the public surface changes.
5. If touching telemetry, keep **tel** optional and allocation-sensitive on the record path.

## Related docs

- [README](README.md)
- [Getting started](docs/getting-started.md)
- [API reference](docs/nats/api-reference.md)
- [Streams and subjects](docs/nats/streams-and-subjects.md)
- [Consumers and binding](docs/nats/consumers-and-binding.md)
- [Push vs pull](docs/nats/push-vs-pull.md)
- [Recipes](docs/nats/recipes.md)
- [Scaling](docs/nats/scaling.md)
- [Performance](docs/performance.md)
- [Compatibility](docs/compatibility.md)
- [docs/nats index](docs/nats/README.md)
