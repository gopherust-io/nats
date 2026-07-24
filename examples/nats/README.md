# NATS orders example (production-shaped)

Demonstrates **publish → JetStream WorkQueue → consume** (push worker and optional pull) with telemetry and ops toolkit (Supervisor, Soft Liveness, DLQ + Autopsy, Flight Recorder, Shadow).

Concept overview: [How JetStream works](../../docs/nats/README.md#how-jetstream-works). Tuning methodology: [Consumer tuning guide](../../docs/nats/consumer-tuning-guide.md).

## Architecture

```text
Publisher  --orders.created + Nats-Msg-Id-->  ORDERS (WorkQueue)
                                                    |
                              +---------------------+---------------------+
                              |                                           |
                       orders-processor                              orders-puller
                              |                                           |
                    queue: orders-workers                          Pull Process
                              |                                    (fetch batch 100)
                           Worker(s)
                              |
              poison every 17th id --> ORDERS_DLQ
```

| Stage | What happens |
|-------|----------------|
| Publish | Encode + optional Msg-Id → stream stores if `Subjects` match |
| Push consume | Server delivers to queue group → handler → Ack/Nak |
| Pull consume | App `Fetch`es batches → handler → Ack/Nak |

| `ROLE` | Behavior |
|--------|----------|
| `all` (default) | Publisher + push worker in one process (local demo) |
| `publisher` | Publish loop only |
| `worker` | Push queue consumer (run multiple for horizontal scale) |
| `puller` | Pull ingester with `WithFetchBatch(100)` (run multiple for pull scale) |

## How the code is wired

### Startup (`main.go`)

1. Builds telemetry, then **pre-warms** `AttrCache` for `orders.created`, `orders.>`, and `orders.dlq.poison` so the first message does not pay a cold-path CoW insert + heap alloc.
2. Picks config by role (`buildWorkerConfig` vs `buildPullConfig`).
3. Ensures topology: `ORDERS` (WorkQueue), `ORDERS_DLQ` (Limits), and pull durable `orders-puller` via `CreateOrUpdateConsumer` (pull durables must exist before `Pull`).

### Push worker config (`config.go` → `buildWorkerConfig`)

Baseline is `ProdWorkerConfig()` (job queue: WorkQueue, worker pool, `BackpressureNak`). Local demo overrides only:

| Setting | Value | Why |
|---------|-------|-----|
| `Replicas` / `Storage` | `1` / memory | Single local server; production keeps ProdWorker stream defaults |
| `DuplicateWindow` | 2m | Publish Msg-Id dedup window |
| Worker pool | enabled (8 / 256 from preset) | Push throughput; keep until metrics say otherwise |
| `AckWait` | `3 × handlerP99` | 2–3× rule; set on runtime config (durable AckWait via subscribe opts in real services) |
| `handlerP99` | 15s | Demo placeholder — replace with your real handler p99 (e.g. NATS handling histogram). Covers simulated slow paths and DLQ |
| `IdleHeartbeat` / `FlowControl` | off | Queue groups cannot use idle heartbeat (nats.go) |
| Backpressure | `BackpressureNak`, MaxAckPending 1000 | Bursty job queues; 1000 mirrors ProdWorker default |
| Metrics tracking | `ORDERS`, `ORDERS_DLQ`, durable `orders-processor` | Lag + pool depth gauges |

### Pull config (`config.go` → `buildPullConfig` + `puller.go`)

| Setting | Value | Why |
|---------|-------|-----|
| Preset | `DefaultConfig()` + stream WorkQueue | Pull path does not need the push worker pool |
| Worker pool | **off** | The `Process` loop is the worker |
| `AckWait` | `3 × (2 × handlerP99)` | Batch handlers can take longer than single-message push |
| Fetch | `WithFetchBatch(100)`, MaxWait 3s, heartbeat 500ms | Layer D — pull fetch/process tuning |
| Durable | Pre-created `orders-puller`, `MaxWaiting: 512` | Required before `Consumer().Pull` |

### Push worker runtime (`worker.go`)

| Piece | Role |
|-------|------|
| Primary handler | Decode JSON; on `id % 17 == 0` return `ErrSendToDLQ` so DLQ + autopsy show up in demos |
| Shadow handler | Optional canary (`SampleRate: 0.25`); same logic today — swap for a v2 handler when migrating |
| `WithDLQ` + `client.WithShadow` | Poison → `orders.dlq.poison` / `ORDERS_DLQ`; FlightRecorder + `shadow_*` metrics |
| `SuperviseQueueSubscribeBound` | Auto-resubscribe when interest is lost (tuning guide §4 / §7) |
| `WatchSoftLiveness` | Stall detection for queue workers (no IdleHeartbeat) |
| Shutdown | Stops liveness + sub; dumps non-empty FlightRecorder JSON to stderr |

### Telemetry (`tel.go`)

Monitor HTTP is **disabled** so multiple local roles (`publisher` + `worker` + `puller`) do not fight over the same port. OTLP follows `DefaultConfig` unless `TEL_ENABLE=true|false`.

### Publisher (`publisher.go`)

One message per second with stable `Nats-Msg-Id` (`order-N`). `CODEC=json` (default) or `raw`/`bytes` via `PublishBytesWithMsgID`.

## Prerequisites

NATS with JetStream:

```bash
# Option A: local binary
nats-server -js

# Option B: Compose (repo)
docker compose -f docker/nats/single/docker-compose.yml up -d
curl -s http://127.0.0.1:8222/healthz
```

Optional OTLP collector on `127.0.0.1:4317` (library default). Metrics/traces export when `TEL_ENABLE` is not forced off.

## Run

```bash
# Single process (publisher + push worker)
go run ./examples/nats

# Split roles (two terminals)
ROLE=publisher go run ./examples/nats
ROLE=worker go run ./examples/nats

# Scale push workers (queue group)
ROLE=worker go run ./examples/nats   # terminal 2
ROLE=worker go run ./examples/nats   # terminal 3

# Pull ingestion (separate durable — do not mix with push worker on same job queue in prod)
ROLE=publisher go run ./examples/nats
ROLE=puller go run ./examples/nats

# Throughput-oriented publish (raw bytes + Msg-Id)
ROLE=publisher CODEC=bytes go run ./examples/nats
```

## Environment

| Variable | Default | Meaning |
|----------|---------|---------|
| `NATS_URL` | `nats://127.0.0.1:4222` | NATS connection URL(s) |
| `ROLE` | `all` | `all` \| `publisher` \| `worker` \| `puller` |
| `CODEC` | `json` | Publisher: `json` or `raw`/`bytes` (`PublishBytesWithMsgID`) |
| `TEL_ENABLE` | (DefaultConfig) | `true` / `false` to force OTLP on/off |
| `ENVIRONMENT` | `dev` | Telemetry environment attribute |
| `SERVICE_VERSION` | `1.0.0` | Service version attribute |

## Tuning layers (map to consumer tuning guide)

| Layer | Push worker | Pull ingester |
|-------|-------------|---------------|
| Preset | `ProdWorkerConfig()` | `DefaultConfig()` + pull durable |
| AckWait | `3 × handlerP99` (45s) | `3 × (2 × handlerP99)` for batch path |
| Pool / batch | pool 8, buffer 256 | `WithFetchBatch(100)`, pool off |
| Backpressure | `BackpressureNak` | n/a (pull) |
| Observability | tracked stream/consumer metrics | tracked pull durable |
| Supervision | `SuperviseQueueSubscribeBound` + `WatchSoftLiveness` | metrics only |
| Scale | same queue name, more `ROLE=worker` | same durable, more `ROLE=puller` |

## Feature coverage (this example vs library)

| Feature | In this example? | Where else |
|---------|------------------|------------|
| DLQ / shadow / supervisor / soft liveness / flight recorder | Yes | Recipe G, devops |
| Publish Msg-Id | Yes | idempotency.md |
| `nats/idempotency` KV middleware | No | [Recipe C](../../docs/nats/recipes.md#recipe-c--high-consistency--financial-style-processing) |
| `nats/shard` | No | [Scaling](../../docs/nats/scaling.md) |
| Replay | No | Recipe E, api-reference |
| Proto / MsgPack | No (`CODEC=bytes` or JSON) | README codecs |
| Fan-out preset | No | Recipe B |
| `SupervisePullProcess` | No (puller is metrics-only) | api-reference / devops |
| TLS / AuthZ roles | No | `docker/nats/auth`, devops §4 |
| `Expected*` publish / ack helpers | No | api-reference |

## What you should see

- JSON logs: `published`, `processed order`, occasional `poison order routed to dlq`
- NATS metrics under prefix `nats/` (publish, ack, lag, `worker_queue_depth`, `consumer_stall`, `fetch_batch_size`, …) when OTLP is enabled
- Custom business counters: `orders.processed`, `orders.failed`, `orders.puller.processed`
- On SIGINT/SIGTERM: graceful NATS drain + telemetry flush; non-empty FlightRecorder dumped as JSON to stderr

## Production notes

- Do not run push worker and puller against the same WorkQueue job in production — they use separate durables and would split traffic. Use one delivery model per workload.
- Keep ProdWorker stream settings (replicas, file storage) outside this local demo.
- Size `handlerP99` from real metrics; AckWait must stay at 2–3× that value on both runtime and durable config.
- Soft Liveness covers “alive but stuck” for queue workers that cannot use IdleHeartbeat.
