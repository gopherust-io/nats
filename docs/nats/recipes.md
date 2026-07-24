# Configuration Recipes

Copy-paste setups for common JetStream patterns. Each recipe lists goal, stream config, consumer config, runtime config, codec recommendation, and operational notes.

**How publish / consume / retention fit together:** [How JetStream works](README.md). **Which delivery model:** [Push vs pull](push-vs-pull.md).

Prerequisites for all recipes:

```go
import (
    libnats "github.com/gopherust-io/nats"
    "github.com/gopherust-io/tel"
    natspkg "github.com/nats-io/nats.go"
)

telem := tel.NewWithConfig(tel.DefaultDebugConfig())
_ = telem.Start(ctx)
ctx = tel.WrapContext(ctx, telem)

client, err := libnats.NewClient(ctx, &cfg)
defer client.Connector().Shutdown()
```

**Streams:** `NewClient` does not create or update streams. Prefer `nats stream add` / platform ops. Labs and apps that bootstrap in-process should call `client.Streams().CreateOrUpdateStream` after connect (setting `cfg.Stream` alone has no effect).

---

## Recipe A — Worker / job processor

**Goal:** Competing workers process each message exactly once with high throughput.

**Stream:** `WorkQueuePolicy`, replicated, file-backed.

**Consumer:** Push + queue group + worker pool.

```go
cfg := libnats.DefaultConfig()
cfg.Conn.Address = os.Getenv("NATS_URL")
cfg.Conn.ClientName = "orders-worker"

cfg.RuntimeConsumer.WorkerPoolEnabled = true
cfg.RuntimeConsumer.WorkerPoolSize = 8
cfg.RuntimeConsumer.WorkerBufferSize = 256
cfg.RuntimeConsumer.AckWait = 45 * time.Second

cfg.Backpressure = libnats.BackpressureConfig{
    Mode:          libnats.BackpressureNak,
    MaxAckPending: 1000,
}

client, _ := libnats.NewClient(ctx, &cfg)

_, _ = client.Streams().CreateOrUpdateStream(ctx, libnats.StreamConfig{
    Name:      "ORDERS",
    Subjects:  []string{"orders.>"},
    Replicas:  3,
    Storage:   libnats.FileStorage,
    Retention: libnats.WorkQueuePolicy,
    MaxAge:    72 * time.Hour,
    Discard:   libnats.DiscardOld,
})

client.Consumers().CreateOrUpdateConsumer(ctx, "ORDERS", libnats.DurableConsumerConfig{
    Durable:       "orders-processor",
    FilterSubject: "orders.>",
    MaxAckPending: 1000,
    AckWait:       45 * time.Second,
    MaxDeliver:    5,
})

client.Consumer().QueueSubscribe(ctx, "orders-workers", "orders.>", handler,
    natspkg.BindStream("ORDERS"), natspkg.Durable("orders-processor"))
```

| Setting | Value | Why |
|---------|-------|-----|
| Codec | **Protobuf** | ~4× faster round-trip vs JSON (see README benchmarks) |
| Retention | `WorkQueuePolicy` | One delivery per message across workers |
| Backpressure | `nak` | Redeliver when pool saturated instead of blocking |

**Ops notes**

- Scale by adding pods with the same queue group `orders-workers`.
- Monitor `worker_queue_depth` and `redelivery_total` metrics.
- Set `AckWait` > p99 handler duration.

---

## Recipe B — Fan-out / event bus

**Goal:** Multiple independent services each receive every message.

**Stream:** `LimitsPolicy` (default).

**Consumer:** Separate durables per downstream service; push without queue group (or each service has its own queue group).

```go
cfg.Stream = libnats.StreamConfig{
    Name:      "ORDERS",
    Subjects:  []string{"orders.>"},
    Replicas:  3,
    Storage:   libnats.FileStorage,
    Retention: libnats.LimitsPolicy,
    MaxAge:    30 * 24 * time.Hour,
}

// Service 1: order processing
client.Consumers().CreateOrUpdateConsumer(ctx, "ORDERS", libnats.DurableConsumerConfig{
    Durable:       "orders-processor",
    FilterSubject: "orders.>",
})

// Service 2: analytics
client.Consumers().CreateOrUpdateConsumer(ctx, "ORDERS", libnats.DurableConsumerConfig{
    Durable:       "orders-analytics",
    FilterSubject: "orders.>",
})

// Service 3: audit (subset)
client.Consumers().CreateOrUpdateConsumer(ctx, "ORDERS", libnats.DurableConsumerConfig{
    Durable:       "orders-audit",
    FilterSubject: "orders.order.created",
})
```

Each service subscribes to its own durable:

```go
client.Consumer().Subscribe(ctx, "orders.>", processorHandler,
    natspkg.BindStream("ORDERS"), natspkg.Durable("orders-processor"))

client.Consumer().Subscribe(ctx, "orders.>", analyticsHandler,
    natspkg.BindStream("ORDERS"), natspkg.Durable("orders-analytics"))
```

| Setting | Value | Why |
|---------|-------|-----|
| Codec | JSON or Protobuf | JSON for heterogeneous consumers; Proto if all are Go/gRPC |
| Retention | `LimitsPolicy` | Independent cursor per durable |

---

## Recipe C — High-consistency / financial-style processing

**Goal:** Minimize duplicate processing and data loss. JetStream does **not** provide multi-message ACID transactions — use **at-least-once delivery + idempotent handlers**.

```go
cfg.Stream = libnats.StreamConfig{
    Name:            "PAYMENTS",
    Subjects:        []string{"payments.>"},
    Replicas:        3,
    Storage:         libnats.FileStorage,
    Retention:       libnats.LimitsPolicy,
    DuplicateWindow: 2 * time.Minute,
    MaxAge:          90 * 24 * time.Hour,
    Discard:         libnats.DiscardOld,
}

cfg.RuntimeConsumer.WorkerPoolEnabled = true
cfg.RuntimeConsumer.WorkerPoolSize = 4
cfg.RuntimeConsumer.AckWait = 60 * time.Second

cfg.Backpressure = libnats.BackpressureConfig{
    Mode:          libnats.BackpressureBlock, // never silently drop
    MaxAckPending: 500,
}

client.Consumers().CreateOrUpdateConsumer(ctx, "PAYMENTS", libnats.DurableConsumerConfig{
    Durable:       "payments-processor",
    FilterSubject: "payments.>",
    AckPolicy:     libnats.AckExplicit,
    MaxAckPending: 500,
    AckWait:       60 * time.Second,
    MaxDeliver:    3,
})
```

**Idempotent publish** with deduplication header:

```go
messageID := "pay-" + transferID // business-level unique ID

client.Publisher().PublishWithMsgID(ctx, "payments.transfer.settled", messageID, libnats.Message{
    Data:        payload,
    MessageType: libnats.Proto,
})
```

**Idempotent handler** — prefer the library middleware (KV claim-before-process):

```go
import "github.com/gopherust-io/nats/idempotency"

kv, _ := client.KV().CreateOrUpdate(ctx, libnats.KeyValueConfig{
    Bucket: "PAYMENTS_DEDUP", TTL: 24 * time.Hour, History: 1, Storage: libnats.FileStorage,
})
store := idempotency.NewKVStore(kv)

handler := idempotency.WithHandler(store, idempotency.MsgIDFromHeader, func(ctx context.Context, msg *natspkg.Msg) error {
    return processTransfer(msg)
})
```

Optimistic publish (optional):

```go
client.Publisher().PublishMessage(ctx, "payments.transfer.settled",
    libnats.Message{Data: payload, MessageType: libnats.Proto}.
        WithMsgID(messageID).
        WithExpectedStream("PAYMENTS"),
)
```

| Setting | Value | Why |
|---------|-------|-----|
| Codec | **Protobuf** | Smallest payload, schema enforcement |
| `DuplicateWindow` | 2 min | Server-side publish dedup by `Nats-Msg-Id` |
| `BackpressureBlock` | — | No silent drops |
| `MaxDeliver` | 3 | Limit poison-message retries |
| Handler | `idempotency.WithHandler` + KV | At-least-once + claim-before-process |

**Optional:** Use `WorkQueuePolicy` if exactly one active worker must process each payment.

See also: [Idempotency](idempotency.md) · `InProgress` / `NakWithDelay` / `TermWithReason` in the root `nats` package for long handlers.

---

## Recipe D — High-throughput ingestion (pull + batch)

**Goal:** Ingest large volumes with explicit batching and back-pressure.

```go
cfg.Stream = libnats.StreamConfig{
    Name:      "EVENTS",
    Subjects:  []string{"events.>"},
    Replicas:  3,
    Storage:   libnats.FileStorage,
    Retention: libnats.LimitsPolicy,
    MaxBytes:  50 << 30, // 50 GiB
    Discard:   libnats.DiscardOld,
}

cfg.RuntimeConsumer.WorkerPoolEnabled = false

client.Consumers().CreateOrUpdateConsumer(ctx, "EVENTS", libnats.DurableConsumerConfig{
    Durable:       "events-ingester",
    FilterSubject: "events.>",
    MaxAckPending: 1000,
    AckWait:       120 * time.Second,
    MaxWaiting:    512,
    RateLimit:     0, // set bits/sec if upstream needs throttling
})

pull, _ := client.Consumer().Pull("EVENTS", "events-ingester")
pull.Process(ctx, handler,
    libnats.WithFetchBatch(100),
    libnats.WithProcessMaxWait(3*time.Second),
)
```

| Setting | Value | Why |
|---------|-------|-----|
| Codec | **Protobuf** | Highest encode/decode throughput |
| Delivery | Pull | Client controls batch size and pace |
| `WithFetchBatch` | 50–200 | Tune to handler throughput |

Scale by running multiple pull processes on the same durable (JetStream distributes batches).

---

## Recipe E — Audit / replay

**Goal:** Long-retention event log with ability to reprocess from a point in time.

```go
cfg.Stream = libnats.StreamConfig{
    Name:      "ORDERS",
    Subjects:  []string{"orders.>"},
    Replicas:  3,
    Storage:   libnats.FileStorage,
    Retention: libnats.LimitsPolicy,
    MaxAge:    365 * 24 * time.Hour,
}

// Primary processor (DeliverNew)
client.Consumers().CreateOrUpdateConsumer(ctx, "ORDERS", libnats.DurableConsumerConfig{
    Durable:       "orders-processor",
    FilterSubject: "orders.>",
    DeliverPolicy: libnats.DeliverNew,
})

// Audit consumer (can be reset for replay)
client.Consumers().CreateOrUpdateConsumer(ctx, "ORDERS", libnats.DurableConsumerConfig{
    Durable:       "orders-audit",
    FilterSubject: "orders.>",
    DeliverPolicy: libnats.DeliverAll,
})
```

**Replay from sequence (preserves consumer limits/filters):**

```go
client.Replay().ResetConsumer(ctx, "ORDERS", "orders-audit",
    libnats.FromSeq(10000), libnats.WithReplayPolicy(libnats.ReplayInstant))
```

**Side-car replay (live durable untouched):**

```go
temp, err := client.Replay().CreateReplayConsumer(ctx, "ORDERS", "orders-processor",
    libnats.FromSeq(10000))
// subscribe / pull on `temp`, then DeleteConsumer when done
```

**Peek without moving cursors:**

```go
msg, err := client.Replay().GetMsg(ctx, "ORDERS", 10000)
last, err := client.Replay().GetLastMsgForSubject(ctx, "ORDERS", "orders.created")
next, err := client.Replay().GetNextMsgAfter(ctx, "ORDERS", 10000)
```

---

## Recipe F — Development / local

**Goal:** Fast iteration, minimal resource usage, no metrics overhead.

```go
cfg := libnats.DefaultConfig()
cfg.Conn.Address = "nats://127.0.0.1:4222"
cfg.Conn.AllowReconnect = false

cfg.Metrics.AllowMetrics = false
cfg.Conn.AllowMetrics = false
cfg.PublisherConfig.AllowMetrics = false
cfg.RuntimeConsumer.AllowMetrics = false

cfg.Stream = libnats.StreamConfig{
    Name:     "DEV_ORDERS",
    Subjects: []string{"orders.>"},
    Replicas: 1,
    Storage:  libnats.MemoryStorage,
}

cfg.RuntimeConsumer.WorkerPoolEnabled = true
cfg.RuntimeConsumer.WorkerPoolSize = 2
cfg.RuntimeConsumer.WorkerBufferSize = 32

telem := tel.NewWithConfig(tel.DefaultDebugConfig())
_ = telem.Start(ctx)
ctx = tel.WrapContext(ctx, telem)
```

Run locally:

```bash
nats-server -js
go run ./examples/nats
```

---

## Recipe G — Dead letter (DLQ) + subscription supervisor

**Goal:** Poison messages land on a DLQ stream with forensic autopsy headers; push interest auto-heals; queue workers get soft-liveness + a dumpable incident timeline; new handlers can dual-run via shadow.

**Streams:** work stream + DLQ stream (separate subjects).

```go
rec := libnats.NewFlightRecorder(128)

_, _ = client.Streams().CreateOrUpdateStream(ctx, libnats.StreamConfig{
    Name: "ORDERS", Subjects: []string{"orders.>"},
    Retention: libnats.WorkQueuePolicy, Storage: libnats.FileStorage, Replicas: 3,
})
_, _ = client.Streams().CreateOrUpdateStream(ctx, libnats.StreamConfig{
    Name: "ORDERS_DLQ", Subjects: []string{"orders.dlq.>"},
    Retention: libnats.LimitsPolicy, Storage: libnats.FileStorage, MaxAge: 30 * 24 * time.Hour,
})

primary := func(ctx context.Context, msg *natspkg.Msg) error {
    return processOrder(ctx, msg)
}
shadow := func(ctx context.Context, msg *natspkg.Msg) error {
    return processOrderV2(ctx, msg) // candidate — never Acks
}

handler := libnats.WithDLQ(libnats.DLQConfig{
    Publisher:  client.Publisher(),
    Subject:    "orders.dlq.poison",
    MaxDeliver: 5,
    Recorder:   rec,
    Autopsy:    libnats.AutopsyConfig{Enabled: true},
}, client.WithShadow(libnats.ShadowConfig{
    SampleRate: 0.1, // canary 10%
    Recorder:   rec,
}, primary, shadow))

supCfg := libnats.SupervisorConfig{
    MaxRetries:     10,
    InitialBackoff: time.Second,
}
rec.AttachSupervisor(&supCfg)

sub, err := client.SuperviseQueueSubscribeBound(ctx,
    "ORDERS", "orders-processor", "orders-workers", "orders.>",
    handler, supCfg)

liveCfg := libnats.SoftLivenessConfig{
    PollInterval: 2 * time.Second, StallAfter: 15 * time.Second, RisingWindows: 3,
}
rec.AttachSoftLiveness(&liveCfg)
live, _ := client.WatchSoftLiveness(ctx, sub, liveCfg)
defer live.Stop()
```

| Setting | Value | Why |
|---------|-------|-----|
| Autopsy headers | `X-NATS-Autopsy-Error/Hash/Stack` | Forensics without a separate debug service |
| Shadow | `shadow_error_total` / `shadow_mismatch_total` | Safe handler deploys; primary owns Ack/DLQ |
| Soft liveness | `consumer_stall` | Queue path cannot use IdleHeartbeat |
| Flight recorder | `Snapshot` / `WriteJSON` | Timeline of supervisor / stall / DLQ / shadow |

**Ops notes**

- Pair with stream `DuplicateWindow` + original `Nats-Msg-Id` so DLQ publish is idempotent on retries.
- Queue workers: Supervisor polls `IsValid()`; SoftLiveness polls rising `NumPending` without process activity — see [Queue soft-liveness](push-vs-pull.md#queue-soft-liveness).
- Shadow clones the message without `Reply`, so shadow Ack/Nak/Term cannot affect JetStream delivery.

---

## Recipe quick-reference

| Recipe | Retention | Delivery | Worker pool | Codec |
|--------|-----------|----------|-------------|-------|
| A — Worker | WorkQueue | Push + queue | Yes | Proto |
| B — Fan-out | Limits | Push | Optional | JSON/Proto |
| C — High consistency | Limits | Push | Yes | Proto |
| D — Ingestion | Limits | Pull | No | Proto |
| E — Audit/replay | Limits | Push/Pull | Optional | JSON |
| F — Development | Limits | Push | Yes | JSON |
| G — DLQ + supervisor | WorkQueue + Limits | Push + queue | Optional | Raw DLQ |

Next: [API reference](api-reference.md)

See also: [Production operations](devops.md) | [Push vs pull](push-vs-pull.md) | [Naming conventions](naming-conventions.md)
