# API Reference — Convenience Helpers

Quick lookup for SDK ergonomics beyond core `Client` methods.

**Flows (publish / consume / JetStream):** [NATS JetStream Guide](README.md) · **Performance knobs:** [Performance](../performance.md)

## Client

| Method | Description |
|--------|-------------|
| `Publisher()` / `Consumer()` | JetStream publish / consume |
| `Requester()` / `Responder()` | Core NATS request/reply |
| `SetupWorker(ctx, setup, handler)` | Create stream + durable + queue subscribe in one call |
| `KV()` | JetStream Key-Value bucket manager |
| `Objects()` | JetStream Object Store manager |
| `Monitoring()` | HTTP fetch of NATS monitoring endpoints (`/jsz`, `/varz`, …) |

## Connector

| Method | Description |
|--------|-------------|
| `IsConnected()` | Whether the underlying NATS connection is active |
| `WaitConnected(ctx)` | Block until connected, connection closed, or context done |
| `HealthCheck(ctx)` | Connected + JetStream `AccountInfo` when available |
| `ConnectionStatus()` | Server URL, reconnect count, last error/disconnect, lame-duck flag |
| `Shutdown()` | Graceful: stop consumers → drain/flush connection → cancel context (idempotent) |
| `Conn()` / `JetStream()` | Escape hatches to the underlying nats.go connection / JetStream context |
| `AccountInfo(ctx)` | JetStream account info |

Optional connection hooks on `Config.Conn`: `OnDisconnect`, `OnReconnect`, `OnClosed` (run after library handlers).

## Streams (`Client.Streams()`)

| Method | Description |
|--------|-------------|
| `CreateOrUpdateStream` | App-oriented create-or-update via `StreamConfig` |
| `AddStream` / `UpdateStream` | Explicit create/update with `*nats.StreamConfig` |
| `StreamNames` / `ListStreams` | Package helpers over `ListStreamsPage` |
| `StreamInfo` / `DeleteStream` / `PurgeStream` | Inspect / delete / purge |
| `GetMsg` / `GetLastMsg` / `GetNextMsgAfter` | Peek stored messages |

## Consumers (`Client.Consumers()`)

| Method | Description |
|--------|-------------|
| `CreateOrUpdateConsumer` | App-oriented durable create-or-update |
| `AddConsumer` / `UpdateConsumer` | Explicit create/update with `*nats.ConsumerConfig` |
| `ConsumerNames` / `ListConsumers` / `ListConsumersPage` | List / paginated list |
| `ConsumerInfo` / `DeleteConsumer` | Inspect / delete |
| `PauseConsumer` / `ResumeConsumer` | Pause until / resume (JetStream v2) |

## Key-Value (`Client.KV()`)

| Method | Description |
|--------|-------------|
| `CreateOrUpdate(ctx, cfg)` | Create or update bucket config (TTL, MaxBytes, …) via JetStream v2; returns legacy `nats.KeyValue` |
| `CreateRaw(ctx, cfg)` | Create via legacy API; returns `KVBucketStatus` |
| `Open(ctx, bucket)` | Bind to an existing bucket |
| `Delete(ctx, bucket)` | Delete the bucket |
| `ListBuckets` / `BucketInfo` | Bucket inventory / status |
| `Client.KVKeys()` | Key helpers: `ListKeys` / `Get` / `Put` / `DeleteKey` / `History` |

## Object Store (`Client.Objects()`)

| Method | Description |
|--------|-------------|
| `Create` / `CreateRaw` | Create bucket |
| `ListBuckets` / `BucketInfo` / `Delete` | Bucket inventory / status / delete |
| `ListObjects` / `Get` / `Put` / `DeleteObject` | Object CRUD (paged list) |

## Monitoring (`Client.Monitoring()`)

| Method | Description |
|--------|-------------|
| `Fetch(ctx, baseURL, path)` | GET `baseURL+path` (per-cluster monitoring root + `/jsz` etc.) |

## Replay

| Method | Description |
|--------|-------------|
| `ResetConsumer(ctx, stream, durable, opts...)` | Seek durable deliver position; preserves ack limits, filters, and other settings |
| `CreateReplayConsumer(ctx, stream, source, opts...)` | Side-car durable for backfill; live source durable is unchanged |
| `GetMsg(ctx, stream, seq)` | Peek one stored message by sequence |
| `GetLastMsgForSubject(ctx, stream, subject)` | Peek last message for a subject |
| `GetNextMsgAfter(ctx, stream, seq)` | Peek next existing message after sequence (skips gaps) |

### Replay options (`ReplayOpt`)

| Option | Description |
|--------|-------------|
| `FromSeq(seq)` | DeliverByStartSequence + OptStartSeq |
| `FromTime(t)` | DeliverByStartTime + OptStartTime |
| `FromBeginning()` | DeliverAll |
| `FromNew()` | DeliverNew |
| `WithReplayPolicy(p)` | Instant vs Original timing |
| `WithFilterSubject(s)` / `WithFilterSubjects(...)` | Override filter(s) |
| `WithReplayDurable(name)` | Side-car durable name for `CreateReplayConsumer` |
| `WithDeliverPolicy` / `WithStartSeq` / `WithStartTime` | Lower-level setters |

## Connection defaults (`DefaultConfig` / prod presets)

| Field | Production-oriented default |
|-------|-----------------------------|
| `MaxReconnect` | `-1` (unlimited) |
| `ReconnectWait` | `1s` (capped exponential via built-in delay) |
| `ReconnectJitter` / `ReconnectJitterTLS` | `100ms` / `1s` |
| `ReconnectBufSize` | `16 MiB` (`-1` disables; `0` = nats.go default) |
| `PingInterval` / `MaxPingsOut` | `20s` / `3` |
| `RetryOnFailedConnect` | `true` |
| `InitialRetryAttempts` | `5` |
| `AllowReconnect` | `true` (`DevConfig` sets `false` → `NoReconnect()`) |
| `RuntimeConsumer.IdleHeartbeat` | `5s` (non-queue push + pull Process default; `0` disables) |
| `RuntimeConsumer.FlowControl` | `true` (with IdleHeartbeat on non-queue push) |

Optional: `TLS` (`ConnectionTLS`), `DontRandomize`, `CustomReconnectDelay`, auth (`User`/`Password`, `Secret`, `Seed`, `CredentialsFile`).

## Config presets

| Function | Description |
|----------|-------------|
| `DefaultConfig()` | Resilient reconnect defaults (see table above) |
| `DevConfig()` | Local dev: no reconnect, memory storage, metrics off |
| `ProdWorkerConfig()` | Job queue: WorkQueue, worker pool, BackpressureNak |
| `ProdFanOutConfig()` | Event bus: LimitsPolicy, BackpressureBlock |
| `ThroughputConfig()` | Job queue + metrics/tracing off, SkipSubjectValidation, Lite metrics |

## Client

| Method | Description |
|--------|-------------|
| `WithShadow(cfg, primary, shadow)` | Dual-run canary; wires `shadow_*` metrics. Prefer over package-level `WithShadow` |

## Consumer

| Method | Description |
|--------|-------------|
| `SubscribeBound(ctx, stream, durable, subject, handler)` | Push subscribe with bind + durable; drains on ctx cancel; IdleHeartbeat when configured |
| `QueueSubscribeBound(ctx, stream, durable, queue, subject, handler)` | Queue group variant (no IdleHeartbeat — nats.go limitation) |
| `SuperviseSubscribeBound` / `SuperviseQueueSubscribeBound` | Same as bound subscribe + auto-resubscribe when `IsValid()` is false |
| `SupervisePullProcess` | Restarts `Pull.Process` with backoff after non-cancel errors |
| `WatchSoftLiveness` | Polls `ConsumerInfo` for rising pending without process activity (`consumer_stall`) |
| `NewFlightRecorder` / `AttachSupervisor` / `AttachSoftLiveness` | Ring-buffer incident timeline; auto-dump on supervisor give-up |
| `WithShadow(cfg, primary, shadow)` | Package-level dual-run (no auto metrics). Prefer `Client.WithShadow` |
| `WithDLQ(cfg, handler)` | Publish poison msgs to a DLQ subject + `Term`; optional `Autopsy` forensics. Prefer [`nats/dlq`](../../nats/dlq) |
| `HandlerTyped[T](mt, fn)` | Typed handler with automatic decode |
| `DecodeTyped[T](msg, mt)` | Decode message to type T |
| `InProgress` / `NakWithDelay` / `TermWithReason` | Manual ack helpers for long/poison handlers |
| `WithFetchHeartbeat(d)` / `WithProcessHeartbeat(d)` | Pull idle heartbeat (must be &lt; MaxWait) |

## Publisher

| Method | Description |
|--------|-------------|
| `PublishJSON(ctx, subject, data)` | Publish JSON-encoded payload |
| `PublishMsgPack(ctx, subject, data)` | Publish MessagePack-encoded payload |
| `PublishBytes(ctx, subject, data)` | Publish pre-encoded bytes (no codec) |
| `PublishBytesWithMsgID(ctx, subject, id, data)` | Publish raw bytes with dedup header |
| `PublishMessage(ctx, subject, msg)` | Publish a fully constructed `Message` (there is no `Publish`) |
| `PublishAsync(ctx, subject, msg)` | Async publish; returns `PubAckFuture` |
| `PublishAsyncBytes(ctx, subject, data)` | Async publish of raw bytes |
| `PublishWithMsgID(ctx, subject, id, msg)` | Publish with dedup header |
| `PublishProto(ctx, subject, proto)` | Publish protobuf |

`Message.WithExpectedStream` / `WithExpectedLastSeq` / `WithExpectedLastSeqPerSubject` / `WithExpectedLastMsgID` set optimistic concurrency PubOpts. `StreamConfig.Mirror` / `Sources` configure mirror/source streams. `SetupWorker` applies only `Consumer.Durable` from `DurableConsumerConfig` (push bind creates the durable).

`PublisherConfig.SkipSubjectValidation` skips per-publish subject tokenization for trusted static subjects. `PublisherConfig.MaxAsyncPending` caps in-flight async publishes (default 1024, `-1` unlimited). Call `PublishAsyncComplete(ctx)` after a burst to drain pending acks.

## Requester / Responder (core NATS request-reply)

Not JetStream. Uses `nats.Conn.RequestMsgWithContext` and core `Subscribe` / `QueueSubscribe` (no auto Ack/Nak).

| API | Description |
|-----|-------------|
| `Requester().RequestBytes` | Raw request, returns `*nats.Msg` |
| `Requester().RequestMessage` | Encode `Message` (any codec) and request |
| `Requester().RequestJSON` / `RequestJSONInto` | JSON request; `Into` decodes reply |
| `Requester().RequestMsgPack` / `RequestMsgPackInto` | MessagePack |
| `Requester().RequestProto` / `RequestProtoInto` | Protobuf (`proto.Message`) |
| `Responder().Subscribe` / `QueueSubscribe` | Core reply handlers |
| `RespondBytes` / `RespondJSON` / `RespondMsgPack` / `RespondProto` | Reply helpers |

`RequesterConfig.Timeout` applies when `ctx` has no deadline (default 2s). `AllowMetrics` / `AllowTracing` mirror publisher flags. Spans: `nats.request`, `nats.reply`.

Do not use request/reply on subjects captured by a JetStream stream: the PubAck is written to the reply inbox and will be mistaken for the application response.

Pull `Process` options: `WithProcessConcurrency(n)` runs a fixed worker set of size `n` per fetched batch (no per-message goroutine spawn).

`MetricsConfig.Lite` skips JetStream gauges and connection RTT. `MetricsConfig.FixedCardinality` collapses subject labels on hot-path counters.

## Message

| Method | Description |
|--------|-------------|
| `Message.WithMsgID(id)` | Set `Nats-Msg-Id` header for publish dedup |
| `Message.WithExpected*` | Optimistic concurrency (stream / last seq / last msg id) |

## Constants

| Name | Value |
|------|-------|
| `HeaderMsgID` | `Nats-Msg-Id` |
| `HeaderContentType` | `Nats-Content-Type` |

## Sharding (`nats/shard`)

Prefer `github.com/gopherust-io/nats/shard`. Root `nats` re-exports the same helpers.

| Function | Description |
|----------|-------------|
| `shard.Index` / `ShardIndex` | Stable shard index for key |
| `shard.Subject` / `ShardSubject` | Build sharded subject |

## Middleware packages

| Package | Entry point | Notes |
|---------|-------------|-------|
| [`nats/dlq`](../../nats/dlq) | `dlq.With` | Root re-exports `WithDLQ`, `ErrSendToDLQ`, `ErrDLQRouted`, header consts |
| [`nats/shadow`](../../nats/shadow) | `shadow.With` | Root re-exports `WithShadow` / `ShadowConfig` |
| [`nats/idempotency`](../../nats/idempotency) | `idempotency.WithHandler` | No root re-export (import the package) |

## Idempotency (`nats/idempotency`)

| Symbol | Description |
|--------|-------------|
| `DedupStore` | Interface: `Seen`, `Mark` |
| `ClaimStore` | Optional: `Claim` / `Release` — used by `NewKVStore` for claim-before-process |
| `NewKVStore(kv)` | JetStream KV-backed `DedupStore`/`ClaimStore` (use a TTL'd bucket) |
| `NewBloomStore(bits, hashes)` | In-memory probabilistic Seen; **not** ClaimStore — use with `WithBackend` or prefer KV for queues |
| `WithHandler(store, extractID, handler)` | Wrap handler with consume-side dedup (ClaimStore path is race-safe for queues) |
| `MsgIDFromHeader(msg)` | Read `Nats-Msg-Id` from message |

## Tracing

| Setting | Description |
|---------|-------------|
| `MetricsConfig.AllowTracing` | Master switch for NATS client tracing |
| `PublisherConfig.AllowTracing` | Spans on publish + inject trace context into headers |
| `RuntimeConsumerConfig.AllowTracing` | Spans on consume + extract parent context from headers |

Spans: `nats.publish`, `nats.process` with `messaging.destination`, `messaging.system=nats`, and JetStream metadata attributes.

Requires `tel.WrapContext` and OTLP collector on `TelConfig.Address` (same as metrics).

## SetupWorker example

```go
sub, err := client.SetupWorker(ctx, libnats.WorkerSetup{
    Stream: libnats.StreamConfig{
        Name: "ORDERS", Subjects: []string{"orders.>"},
        Retention: libnats.WorkQueuePolicy, Replicas: 3, Storage: libnats.FileStorage,
    },
    Consumer: libnats.DurableConsumerConfig{
        Durable: "orders-processor", FilterSubject: "orders.>",
        MaxAckPending: 1000, AckWait: 45 * time.Second,
    },
    Queue:   "orders-workers",
    Subject: "orders.>",
}, handler)
```

## QueueSubscribeBound example

```go
_, err := client.Consumer().QueueSubscribeBound(ctx,
    "ORDERS", "orders-processor", "orders-workers", "orders.>", handler)
```

## Subscription supervisor

```go
sub, err := client.SuperviseSubscribeBound(ctx, "ORDERS", "orders-processor", "orders.>", handler,
    libnats.SupervisorConfig{MaxRetries: 10, InitialBackoff: time.Second})
// sub.Events() ← SupervisorResubscribed / SupervisorGiveUp
// Metrics: resubscribe_total, supervisor_give_up
```

Use `SuperviseQueueSubscribeBound` for queue workers. Pull: `client.SupervisePullProcess(...)`.

## Soft liveness + flight recorder

```go
rec := libnats.NewFlightRecorder(128)
supCfg := libnats.SupervisorConfig{MaxRetries: 10}
rec.AttachSupervisor(&supCfg)

liveCfg := libnats.SoftLivenessConfig{StallAfter: 15 * time.Second, RisingWindows: 3}
rec.AttachSoftLiveness(&liveCfg)
live, _ := client.WatchSoftLiveness(ctx, sub, liveCfg)
// Metrics: consumer_stall — snapshot: rec.WriteJSON(os.Stdout)
```

## Dead-letter helper

```go
handler := libnats.WithDLQ(libnats.DLQConfig{
    Publisher: client.Publisher(), Subject: "orders.dlq.poison", MaxDeliver: 5,
    Recorder: rec,
    Autopsy:  libnats.AutopsyConfig{Enabled: true}, // X-NATS-Autopsy-Error/Hash
}, myHandler)
// or return libnats.ErrSendToDLQ from the handler
```

## Shadow consumer

```go
handler := client.WithShadow(libnats.ShadowConfig{
    SampleRate: 0.1, Recorder: rec,
}, primaryHandler, candidateHandler)
// Metrics: shadow_error_total, shadow_mismatch_total
// Shadow message has no Reply — cannot Ack/Nak/Term the real delivery
```
