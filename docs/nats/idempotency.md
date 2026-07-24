# Idempotency

JetStream gives you **at-least-once** delivery: after a crash or Nak, the same message can run your handler again. Dedup has two layers:

| Layer | Who | When |
|-------|-----|------|
| **Publish** | JetStream `DuplicateWindow` + `Nats-Msg-Id` | Same ID within the window → not stored twice |
| **Consume** | Your app (KV / Bloom / DB) | Handler already processed this business key → Ack and skip |

Flow overview: [How JetStream works](README.md#end-to-end-publish-flow).

## Publish deduplication

Set `DuplicateWindow` on the stream and publish with a stable message ID:

```go
cfg.Stream.DuplicateWindow = 2 * time.Minute

client.Publisher().PublishWithMsgID(ctx, "payments.transfer.settled", "pay-"+transferID,
    libnats.Message{Data: payload, MessageType: libnats.Proto},
)

// Or fluently:
client.Publisher().PublishMessage(ctx, subject, libnats.Message{
    Data: payload, MessageType: libnats.Proto,
}.WithMsgID("pay-"+transferID))
```

The library sets `Nats-Msg-Id` via `HeaderMsgID` constant.

## Consume-side deduplication

Handlers must tolerate redelivery. Prefer a **JetStream KV** bucket with TTL so processed IDs survive restarts and cannot grow without bound in process memory:

```go
import (
    libnats "github.com/gopherust-io/nats"
    "github.com/gopherust-io/nats/idempotency"
)

kv, err := client.KV().CreateOrUpdate(ctx, libnats.KeyValueConfig{
    Bucket:  "PAYMENTS_DEDUP",
    TTL:     24 * time.Hour, // keys expire; set MaxBytes as a hard cap if needed
    History: 1,
    Storage: libnats.FileStorage,
})
store := idempotency.NewKVStore(kv)

handler := idempotency.WithHandler(store,
    idempotency.MsgIDFromHeader, // or extract from payload
    func(ctx context.Context, msg *nats.Msg) error {
        return processTransfer(msg)
    },
)

client.Consumer().QueueSubscribeBound(ctx, "PAYMENTS", "payments-processor",
    "payments-workers", "payments.>", handler)
```

`NewKVStore` implements `ClaimStore`. `WithHandler` uses **claim-before-process** (`Create` key, release/`Purge` on handler error) so queue workers do not double-process the same ID.

Message IDs must be valid NATS KV keys (`[-/_=.a-zA-Z0-9]+`). Invalid IDs return `ErrInvalidKVKey`.

You can still implement `DedupStore` with Redis, PostgreSQL, or an in-memory map for tests — avoid unbounded in-memory maps in production (not durable; risk of OOM). Plain `DedupStore` (without `Claim`) still uses Seen→handler→Mark and can race under concurrency.

### DedupStore / ClaimStore

```go
type DedupStore interface {
    Seen(ctx context.Context, id string) (bool, error)
    Mark(ctx context.Context, id string) error
}

type ClaimStore interface {
    DedupStore
    Claim(ctx context.Context, id string) (acquired bool, err error)
    Release(ctx context.Context, id string) error
}
```

**TTL guidance:** set a positive `TTL` on the bucket (and optionally `MaxBytes`). `TTL: 0` with no size limit can still grow until the JetStream account is exhausted. `CreateOrUpdate` updates bucket config (including TTL) when the bucket already exists.

### BloomStore (in-memory only)

`idempotency.NewBloomStore` is a **probabilistic Seen filter** for hot paths. It implements `DedupStore` (Seen/Mark) but **not** `ClaimStore` — do **not** use it alone under competing queue workers (race: two workers can both miss Seen before Mark). Prefer:

- `NewKVStore` + `WithHandler` for production queues, or
- `NewBloomStore(...).WithBackend(kvStore)` as a front cache in front of KV

False positives skip work (safe if handlers are idempotent). Bloom alone is not durable across process restart.

## What JetStream does not provide

- Multi-message ACID transactions
- Exactly-once end-to-end processing
- Consumer-side dedup without application logic

## Recommended stack

| Layer | Mechanism |
|-------|-----------|
| Publish retry | `DuplicateWindow` + `PublishWithMsgID` |
| Delivery | `AckExplicit`, `MaxDeliver: 3`, `AckWait` > p99 handler |
| Processing | `idempotency.WithHandler` + `NewKVStore` (TTL'd bucket, claim-before-process) |
| Backpressure | `BackpressureBlock` (no silent drops) |

See [Recipe C — high consistency](recipes.md#recipe-c--high-consistency--financial-style-processing).
