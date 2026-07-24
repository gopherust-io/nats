# Naming Conventions

Consistent naming prevents binding errors, simplifies operations, and keeps JetStream resources discoverable in monitoring tools.

How names fit the publish/consume model: [How JetStream works](README.md#how-jetstream-works).

## Streams

| Rule | Example | Anti-pattern |
|------|---------|--------------|
| UPPER_SNAKE or UPPER-KEBAB | `ORDERS`, `PAYMENT_EVENTS` | `orders`, `myStream` |
| One bounded domain per stream | `ORDERS`, `INVENTORY` | `EVENTS` (too generic) |
| Environment prefix only when sharing a cluster | `PROD_ORDERS`, `DEV_ORDERS` | `ORDERS` on every env when isolated clusters |

Stream names are JetStream identifiers. They appear in metrics, CLI (`nats stream info ORDERS`), and `BindStream("ORDERS")`.

**Examples from this library**

| Stream | Domain |
|--------|--------|
| `ORDERS` | Order lifecycle events |
| `PAYMENT_EVENTS` | Payment settlement |
| `DEV_ORDERS` | Local development |

## Subjects

Subjects are the publish routing key and must match a stream's `Subjects` list to be stored.

### Hierarchy

Use dot-separated tokens: `{domain}.{entity}.{action}`

```
orders.order.created
orders.order.cancelled
payments.transfer.settled
payments.transfer.failed
```

### Wildcards (stream capture only)

| Token | Meaning | Example |
|-------|---------|---------|
| `*` | Single token | `orders.*.created` matches `orders.v1.created` |
| `>` | One or more tokens | `orders.>` matches `orders.created` and `orders.v1.created` |

**Rules**

- Publish to **concrete** subjects (`orders.order.created`), not wildcards.
- Define stream capture with wildcards: `Subjects: []string{"orders.>"}`.
- Use a **domain-specific first token**. Avoid generic tokens like `after`, `test`, or `data` as the root — they can collide with internal or test traffic.

### Subject naming checklist

- [ ] First token identifies the business domain (`orders`, `payments`, `inventory`)
- [ ] Actions are past tense or event nouns (`created`, `settled`, `failed`)
- [ ] Version in the path if needed: `orders.v2.order.created`
- [ ] Stream `Subjects` covers every publish subject

## Durable consumers

| Rule | Example | Anti-pattern |
|------|---------|--------------|
| kebab-case | `orders-processor` | `OrdersProcessor`, `orders_processor` |
| `{domain}-{role}` pattern | `orders-audit`, `orders-dlq` | `consumer1` |
| One durable = one delivery intent | `orders-processor` per service role | Reusing across unrelated services |

Durable names are stable identities. Changing a durable's deliver policy requires an **explicit** delete-and-recreate (`ErrConsumerRecreateRequired` is returned otherwise). Use `Replay().ResetConsumer` when you intentionally seek and reset consumer state.

**Common roles**

| Suffix | Purpose |
|--------|---------|
| `-processor` | Primary business handler |
| `-audit` | Compliance / audit trail consumer |
| `-analytics` | Read-only analytics pipeline |
| `-dlq` | Dead-letter style reprocessing |
| `-puller` | Pull-based batch consumer |

## Queue groups (push scaling)

Queue groups load-balance push delivery across multiple application instances subscribed to the same durable.

| Rule | Example |
|------|---------|
| kebab-case service pool name | `orders-workers` |
| Same name on every replica of the service | All pods use `orders-workers` |
| Different from durable name | Durable: `orders-processor`, queue: `orders-workers` |

```go
// All replicas of orders-api use the same queue name.
client.Consumer().QueueSubscribe(ctx, "orders-workers", "orders.>", handler,
    nats.BindStream("ORDERS"), nats.Durable("orders-processor"))
```

## Connection client names

Set `cfg.Conn.ClientName` for NATS monitoring and connection dashboards:

```go
cfg.Conn.ClientName = "orders-api-" + podName
```

## Library validation

The client validates names before calling NATS (fail fast):

| Resource | Allowed | Rejected |
|----------|---------|----------|
| Stream / durable / queue | Non-empty; no whitespace, `.`, `*`, `>`, `/`, `\`; max 255 chars | `orders.events`, `bad name`, `a/b` |
| Subscribe / filter subjects | Dot tokens; `*` / `>` as standalone wildcards | `orders.`, `.orders`, `ord*ers`, `a.>.b` |
| Publish subjects | Literal only (no wildcards) | `orders.*`, `orders.>` |

Errors: `ErrInvalidStreamName`, `ErrInvalidDurableName`, `ErrInvalidQueueName`, `ErrInvalidSubject`.

```go
err := client.Streams().CreateOrUpdateStream(ctx, libnats.StreamConfig{
    Name: "orders.bad", // rejected: ErrInvalidStreamName
})
```

## Cross-reference table

| Resource | Case | Example | Used in |
|----------|------|---------|---------|
| Stream | UPPER_SNAKE | `ORDERS` | `BindStream`, `CreateOrUpdateStream`, `Pull` |
| Subject | dot.lower | `orders.order.created` | `Publish`, `Subscribe`, `FilterSubject` |
| Durable | kebab-case | `orders-processor` | `Durable()`, `CreateOrUpdateConsumer` |
| Queue group | kebab-case | `orders-workers` | `QueueSubscribe` |

See [Consumers & binding](consumers-and-binding.md) for how these names connect at runtime.
