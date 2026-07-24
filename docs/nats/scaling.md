# Scaling JetStream

JetStream does **not** have Kafka-style partitions inside one stream. You scale **publish** with subject design / sharding, and **consume** with more workers on the same durable (queue group or pullers).

Mental model: [How JetStream works](README.md) · Delivery: [Push vs pull](push-vs-pull.md) · Tuning loop: [Consumer tuning guide](consumer-tuning-guide.md).

```mermaid
flowchart TB
  stream[One_stream]
  durable[One_durable]
  stream --> durable
  durable --> q1[Worker_A]
  durable --> q2[Worker_B]
  durable --> q3[Worker_C]
```

Same durable + same queue name (push) or multiple pull processes → each message is processed by **one** worker. More workers = more throughput, not fan-out.

## Comparison

| Approach | Use when | Ordering |
|----------|----------|----------|
| Queue group (push) | Competing workers on one durable | Stream-level |
| Multiple pullers | Batch ingestion | Stream-level |
| Subject sharding | High throughput + per-key order | Per shard |
| Fan-out (multiple durables) | Each service needs every event | Per consumer |

## Queue groups (worker scale)

```go
client.Consumer().QueueSubscribeBound(ctx, "ORDERS", "orders-processor",
    "orders-workers", "orders.>", handler)
```

- Add pods with the same queue name
- Use `WorkQueuePolicy` for job-queue semantics
- See [Optimal setups — job queue](optimal-setups.md#pattern-1-job-queue--competing-workers)

## Pull consumer scale

Multiple processes pull from the same durable; JetStream distributes batches.

```go
pull.Process(ctx, handler, libnats.WithFetchBatch(100))
```

Tune `MaxWaiting` on the durable for concurrent pull requests.

## Subject sharding (partition-like)

Route by business key to parallel pipelines with per-shard ordering:

```go
shard := libnats.ShardIndex(accountID, 8)
subject := libnats.ShardSubject("orders.shard", accountID, 8, "created")
// → orders.shard.3.created

client.Publisher().PublishJSON(ctx, subject, event)
```

### Stream layout options

**One stream, wildcard capture:**

```go
Subjects: []string{"orders.shard.>"}
```

**Multiple durables per shard** (filter per shard):

```go
FilterSubject: "orders.shard.3.>"
```

**Multiple streams** (`ORDERS_SHARD_0` … `ORDERS_SHARD_7`) for strongest isolation.

## Fan-out (not load sharing)

Separate durables on `LimitsPolicy` — each service gets every message. Do not use queue groups across services.

## Cluster capacity

- `Replicas: 3` for production durability
- `FileStorage` for persistence
- Tune `MaxBytes`, `MaxAge` for retention bounds
- Superclusters / leaf nodes for geo scale — production patterns: [Production operations — Server](devops.md#part-b--nats-server); local gateway lab: [Local Docker](local-docker.md#mini-supercluster-2--2)
- Basic mirror/source streams: set `StreamConfig.Mirror` / `Sources` on `CreateOrUpdateStream` (see [API / StreamConfig](api-reference.md)); leaf-node and multi-account export/import remain upstream NATS ops

## Decision guide

```
Need more workers on same job queue?
  → Queue group + WorkQueuePolicy

Need each service to see all events?
  → LimitsPolicy + separate durables

Need Kafka-like partitions?
  → ShardSubject by key + one consumer (or queue group) per shard

Need batch ETL throughput?
  → Pull + large fetch batches + multiple pullers
```
