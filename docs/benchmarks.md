# Benchmarks methodology

## Internal suites

| Suite | Command | Measures |
|-------|---------|----------|
| Codecs | `make bench-codec` | JSON / MsgPack / Proto encode-decode |
| Publish | `go test -bench=BenchmarkPublish -benchmem -run '^$' .` | JetStream publish paths |
| Competitive | `make bench-compete` | Wrapper tax vs legacy `nats.go` JetStreamContext |
| Load | `go run ./tools/loadtest -impl gopherust\|natsgo …` | Sustained msgs/s + alloc delta |

Refresh a checked-in-style capture:

```bash
./scripts/bench-baseline.sh
# or
./scripts/bench-baseline.sh --out bench/after.txt
benchstat bench/baseline.txt bench/after.txt
```

## Competitive vs nats.go (fairness)

This library is a **façade over legacy `nats.go` JetStream** (`JetStreamContext`). Competitive benches measure **wrapper overhead**, not broker speed.

Rules used by [`benchcmp/`](../benchcmp/):

1. Same embedded `nats-server` (JetStream, memory storage)
2. Same sync model (sync PubAck vs async+wait; never fire-and-forget vs sync)
3. Bytes payload (no JSON encode on the hot path)
4. Observability off (`ThroughputConfig`, metrics/tracing disabled)
5. Worker pool off for push consume (direct handler vs raw `QueueSubscribe`)
6. Report ns/op, B/op, allocs/op

The new `nats.go/jetstream` API is **not** mixed into the headline table; label any such comparison separately.

## Codec comparison

See [README § Codec comparison benchmarks](../README.md#codec-comparison-benchmarks). Prefer Protobuf or raw bytes on hot paths; JSON is for interoperability/debug.
