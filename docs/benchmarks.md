# Benchmarks methodology

## Internal suites

| Suite | Command | Measures |
|-------|---------|----------|
| Codecs | `make bench-codec` | JSON / MsgPack / Proto encode-decode |
| Publish | `go test -bench=BenchmarkPublish -benchmem -run '^$' .` | JetStream publish paths |
| Payload compress | `make bench-payload` | Pure + **pub/sub** + **request/reply** JSON/MsgPack/Proto × off/gzip/br |
| Adaptive pressure | `go test -bench=BenchmarkDecideAdaptivePressure -benchmem -run '^$' .` | Pure control-loop decision |
| Competitive | `make bench-compete` | Wrapper tax vs legacy `nats.go` JetStreamContext |
| Load | `go run ./tools/loadtest -impl gopherust\|natsgo …` | Sustained msgs/s + alloc delta |

Refresh a checked-in-style capture:

```bash
./scripts/bench-baseline.sh
# or
./scripts/bench-baseline.sh --out bench/after.txt
benchstat bench/baseline.txt bench/after.txt
```

`bench-baseline.sh` includes codec/publish/workerpool plus `BenchmarkPayload*` / `BenchmarkPubSubPayload` / `BenchmarkRequestReplyPayload` / `BenchmarkDecideAdaptivePressure` (competitive wrapper tax is `make bench-compete`).

## Payload compression fairness

- Fixture: compressible JSON-like bytes (same idea as nats-consol `largeJSON`)
- Threshold: strictly **greater than 32 KiB** before attempting compress
- Modes: `PayloadCompressionOff` / `Gzip` / `Brotli` / `Auto` (br then gzip)
- Levels: best-speed; shrink-only
- Pure helpers: ns/op, B/op, allocs/op, `bytes_out`, `ratio`
- Pub/sub (`BenchmarkPubSubPayload`): encode → publish → deliver → decompress → decode for each codec × off/gzip/br
- Request/reply (`BenchmarkRequestReplyPayload`): encode → request compress → reply compress → decode (both directions)
- Numbers in [performance.md](performance.md) are directional (darwin/arm64); re-run on your machine

## Competitive vs nats.go (fairness)

This library is a **façade over legacy `nats.go` JetStream** (`JetStreamContext`). Competitive benches measure **wrapper overhead**, not broker speed.

Rules used by [`benchcmp/`](../benchcmp/):

1. Same embedded `nats-server` (JetStream, memory storage)
2. Same sync model (sync PubAck vs async+wait; never fire-and-forget vs sync)
3. Bytes payload (no JSON encode on the hot path)
4. Observability off (max-QPS recipe: metrics/tracing disabled)
5. Worker pool off for push consume (direct handler vs raw `QueueSubscribe`)
6. Report ns/op, B/op, allocs/op

The new `nats.go/jetstream` API is **not** mixed into the headline table; label any such comparison separately.

## Codec comparison

See [README § Codec comparison benchmarks](../README.md#codec-comparison-benchmarks). Prefer Protobuf or raw bytes on hot paths; JSON is for interoperability/debug.
