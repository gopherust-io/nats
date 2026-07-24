# Benchmark baselines

Reproducible CPU/memory baselines for `nats` and `tel` packages.

## Quick start

```bash
# Full matrix (writes bench/baseline.txt + optional profiles)
./scripts/bench-baseline.sh

# Focused suites
go test -bench=BenchmarkCodecComparison -benchmem -count=5 -run '^$' ./nats/
go test -bench=BenchmarkPublishJSON -benchmem -count=5 -run '^$' ./nats/
go test -bench=BenchmarkWorkerPool -benchmem -count=5 -run '^$' ./nats/workerpool/
go test -bench=. -benchmem -count=5 -run '^$' ./telemetry/
```

## Comparing before/after

```bash
# Capture a new run
./scripts/bench-baseline.sh --out bench/after.txt

# Diff with benchstat (install: go install golang.org/x/perf/cmd/benchstat@latest)
benchstat bench/baseline.txt bench/after.txt
```

Noise threshold of ~5% is expected on CI runners. Fail a change only if `ns/op` or `allocs/op` regress beyond that on critical benches.

## Critical benchmarks (CI gate)

| Benchmark | Package | Why |
|-----------|---------|-----|
| `BenchmarkCodecComparison` | `nats` | Encode/decode allocs |
| `BenchmarkPublishJSON` | `nats` | End-to-end publish |
| `BenchmarkPublishBytes` | `nats` | Raw fast path |
| `BenchmarkWorkerPool` | `nats/workerpool` | 0-alloc enqueue |
| `BenchmarkAttrCache_SubjectMiss` / warm paths | `tel` | Label cache |

## KPI targets

| KPI | Baseline (see baseline.txt) | Phase 2 | Phase 4 |
|-----|----------------------------|---------|---------|
| JSON round-trip allocs | ~7 | — | — |
| Proto round-trip allocs | ~4 | Prefer on hot paths | — |
| `recordMessageMetrics` warmed | 0 allocs | Maintain | Maintain |
| Worker pool publish | 0 allocs/op | Maintain | Maintain |
| AttrCache cold miss | CoW insert + heap | Lock-free shards | <100 ns |

## Profiles

```bash
go test -bench=BenchmarkCodecComparison -cpuprofile=bench/cpu_codec.prof -memprofile=bench/mem_codec.prof -run '^$' ./nats/
go test -bench=BenchmarkPublishJSON -cpuprofile=bench/cpu_pub.prof -memprofile=bench/mem_pub.prof -run '^$' ./nats/
go tool pprof -top bench/mem_pub.prof
go tool pprof -top bench/cpu_codec.prof
```

Profiles are gitignored (`*.prof`); regenerate locally with the script.
