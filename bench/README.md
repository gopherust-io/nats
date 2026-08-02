# Benchmark baselines

Reproducible CPU/memory baselines for this module (and optional competitive wrapper-tax capture).

## Quick start

```bash
# Full matrix (writes bench/baseline.txt)
./scripts/bench-baseline.sh

# Custom output
./scripts/bench-baseline.sh --out bench/after.txt

# Focused suites
go test -bench=BenchmarkCodecComparison -benchmem -count=5 -run '^$' .
go test -bench=BenchmarkPublishBytes -benchmem -count=5 -run '^$' .
go test -bench=BenchmarkWorkerPool -benchmem -count=5 -run '^$' ./workerpool/
make bench-compete
```

## Comparing before/after

```bash
./scripts/bench-baseline.sh --out bench/after.txt
benchstat bench/baseline.txt bench/after.txt
```

Noise threshold of ~5% is expected on CI runners. Fail a change only if `ns/op` or `allocs/op` regress beyond that on critical benches.

## Critical benchmarks (CI gate)

| Benchmark | Package | Why |
|-----------|---------|-----|
| `BenchmarkCodecComparison` | `.` | Encode/decode allocs |
| `BenchmarkPublishJSON` | `.` | End-to-end publish |
| `BenchmarkPublishBytes` | `.` | Raw fast path |
| `BenchmarkWorkerPool` | `workerpool` | 0-alloc enqueue |

Competitive wrapper tax lives in [`../benchcmp/`](../benchcmp/) (`make bench-compete`) and is not a CI regression gate.

## Profiles

```bash
go test -bench=BenchmarkCodecComparison -cpuprofile=bench/cpu_codec.prof -memprofile=bench/mem_codec.prof -run '^$' .
go tool pprof -top bench/mem_codec.prof
```

Profiles are gitignored (`*.prof`); regenerate locally with the script.
