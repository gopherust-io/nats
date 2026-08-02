#!/usr/bin/env bash
# Capture reproducible CPU/memory baselines for this module.
set -euo pipefail

root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$root"

OUT="${1:-}"
COUNT="${COUNT:-5}"
if [[ "${1:-}" == "--out" ]]; then
	OUT="${2:-bench/after.txt}"
fi
OUT="${OUT:-bench/baseline.txt}"

mkdir -p "$(dirname "$OUT")"

{
	echo "# nats baseline $(date -u +%Y-%m-%dT%H:%M:%SZ)"
	echo "# $(go version)"
	echo "# $(go env GOOS)/$(go env GOARCH) — $(sysctl -n machdep.cpu.brand_string 2>/dev/null || true)"
	echo "#"
	go test -bench='BenchmarkCodecComparison|BenchmarkPublishJSON|BenchmarkPublishBytes|BenchmarkPublishAsyncBytes|BenchmarkWorkerPool' \
		-benchmem -count="${COUNT}" -run '^$' \
		. ./workerpool/
	echo "#"
	echo "# competitive wrapper tax (legacy JetStreamContext)"
	go test -bench=BenchmarkCmp -benchmem -count=1 -benchtime=100x -run '^$' ./benchcmp/
} | tee "$OUT"

echo "Wrote $OUT"
