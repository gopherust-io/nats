export GOWORK := off

PKGS := $(shell go list ./... | grep -vE '/(examples|tools)/')
FUZZ_PKGS := .
FUZZ_TESTS := FuzzDecodeJSON FuzzCommonWildcardSubject FuzzShardIndex
NPROCS := $(shell getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
GO_TEST_FLAGS := -count=1 -parallel=$(NPROCS) -timeout=60s
COVERAGE_MIN ?= 75

.PHONY: help test test-race coverage coverage-html bench bench-codec bench-payload bench-compete fuzz ci vet fmt fmt-check lint lint-fix govulncheck align align-fix examples \
	loadtest demo demo-nats dev

GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2
GOVULNCHECK := go run golang.org/x/vuln/cmd/govulncheck@v1.6.0
BETTERALIGN := go run github.com/dkorunic/betteralign/cmd/betteralign@v0.7.2
GOALIGN_VERSION := v1.4.0
GOALIGN_BIN := $(HOME)/go/bin/goalign
GOALIGN_FLAGS := analyze -r --arch=amd64 --fail-on-findings --min-waste=1 -e examples/,tools/ .

help:
	@echo "Targets:"
	@echo "  loadtest          Run tools/loadtest against local NATS"
	@echo "  demo-nats         Run examples/nats (requires local JetStream)"
	@echo "  demo              Alias for demo-nats"
	@echo "  dev               Print local broker + next steps"
	@echo "  test              Run all unit tests (max parallel)"
	@echo "  test-race         Run tests with -race"
	@echo "  coverage          Write coverage.out, print summary, enforce COVERAGE_MIN ($(COVERAGE_MIN)%)"
	@echo "  coverage-html     Open HTML coverage report (requires coverage.out)"
	@echo "  bench             Run all benchmarks"
	@echo "  bench-codec       Run BenchmarkCodecComparison only"
	@echo "  bench-payload     Pure + pub/sub + request-reply compress matrices"
	@echo "  bench-compete     Wrapper tax vs legacy nats.go JetStream"
	@echo "  fuzz              Fuzz smoke (15s)"
	@echo "  ci                fmt-check + unit tests + race detector + vet + lint"
	@echo "  fmt               gofmt -w all Go files"
	@echo "  fmt-check         fail if any file needs gofmt"
	@echo "  lint              Run govulncheck + goalign + golangci-lint (includes fieldalignment)"
	@echo "  lint-fix          Run golangci-lint with --fix"
	@echo "  govulncheck       Scan dependencies for known vulnerabilities"
	@echo "  align             Fail if goalign finds waste >= 1 byte (excludes examples/,tools/)"
	@echo "  align-fix        Reorder struct fields via betteralign -apply (local; then make align)"
	@echo "  vet               go vet on all packages"
	@echo "  examples          Build example programs"
	@echo ""
	@echo "Docker JetStream labs live in nats-console: make nats-up / nats-cluster-up there."

demo-nats:
	go run ./examples/nats

demo: demo-nats

dev:
	@echo "Start a local JetStream broker, then run demos/tests."
	@echo "  Labs: https://github.com/gopherust-io/nats-console/tree/main/docker/nats"
	@echo "  In nats-console: make nats-up"
	@echo "  Or: nats-server -js"
	@echo ""
	@echo "Next:"
	@echo "  make demo-nats    # JetStream publish/consume demo"
	@echo "  make loadtest     # publish/consume load harness"
	@echo "  make test         # unit tests"

test:
	go test $(GO_TEST_FLAGS) $(PKGS)

test-race:
	go test -race $(GO_TEST_FLAGS) $(PKGS)

coverage:
	go test $(GO_TEST_FLAGS) $(PKGS) -coverprofile=coverage.out
	@total=$$(go tool cover -func=coverage.out | awk '/^total:/ {gsub(/%/,"",$$NF); print $$NF}'); \
	echo "Total coverage: $${total}% (minimum $(COVERAGE_MIN)%)"; \
	awk -v t="$${total}" -v m="$(COVERAGE_MIN)" 'BEGIN { if (t+0 < m+0) { print "coverage below threshold"; exit 1 } }'
	go tool cover -func=coverage.out

coverage-html: coverage
	go tool cover -html=coverage.out -o coverage.html
	@echo "Wrote coverage.html"

bench:
	go test -bench=. -benchmem $(PKGS) -run '^$$'

bench-codec:
	go test -bench=BenchmarkCodecComparison -benchmem ./... -run '^$$'

bench-payload:
	go test -bench='BenchmarkPayload|BenchmarkPubSubPayload|BenchmarkRequestReplyPayload' -benchmem -count=1 . -run '^$$'

bench-compete:
	go test -bench=BenchmarkCmp -benchmem -count=1 -benchtime=100x ./benchcmp/ -run '^$$'

loadtest:
	go run ./tools/loadtest -nats nats://127.0.0.1:4222

fuzz:
	@set -e; for fuzz in $(FUZZ_TESTS); do \
		go test -fuzz=$$fuzz -fuzztime=15s $(FUZZ_PKGS) -run '^$$'; \
	done

ci: fmt-check test test-race vet lint

vet:
	go vet ./...

fmt:
	gofmt -w .

fmt-check:
	@test -z "$$(gofmt -l .)" || (gofmt -l . && exit 1)

lint: govulncheck align
	$(GOLANGCI_LINT) run ./...

lint-fix:
	$(GOLANGCI_LINT) run ./... --fix

govulncheck:
	$(GOVULNCHECK) ./...

align: $(GOALIGN_BIN)
	$(GOALIGN_BIN) $(GOALIGN_FLAGS)

$(GOALIGN_BIN):
	go install github.com/gopherust-io/goalign@$(GOALIGN_VERSION)

align-fix:
	$(BETTERALIGN) -apply ./...

examples:
	go build -o /dev/null ./examples/...
