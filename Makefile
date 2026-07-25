export GOWORK := off

PKGS := $(shell go list ./... | grep -vE '/(examples|tools)/')
FUZZ_PKGS := .
FUZZ_TESTS := FuzzDecodeJSON FuzzCommonWildcardSubject FuzzShardIndex
NPROCS := $(shell getconf _NPROCESSORS_ONLN 2>/dev/null || sysctl -n hw.ncpu 2>/dev/null || echo 4)
GO_TEST_FLAGS := -count=1 -parallel=$(NPROCS) -timeout=60s
COVERAGE_MIN ?= 75
NATS_COMPOSE := docker/nats/single/docker-compose.yml
NATS_CLUSTER_COMPOSE := docker/nats/cluster/docker-compose.yml
NATS_AUTH_COMPOSE := docker/nats/auth/docker-compose.yml
NATS_HEALTHZ := http://127.0.0.1:8222/healthz

.PHONY: help test test-race coverage coverage-html bench bench-codec fuzz ci vet fmt fmt-check lint lint-fix govulncheck align align-fix examples \
	nats-up nats-down nats-cluster-up nats-auth-up nats-down-all loadtest demo demo-nats dev

GOLANGCI_LINT := go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.12.2
GOVULNCHECK := go run golang.org/x/vuln/cmd/govulncheck@v1.6.0
BETTERALIGN := go run github.com/dkorunic/betteralign/cmd/betteralign@v0.7.2
GOALIGN_VERSION := v1.1.0
GOALIGN_BIN := $(CURDIR)/.cache/goalign-$(GOALIGN_VERSION)
GOALIGN_FLAGS := analyze -r --arch=amd64 --fail-on-findings --min-waste=1 -e examples/,tools/ .

help:
	@echo "Targets:"
	@echo "  nats-up           Start single-node JetStream (Docker) and wait for healthz"
	@echo "  nats-cluster-up   Start 3-node cluster lab"
	@echo "  nats-auth-up      Start auth/subject-permissions lab"
	@echo "  nats-down         Stop single-node stack"
	@echo "  nats-down-all     Stop single, cluster, auth, and supercluster stacks"
	@echo "  loadtest          Run tools/loadtest against local NATS"
	@echo "  demo-nats         Run examples/nats (requires nats-up)"
	@echo "  demo              Alias for demo-nats"
	@echo "  dev               nats-up then print next steps"
	@echo "  test              Run all unit tests (max parallel)"
	@echo "  test-race         Run tests with -race"
	@echo "  coverage          Write coverage.out, print summary, enforce COVERAGE_MIN ($(COVERAGE_MIN)%)"
	@echo "  coverage-html     Open HTML coverage report (requires coverage.out)"
	@echo "  bench             Run all benchmarks"
	@echo "  bench-codec       Run BenchmarkCodecComparison only"
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

nats-up:
	docker compose -f $(NATS_COMPOSE) up -d
	@echo "Waiting for JetStream healthz..."
	@i=0; \
	while [ $$i -lt 30 ]; do \
		if curl -sf $(NATS_HEALTHZ) >/dev/null 2>&1; then \
			echo "NATS ready: nats://127.0.0.1:4222  monitor $(NATS_HEALTHZ)"; \
			exit 0; \
		fi; \
		i=$$((i+1)); \
		sleep 1; \
	done; \
	echo "Timed out waiting for $(NATS_HEALTHZ)"; \
	exit 1

nats-cluster-up:
	docker compose -f $(NATS_CLUSTER_COMPOSE) up -d
	@echo "Cluster lab up (ports 4222-4224). See docs/nats/local-docker.md"

nats-auth-up:
	docker compose -f $(NATS_AUTH_COMPOSE) up -d
	@echo "Auth lab up. Users: orders-pub/pubpass, orders-worker/workerpass, js-admin/adminpass"

nats-down:
	docker compose -f $(NATS_COMPOSE) down

nats-down-all:
	-docker compose -f $(NATS_COMPOSE) down -v
	-docker compose -f $(NATS_CLUSTER_COMPOSE) down -v
	-docker compose -f $(NATS_AUTH_COMPOSE) down -v
	-docker compose -f docker/nats/supercluster/docker-compose.yml down -v

loadtest:
	go run ./tools/loadtest -url nats://127.0.0.1:4222

demo-nats:
	go run ./examples/nats

demo: demo-nats

dev: nats-up
	@echo ""
	@echo "Next:"
	@echo "  make demo-nats    # JetStream publish/consume demo"
	@echo "  make loadtest     # publish/consume load harness"
	@echo "  make test         # unit tests"
	@echo "  make nats-down    # stop JetStream"

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
	@mkdir -p $(dir $@)
	@tmpdir=$$(mktemp -d) && \
		curl -fsSL https://github.com/gopherust-io/goalign/archive/refs/tags/$(GOALIGN_VERSION).tar.gz | tar -xz -C $$tmpdir && \
		(cd $$tmpdir/goalign-$(patsubst v%,%,$(GOALIGN_VERSION)) && go build -o $(GOALIGN_BIN) .) && \
		rm -rf $$tmpdir

align-fix:
	$(BETTERALIGN) -apply ./...

examples:
	go build -o /dev/null ./examples/...
