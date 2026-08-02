# Contributing

## Prerequisites

- Go version from [`go.mod`](go.mod)
- Docker optional — JetStream Compose labs live in [nats-console `docker/nats`](https://github.com/gopherust-io/nats-console/tree/main/docker/nats)
- `make` helpers (optional)

## Development

```bash
make test             # unit tests
make test-race        # race detector
make lint             # govulncheck + golangci-lint
make fuzz             # fuzz smoke
make examples         # build examples
```

Local broker for demos / loadtest (pick one):

```bash
# nats-console checkout
make nats-up

# or
nats-server -js
```

Then in this repo: `make demo-nats` / `make loadtest`. Full lab guide: [local-docker.md](docs/nats/local-docker.md).

## Pull requests

1. Keep changes focused; match existing package style (small interfaces, table-driven tests).
2. Run `make fmt-check`, `make test`, and `make lint` before opening a PR.
3. Update docs when changing public APIs or config recipes (`docs/nats/`, root `README.md`, `CHANGELOG.md`).
4. Do not commit secrets.

CI on PRs and `main` runs format, vet, unit tests, examples build, golangci-lint, race, fuzz, benchmarks, and govulncheck.

Security reports: see [SECURITY.md](SECURITY.md). Do not open public issues for vulnerabilities.

## Compatibility

See [docs/compatibility.md](docs/compatibility.md).
