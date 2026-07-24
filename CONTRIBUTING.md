# Contributing

## Prerequisites

- Go version from [`go.mod`](go.mod)
- Docker (for local JetStream labs)
- `make` helpers (optional)

## Development

```bash
make nats-up          # single-node JetStream
make test             # unit tests
make test-race        # race detector
make lint             # govulncheck + golangci-lint
make fuzz             # fuzz smoke
make examples         # build examples
```

Cluster / auth labs:

```bash
make nats-cluster-up
make nats-auth-up
make nats-down-all
```

## Pull requests

1. Keep changes focused; match existing package style (small interfaces, table-driven tests).
2. Run `make fmt-check`, `make test`, and `make lint` before opening a PR.
3. Update docs when changing public APIs or presets (`docs/nats/`, root `README.md`, `CHANGELOG.md`).
4. Do not commit secrets; auth lab passwords are local-only plaintext by design.

CI on PRs runs format, vet, unit tests, examples build, and golangci-lint. Race, fuzz, benchmarks, and govulncheck run on pushes to `main`. Local `make ci` also runs race and govulncheck (stricter than PR CI).

Security reports: see [SECURITY.md](SECURITY.md). Do not open public issues for vulnerabilities.

## Compatibility

See [docs/compatibility.md](docs/compatibility.md).
