# Compatibility

| Component | Version / note |
|-----------|----------------|
| Go | See `go` directive in [`go.mod`](../go.mod) |
| `github.com/nats-io/nats.go` | See `go.mod` require |
| Embedded test server `nats-server/v2` | See `go.mod` require (currently `v2.14.x`) |
| Docker lab image | `nats:2.14` under [`docker/nats/`](../docker/nats/) |

Keep the **lab image major.minor** aligned with the `nats-server` module used in tests when bumping dependencies.

### Client auth mechanisms supported

| Mechanism | Config field |
|-----------|--------------|
| User/password | `Conn.User` + `Conn.Password` |
| Token | `Conn.Secret` |
| NKey | `Conn.Seed` |
| Operator JWT creds file | `Conn.CredentialsFile` |

Only one mechanism may be set at a time.

### Breaking changes

Document API breaks in [`CHANGELOG.md`](../CHANGELOG.md). Consumer immutable setting changes still require delete+recreate (`ErrConsumerRecreateRequired`).
