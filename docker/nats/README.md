# Local NATS (Docker Compose)

Ready-to-run JetStream topologies for developing against `github.com/gopherust-io/nats`.

| Topology | Compose | Client `Address` |
|----------|---------|------------------|
| Single | [`single/`](single/) | `nats://127.0.0.1:4222` |
| Cluster (3 nodes) | [`cluster/`](cluster/) | `nats://127.0.0.1:4222,nats://127.0.0.1:4223,nats://127.0.0.1:4224` |
| Supercluster (2×2) | [`supercluster/`](supercluster/) | East: `…4222,4223` · West: `…4225,4226` |
| Auth (users + permissions) | [`auth/`](auth/) | `nats://127.0.0.1:4222` + User/Password |

Full guide: [docs/nats/local-docker.md](../../docs/nats/local-docker.md).

```bash
# Everyday local
docker compose -f docker/nats/single/docker-compose.yml up -d

# Tear down (wipe JetStream volumes)
docker compose -f docker/nats/single/docker-compose.yml down -v
```

**Do not** run `cluster` and `supercluster` at the same time (shared host ports `4222`/`4223`).

These confs have **no TLS and no auth** — local development only. Production HA cluster / supercluster and security: [docs/nats/devops.md — Part B Server](../../docs/nats/devops.md#part-b--nats-server).
