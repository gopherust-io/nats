# Local NATS with Docker Compose

Docker Compose labs for this library (single, 5-node cluster, supercluster, auth) live in **nats-console**:

- Guide: [nats-console `docs/local-docker.md`](https://github.com/gopherust-io/nats-console/blob/main/docs/local-docker.md)
- Compose + confs: [nats-console `docker/nats/`](https://github.com/gopherust-io/nats-console/tree/main/docker/nats)

```bash
# in a checkout of github.com/gopherust-io/nats-console
make nats-up            # single-node JetStream
make nats-cluster-up    # 5-node / Replicas: 5
make nats-auth-up       # users + subject permissions
make nats-down-all
```

Or run a local binary: `nats-server -js`.

Unit/integration tests in **this** repo embed an in-process `nats-server` and do **not** require Docker.

Production HA / security patterns: [Production operations — Part B Server](devops.md#part-b--nats-server).
