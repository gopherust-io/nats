# Changelog

## v0.1.1

- Depend on `github.com/gopherust-io/tel` v0.1.1 (tracing/metrics hot-path optimizations).
- Reduce span attribute slice allocations on publish/process paths.

## v0.1.0

- Initial release: split from `github.com/gopherust-io/libs/nats` as module `github.com/gopherust-io/nats`.
- Depends on `github.com/gopherust-io/tel` for metrics and tracing.
