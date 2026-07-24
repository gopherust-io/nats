package nats

import "github.com/gopherust-io/nats/shard"

// ShardIndex returns a stable shard index for key in [0, numShards).
// Prefer importing github.com/gopherust-io/nats/shard directly.
func ShardIndex(key string, numShards int) int {
	return shard.Index(key, numShards)
}

// ShardSubject builds a subject for keyed sharding.
// Prefer importing github.com/gopherust-io/nats/shard directly.
func ShardSubject(prefix, key string, numShards int, action string) string {
	return shard.Subject(prefix, key, numShards, action)
}
