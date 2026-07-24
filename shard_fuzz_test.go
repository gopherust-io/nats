package nats

import "testing"

// FuzzShardIndex keeps the historical fuzz name in the root package for CI/Makefile.
func FuzzShardIndex(f *testing.F) {
	f.Add("account-1", 8)
	f.Fuzz(func(t *testing.T, key string, shards int) {
		if shards <= 0 {
			return
		}
		idx := ShardIndex(key, shards)
		if idx < 0 || idx >= shards {
			t.Fatalf("ShardIndex(%q, %d) = %d out of range", key, shards, idx)
		}
	})
}
