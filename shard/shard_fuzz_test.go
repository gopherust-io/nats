package shard

import "testing"

func FuzzIndex(f *testing.F) {
	f.Add("account-1", 8)
	f.Fuzz(func(t *testing.T, key string, shards int) {
		if shards <= 0 {
			return
		}
		idx := Index(key, shards)
		if idx < 0 || idx >= shards {
			t.Fatalf("Index(%q, %d) = %d out of range", key, shards, idx)
		}
	})
}
