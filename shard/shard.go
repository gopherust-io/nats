package shard

import (
	"github.com/gopherust-io/nats/internal/bytesconv"
)

const (
	itoaBufLen = 20

	fnvOffset32 = 2166136261
	fnvPrime32  = 16777619
)

// Index returns a stable shard index for key in [0, numShards).
func Index(key string, numShards int) int {
	if numShards <= 0 {
		return 0
	}

	return int(fnv32aString(key) % uint32(numShards))
}

// Subject builds a subject for keyed sharding, e.g. Subject("orders.shard", "acct-1", 8, "created")
// → "orders.shard.3.created".
func Subject(prefix, key string, numShards int, action string) string {
	idx := Index(key, numShards)
	if bytesconv.IsEmpty(action) {
		return prefix + "." + itoa(idx)
	}

	return prefix + "." + itoa(idx) + "." + action
}

func fnv32aString(s string) uint32 {
	hash := uint32(fnvOffset32)
	for i := range len(s) {
		hash ^= uint32(s[i])
		hash *= fnvPrime32
	}

	return hash
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}

	var buf [itoaBufLen]byte

	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}

	// Must copy: buf is stack-backed; unsafe.String would dangle.
	return bytesconv.BytesToString(buf[i:])
}
