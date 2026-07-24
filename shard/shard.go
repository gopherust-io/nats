package shard

import (
	"hash/fnv"
)

const itoaBufLen = 20

// Index returns a stable shard index for key in [0, numShards).
func Index(key string, numShards int) int {
	if numShards <= 0 {
		return 0
	}

	h := fnv.New32a()
	_, _ = h.Write([]byte(key))

	return int(h.Sum32() % uint32(numShards))
}

// Subject builds a subject for keyed sharding, e.g. Subject("orders.shard", "acct-1", 8, "created")
// → "orders.shard.3.created".
func Subject(prefix, key string, numShards int, action string) string {
	idx := Index(key, numShards)
	if action == "" {
		return prefix + "." + itoa(idx)
	}

	return prefix + "." + itoa(idx) + "." + action
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

	return string(buf[i:])
}
