package shard

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestIndexStable(t *testing.T) {
	a := Index("account-42", 8)
	b := Index("account-42", 8)
	assert.Equal(t, a, b)
	assert.GreaterOrEqual(t, a, 0)
	assert.Less(t, a, 8)
}

func TestIndexZeroShards(t *testing.T) {
	assert.Equal(t, 0, Index("key", 0))
	assert.Equal(t, 0, Index("key", -1))
}

func TestSubjectFormat(t *testing.T) {
	tests := []struct {
		prefix string
		key    string
		action string
		shards int
	}{
		{prefix: "orders.shard", key: "acct-1", shards: 8, action: "created"},
		{prefix: "events", key: "x", shards: 4, action: ""},
	}
	for _, tt := range tests {
		subject := Subject(tt.prefix, tt.key, tt.shards, tt.action)
		assert.Contains(t, subject, tt.prefix+".")
		idx := Index(tt.key, tt.shards)
		assert.Contains(t, subject, itoa(idx))
		if tt.action != "" {
			assert.Contains(t, subject, "."+tt.action)
		}
	}
}

func TestIndexInRange(t *testing.T) {
	const shards = 16
	keys := []string{"a", "b", "acct-1", "acct-2", "long-key-name"}
	for _, key := range keys {
		idx := Index(key, shards)
		assert.GreaterOrEqual(t, idx, 0)
		assert.Less(t, idx, shards)
	}
}
