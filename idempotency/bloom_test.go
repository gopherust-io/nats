package idempotency

import (
	"context"
	"fmt"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestBloomStoreSeenMark(t *testing.T) {
	t.Parallel()
	store := NewBloomStore(1024, 4)
	ctx := context.Background()

	seen, err := store.Seen(ctx, "a")
	require.NoError(t, err)
	assert.False(t, seen)

	require.NoError(t, store.Mark(ctx, "a"))
	seen, err = store.Seen(ctx, "a")
	require.NoError(t, err)
	assert.True(t, seen)

	seen, err = store.Seen(ctx, "b")
	require.NoError(t, err)
	assert.False(t, seen)
}

func TestBloomStoreDefaultsAndBackend(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	backend := &memStore{}
	store := NewBloomStore(0, 0).WithBackend(backend)
	require.NotNil(t, store)
	assert.Equal(t, defaultBloomHashes, store.hashes)
	assert.Equal(t, (defaultBloomBits+63)/64, len(store.bits))

	require.NoError(t, store.Mark(ctx, "backed"))
	seen, err := store.Seen(ctx, "backed")
	require.NoError(t, err)
	assert.True(t, seen)

	backendSeen, err := backend.Seen(ctx, "backed")
	require.NoError(t, err)
	assert.True(t, backendSeen)
}

func TestBloomStoreConcurrentSeenMark(t *testing.T) {
	t.Parallel()
	store := NewBloomStore(1<<16, 7)
	ctx := context.Background()

	const goroutines = 32
	const perG = 200

	var wg sync.WaitGroup
	for g := range goroutines {
		wg.Go(func() {
			for i := range perG {
				id := fmt.Sprintf("id-%d-%d", g, i)
				require.NoError(t, store.Mark(ctx, id))
				seen, err := store.Seen(ctx, id)
				require.NoError(t, err)
				assert.True(t, seen)
			}
		})
	}
	wg.Wait()
}
