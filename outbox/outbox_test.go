package outbox_test

import (
	"context"
	"sync"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gopherust-io/nats/outbox"
)

type memStore struct {
	mu   sync.Mutex
	data map[string][]byte
}

func newMemStore() *memStore {
	return &memStore{data: make(map[string][]byte)}
}

func (s *memStore) Put(_ context.Context, key string, value []byte) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.data[key] = append([]byte(nil), value...)

	return nil
}

func (s *memStore) Get(_ context.Context, key string) ([]byte, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	v, ok := s.data[key]
	if !ok {
		return nil, assert.AnError
	}

	return append([]byte(nil), v...), nil
}

func (s *memStore) Delete(_ context.Context, key string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.data, key)

	return nil
}

func (s *memStore) Keys(_ context.Context) ([]string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	keys := make([]string, 0, len(s.data))
	for k := range s.data {
		keys = append(keys, k)
	}

	return keys, nil
}

type memPub struct {
	n int
}

func (p *memPub) PublishRaw(context.Context, string, []byte, map[string]string) error {
	p.n++

	return nil
}

func TestOutboxEnqueueFlush(t *testing.T) {
	t.Parallel()
	store := newMemStore()
	pub := &memPub{}
	rec, err := outbox.Enqueue(context.Background(), store, "orders.created", []byte(`{}`), nil)
	require.NoError(t, err)
	require.NotEmpty(t, rec.ID)

	n, err := outbox.Flush(context.Background(), store, pub, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	assert.Equal(t, 1, pub.n)
	keys, err := store.Keys(context.Background())
	require.NoError(t, err)
	assert.Empty(t, keys)
}
