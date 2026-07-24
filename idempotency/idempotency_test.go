package idempotency

import (
	"context"
	"sync"
	"testing"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libnats "github.com/gopherust-io/nats"
)

type memStore struct {
	seen map[string]struct{}
	mu   sync.Mutex
}

func (m *memStore) Seen(_ context.Context, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.seen[id]
	return ok, nil
}

func (m *memStore) Mark(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.seen == nil {
		m.seen = make(map[string]struct{})
	}
	m.seen[id] = struct{}{}
	return nil
}

func TestWithHandlerDedup(t *testing.T) {
	store := &memStore{}
	calls := 0
	handler := WithHandler(store, MsgIDFromHeader, func(_ context.Context, _ *natspkg.Msg) error {
		calls++
		return nil
	})

	msg := &natspkg.Msg{
		Header: natspkg.Header{libnats.HeaderMsgID: []string{"id-1"}},
	}

	require.NoError(t, handler(context.Background(), msg))
	require.NoError(t, handler(context.Background(), msg))
	assert.Equal(t, 1, calls)
}

func TestMsgIDFromHeader(t *testing.T) {
	msg := &natspkg.Msg{
		Header: natspkg.Header{libnats.HeaderMsgID: []string{"abc"}},
	}
	assert.Equal(t, "abc", MsgIDFromHeader(msg))
	assert.Empty(t, MsgIDFromHeader(nil))
}

func TestWithHandlerEmptyIDSkipsDedup(t *testing.T) {
	store := &memStore{}
	calls := 0
	handler := WithHandler(store, func(msg *natspkg.Msg) string { return "" },
		func(_ context.Context, _ *natspkg.Msg) error {
			calls++
			return nil
		})
	require.NoError(t, handler(context.Background(), &natspkg.Msg{}))
	require.NoError(t, handler(context.Background(), &natspkg.Msg{}))
	assert.Equal(t, 2, calls)
}

func TestWithHandlerErrorDoesNotMark(t *testing.T) {
	store := &memStore{}
	handler := WithHandler(store, func(msg *natspkg.Msg) string { return "id-1" },
		func(_ context.Context, _ *natspkg.Msg) error { return assert.AnError })

	msg := &natspkg.Msg{Header: natspkg.Header{libnats.HeaderMsgID: []string{"id-1"}}}
	require.Error(t, handler(context.Background(), msg))
	seen, err := store.Seen(context.Background(), "id-1")
	require.NoError(t, err)
	assert.False(t, seen)
}
