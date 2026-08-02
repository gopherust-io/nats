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

type memClaimStore struct {
	mu      sync.Mutex
	pending map[string]struct{}
	done    map[string]struct{}
}

func (m *memClaimStore) Seen(_ context.Context, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, ok := m.done[id]
	return ok, nil
}

func (m *memClaimStore) Mark(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done == nil {
		m.done = make(map[string]struct{})
	}
	delete(m.pending, id)
	m.done[id] = struct{}{}
	return nil
}

func (m *memClaimStore) Claim(_ context.Context, id string) (bool, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.pending == nil {
		m.pending = make(map[string]struct{})
	}
	if _, ok := m.pending[id]; ok {
		return false, nil
	}
	if _, ok := m.done[id]; ok {
		return false, nil
	}
	m.pending[id] = struct{}{}
	return true, nil
}

func (m *memClaimStore) Release(_ context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.pending, id)
	return nil
}

func TestWithHandlerClaimInFlight(t *testing.T) {
	store := &memClaimStore{}
	acquired, err := store.Claim(context.Background(), "id-1")
	require.NoError(t, err)
	require.True(t, acquired)

	calls := 0
	handler := WithHandler(store, MsgIDFromHeader, func(_ context.Context, _ *natspkg.Msg) error {
		calls++
		return nil
	})
	err = handler(context.Background(), &natspkg.Msg{
		Header: natspkg.Header{libnats.HeaderMsgID: []string{"id-1"}},
	})
	require.ErrorIs(t, err, ErrClaimInFlight)
	assert.Equal(t, 0, calls)
}

func TestWithHandlerClaimDoneAcks(t *testing.T) {
	store := &memClaimStore{}
	calls := 0
	handler := WithHandler(store, MsgIDFromHeader, func(_ context.Context, _ *natspkg.Msg) error {
		calls++
		return nil
	})
	msg := &natspkg.Msg{Header: natspkg.Header{libnats.HeaderMsgID: []string{"id-1"}}}
	require.NoError(t, handler(context.Background(), msg))
	require.NoError(t, handler(context.Background(), msg))
	assert.Equal(t, 1, calls)
}

func TestWithHandlerReleaseFailureSurfaced(t *testing.T) {
	store := &failReleaseStore{}
	handler := WithHandler(store, MsgIDFromHeader, func(_ context.Context, _ *natspkg.Msg) error {
		return assert.AnError
	})
	err := handler(context.Background(), &natspkg.Msg{
		Header: natspkg.Header{libnats.HeaderMsgID: []string{"id-1"}},
	})
	require.Error(t, err)
	assert.ErrorContains(t, err, "idempotency release")
}

type failReleaseStore struct {
	memClaimStore
}

func (f *failReleaseStore) Release(context.Context, string) error {
	return assert.AnError
}
