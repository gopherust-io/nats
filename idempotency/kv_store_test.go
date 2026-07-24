package idempotency

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libnats "github.com/gopherust-io/nats"
)

type fakeEntry struct {
	key   string
	value []byte
}

func (e *fakeEntry) Bucket() string     { return "test" }
func (e *fakeEntry) Key() string        { return e.key }
func (e *fakeEntry) Value() []byte      { return e.value }
func (e *fakeEntry) Revision() uint64   { return 1 }
func (e *fakeEntry) Created() time.Time { return time.Time{} }
func (e *fakeEntry) Delta() uint64      { return 0 }
func (e *fakeEntry) Operation() natspkg.KeyValueOp {
	return natspkg.KeyValuePut
}

type fakeKV struct {
	data map[string][]byte
	mu   sync.Mutex
}

func newFakeKV() *fakeKV {
	return &fakeKV{data: make(map[string][]byte)}
}

func (f *fakeKV) Get(key string) (natspkg.KeyValueEntry, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	v, ok := f.data[key]
	if !ok {
		return nil, natspkg.ErrKeyNotFound
	}

	return &fakeEntry{key: key, value: v}, nil
}

func (f *fakeKV) Put(key string, value []byte) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.data[key] = append([]byte(nil), value...)
	return 1, nil
}

func (f *fakeKV) GetRevision(string, uint64) (natspkg.KeyValueEntry, error) {
	panic("unexpected")
}
func (f *fakeKV) PutString(string, string) (uint64, error) { panic("unexpected") }

func (f *fakeKV) Create(key string, value []byte) (uint64, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.data[key]; ok {
		return 0, natspkg.ErrKeyExists
	}
	f.data[key] = append([]byte(nil), value...)
	return 1, nil
}

func (f *fakeKV) Update(string, []byte, uint64) (uint64, error) {
	panic("unexpected")
}
func (f *fakeKV) Delete(string, ...natspkg.DeleteOpt) error { panic("unexpected") }

func (f *fakeKV) Purge(key string, _ ...natspkg.DeleteOpt) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if _, ok := f.data[key]; !ok {
		return natspkg.ErrKeyNotFound
	}
	delete(f.data, key)
	return nil
}
func (f *fakeKV) Watch(string, ...natspkg.WatchOpt) (natspkg.KeyWatcher, error) {
	panic("unexpected")
}
func (f *fakeKV) WatchAll(...natspkg.WatchOpt) (natspkg.KeyWatcher, error) {
	panic("unexpected")
}
func (f *fakeKV) WatchFiltered([]string, ...natspkg.WatchOpt) (natspkg.KeyWatcher, error) {
	panic("unexpected")
}
func (f *fakeKV) Keys(...natspkg.WatchOpt) ([]string, error) { panic("unexpected") }
func (f *fakeKV) ListKeys(...natspkg.WatchOpt) (natspkg.KeyLister, error) {
	panic("unexpected")
}
func (f *fakeKV) History(string, ...natspkg.WatchOpt) ([]natspkg.KeyValueEntry, error) {
	panic("unexpected")
}
func (f *fakeKV) Bucket() string                          { return "test" }
func (f *fakeKV) PurgeDeletes(...natspkg.PurgeOpt) error  { panic("unexpected") }
func (f *fakeKV) Status() (natspkg.KeyValueStatus, error) { panic("unexpected") }

func TestKVStoreSeenMark(t *testing.T) {
	store := NewKVStore(newFakeKV())
	ctx := context.Background()

	seen, err := store.Seen(ctx, "msg-1")
	require.NoError(t, err)
	assert.False(t, seen)

	require.NoError(t, store.Mark(ctx, "msg-1"))

	seen, err = store.Seen(ctx, "msg-1")
	require.NoError(t, err)
	assert.True(t, seen)
}

func TestKVStoreRejectsInvalidKey(t *testing.T) {
	store := NewKVStore(newFakeKV())
	ctx := context.Background()

	_, err := store.Seen(ctx, "")
	require.ErrorIs(t, err, libnats.ErrInvalidKVKey)

	err = store.Mark(ctx, "bad key")
	require.ErrorIs(t, err, libnats.ErrInvalidKVKey)
}

func TestKVStoreWithHandler(t *testing.T) {
	store := NewKVStore(newFakeKV())
	calls := 0
	handler := WithHandler(store, MsgIDFromHeader, func(_ context.Context, _ *natspkg.Msg) error {
		calls++
		return nil
	})

	msg := &natspkg.Msg{
		Header: natspkg.Header{libnats.HeaderMsgID: []string{"pay-42"}},
	}

	require.NoError(t, handler(context.Background(), msg))
	require.NoError(t, handler(context.Background(), msg))
	assert.Equal(t, 1, calls)
}

func TestKVStoreClaimRelease(t *testing.T) {
	store := NewKVStore(newFakeKV()).(ClaimStore)
	ctx := context.Background()

	ok, err := store.Claim(ctx, "id-1")
	require.NoError(t, err)
	assert.True(t, ok)

	ok, err = store.Claim(ctx, "id-1")
	require.NoError(t, err)
	assert.False(t, ok)

	require.NoError(t, store.Release(ctx, "id-1"))

	ok, err = store.Claim(ctx, "id-1")
	require.NoError(t, err)
	assert.True(t, ok)
}

func TestKVStoreWithHandlerReleasesOnError(t *testing.T) {
	store := NewKVStore(newFakeKV())
	handler := WithHandler(store, MsgIDFromHeader, func(_ context.Context, _ *natspkg.Msg) error {
		return assert.AnError
	})
	msg := &natspkg.Msg{Header: natspkg.Header{libnats.HeaderMsgID: []string{"id-err"}}}
	require.Error(t, handler(context.Background(), msg))

	seen, err := store.Seen(context.Background(), "id-err")
	require.NoError(t, err)
	assert.False(t, seen)
}

func TestKVStoreConcurrentClaims(t *testing.T) {
	store := NewKVStore(newFakeKV()).(ClaimStore)
	ctx := context.Background()
	const n = 32
	var acquired atomic.Int64
	var wg sync.WaitGroup
	wg.Add(n)
	for i := 0; i < n; i++ {
		go func() {
			defer wg.Done()
			ok, err := store.Claim(ctx, "race-id")
			require.NoError(t, err)
			if ok {
				acquired.Add(1)
			}
		}()
	}
	wg.Wait()
	assert.Equal(t, int64(1), acquired.Load())
}
