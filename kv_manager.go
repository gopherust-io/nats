package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// KVBucketStatus summarizes a Key-Value bucket.
type KVBucketStatus struct {
	Bucket  string
	Values  uint64
	History int64
}

// KVEntry is a Key-Value entry with payload.
type KVEntry struct {
	Created  time.Time
	Bucket   string
	Key      string
	Value    []byte
	Revision uint64
}

// KeyValueManager manages JetStream Key-Value buckets.
type KeyValueManager interface {
	CreateOrUpdate(ctx context.Context, cfg KeyValueConfig) (natspkg.KeyValue, error)
	CreateRaw(ctx context.Context, cfg *natspkg.KeyValueConfig) (KVBucketStatus, error)
	Open(ctx context.Context, bucket string) (natspkg.KeyValue, error)
	Delete(ctx context.Context, bucket string) error
	ListBuckets(ctx context.Context) ([]KVBucketStatus, error)
	BucketInfo(ctx context.Context, bucket string) (KVBucketStatus, error)
}

// KeyValueKeys provides key-level KV helpers (kept separate to avoid interfacebloat).
type KeyValueKeys interface {
	ListKeys(ctx context.Context, bucket string, offset, limit int) ([]string, int, error)
	Get(ctx context.Context, bucket, key string) (*KVEntry, error)
	Put(ctx context.Context, bucket, key string, value []byte) (*KVEntry, error)
	DeleteKey(ctx context.Context, bucket, key string) error
	History(ctx context.Context, bucket, key string) ([]KVEntry, error)
}

type keyValueManager struct {
	js    natspkg.JetStreamContext
	jsNew jetstream.JetStream
}

func newKeyValueManager(conn *natspkg.Conn, js natspkg.JetStreamContext) *keyValueManager {
	mgr := &keyValueManager{js: js}
	if jsNew, err := jetstream.New(conn); err == nil {
		mgr.jsNew = jsNew
	}

	return mgr
}

func toJetStreamKeyValueConfig(cfg KeyValueConfig) jetstream.KeyValueConfig {
	history := cfg.History
	if history == 0 {
		history = 1
	}

	kc := jetstream.KeyValueConfig{
		Bucket:      cfg.Bucket,
		Description: cfg.Description,
		History:     history,
		TTL:         cfg.TTL,
		MaxBytes:    cfg.MaxBytes,
		Replicas:    cfg.Replicas,
		Compression: cfg.Compression,
	}
	if cfg.Storage != 0 {
		kc.Storage = jetstream.StorageType(cfg.Storage)
	}

	return kc
}

func kvBucketStatusFrom(status natspkg.KeyValueStatus) KVBucketStatus {
	return KVBucketStatus{
		Bucket:  status.Bucket(),
		Values:  status.Values(),
		History: status.History(),
	}
}

func kvEntryFrom(bucket string, entry natspkg.KeyValueEntry) *KVEntry {
	return &KVEntry{
		Bucket:   bucket,
		Key:      entry.Key(),
		Value:    entry.Value(),
		Revision: entry.Revision(),
		Created:  entry.Created(),
	}
}

func (m *keyValueManager) CreateOrUpdate(ctx context.Context, cfg KeyValueConfig) (natspkg.KeyValue, error) {
	if err := ValidateBucketName(cfg.Bucket); err != nil {
		return nil, err
	}

	if m.jsNew == nil {
		return nil, fmt.Errorf("create or update kv bucket %q: %w", cfg.Bucket, ErrJetStreamV2Required)
	}

	_, err := m.jsNew.CreateOrUpdateKeyValue(ctx, toJetStreamKeyValueConfig(cfg))
	if err != nil {
		return nil, fmt.Errorf("create or update kv bucket %q: %w", cfg.Bucket, err)
	}

	kv, openErr := m.js.KeyValue(cfg.Bucket)
	if openErr != nil {
		return nil, fmt.Errorf("open kv bucket %q after create/update: %w", cfg.Bucket, openErr)
	}

	return kv, nil
}

func (m *keyValueManager) CreateRaw(_ context.Context, cfg *natspkg.KeyValueConfig) (KVBucketStatus, error) {
	if cfg == nil {
		return KVBucketStatus{}, fmt.Errorf("create kv bucket: %w", ErrEmptyConfigNotAllowed)
	}

	if err := ValidateBucketName(cfg.Bucket); err != nil {
		return KVBucketStatus{}, err
	}

	kv, err := m.js.CreateKeyValue(cfg)
	if err != nil {
		return KVBucketStatus{}, fmt.Errorf("create kv bucket %q: %w", cfg.Bucket, err)
	}

	status, err := kv.Status()
	if err != nil {
		return KVBucketStatus{}, fmt.Errorf("create kv bucket %q: status: %w", cfg.Bucket, err)
	}

	return kvBucketStatusFrom(status), nil
}

func (m *keyValueManager) Open(_ context.Context, bucket string) (natspkg.KeyValue, error) {
	if err := ValidateBucketName(bucket); err != nil {
		return nil, err
	}

	kv, err := m.js.KeyValue(bucket)
	if err != nil {
		return nil, fmt.Errorf("open kv bucket %q: %w", bucket, err)
	}

	return kv, nil
}

func (m *keyValueManager) Delete(_ context.Context, bucket string) error {
	if err := ValidateBucketName(bucket); err != nil {
		return err
	}

	if err := m.js.DeleteKeyValue(bucket); err != nil {
		return fmt.Errorf("delete kv bucket %q: %w", bucket, err)
	}

	return nil
}

func (m *keyValueManager) ListBuckets(_ context.Context) ([]KVBucketStatus, error) {
	out := make([]KVBucketStatus, 0)

	for name := range m.js.KeyValueStoreNames() {
		kv, err := m.js.KeyValue(name)
		if err != nil {
			return nil, fmt.Errorf("list kv buckets: open %q: %w", name, err)
		}

		status, err := kv.Status()
		if err != nil {
			return nil, fmt.Errorf("list kv buckets: status %q: %w", name, err)
		}

		out = append(out, kvBucketStatusFrom(status))
	}

	return out, nil
}

func (m *keyValueManager) BucketInfo(_ context.Context, bucket string) (KVBucketStatus, error) {
	if err := ValidateBucketName(bucket); err != nil {
		return KVBucketStatus{}, err
	}

	kv, err := m.js.KeyValue(bucket)
	if err != nil {
		return KVBucketStatus{}, fmt.Errorf("kv bucket info %q: %w", bucket, err)
	}

	status, err := kv.Status()
	if err != nil {
		return KVBucketStatus{}, fmt.Errorf("kv bucket info %q: status: %w", bucket, err)
	}

	return kvBucketStatusFrom(status), nil
}

func (m *keyValueManager) ListKeys(_ context.Context, bucket string, offset, limit int) ([]string, int, error) {
	if err := ValidateBucketName(bucket); err != nil {
		return nil, 0, err
	}

	kv, err := m.js.KeyValue(bucket)
	if err != nil {
		return nil, 0, fmt.Errorf("list kv keys bucket=%q: %w", bucket, err)
	}

	keys, err := kv.Keys()
	if errors.Is(err, natspkg.ErrNoKeysFound) {
		page, total := pageSlice([]string{}, offset, limit)

		return page, total, nil
	}

	if err != nil {
		return nil, 0, fmt.Errorf("list kv keys bucket=%q: %w", bucket, err)
	}

	page, total := pageSlice(keys, offset, limit)

	return page, total, nil
}

func (m *keyValueManager) Get(_ context.Context, bucket, key string) (*KVEntry, error) {
	if err := ValidateBucketName(bucket); err != nil {
		return nil, err
	}

	if err := ValidateKVKey(key); err != nil {
		return nil, err
	}

	kv, err := m.js.KeyValue(bucket)
	if err != nil {
		return nil, fmt.Errorf("get kv bucket=%q key=%q: %w", bucket, key, err)
	}

	entry, err := kv.Get(key)
	if err != nil {
		return nil, fmt.Errorf("get kv bucket=%q key=%q: %w", bucket, key, err)
	}

	return kvEntryFrom(bucket, entry), nil
}

func (m *keyValueManager) Put(_ context.Context, bucket, key string, value []byte) (*KVEntry, error) {
	if err := ValidateBucketName(bucket); err != nil {
		return nil, err
	}

	if err := ValidateKVKey(key); err != nil {
		return nil, err
	}

	kv, err := m.js.KeyValue(bucket)
	if err != nil {
		return nil, fmt.Errorf("put kv bucket=%q key=%q: %w", bucket, key, err)
	}

	revision, err := kv.Put(key, value)
	if err != nil {
		return nil, fmt.Errorf("put kv bucket=%q key=%q: %w", bucket, key, err)
	}

	entry, err := kv.Get(key)
	if err != nil {
		return nil, fmt.Errorf("put kv bucket=%q key=%q: get after put (rev=%d): %w", bucket, key, revision, err)
	}

	return kvEntryFrom(bucket, entry), nil
}

func (m *keyValueManager) DeleteKey(_ context.Context, bucket, key string) error {
	if err := ValidateBucketName(bucket); err != nil {
		return err
	}

	if err := ValidateKVKey(key); err != nil {
		return err
	}

	kv, err := m.js.KeyValue(bucket)
	if err != nil {
		return fmt.Errorf("delete kv key bucket=%q key=%q: %w", bucket, key, err)
	}

	if err := kv.Delete(key); err != nil {
		return fmt.Errorf("delete kv key bucket=%q key=%q: %w", bucket, key, err)
	}

	return nil
}

func (m *keyValueManager) History(_ context.Context, bucket, key string) ([]KVEntry, error) {
	if err := ValidateBucketName(bucket); err != nil {
		return nil, err
	}

	if err := ValidateKVKey(key); err != nil {
		return nil, err
	}

	kv, err := m.js.KeyValue(bucket)
	if err != nil {
		return nil, fmt.Errorf("kv history bucket=%q key=%q: %w", bucket, key, err)
	}

	entries, err := kv.History(key)
	if err != nil {
		return nil, fmt.Errorf("kv history bucket=%q key=%q: %w", bucket, key, err)
	}

	out := make([]KVEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, *kvEntryFrom(bucket, e))
	}

	return out, nil
}
