package nats

import (
	"context"
	"fmt"

	natspkg "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// KeyValueManager manages JetStream Key-Value buckets.
type KeyValueManager interface {
	CreateOrUpdate(ctx context.Context, cfg KeyValueConfig) (natspkg.KeyValue, error)
	Open(ctx context.Context, bucket string) (natspkg.KeyValue, error)
	Delete(ctx context.Context, bucket string) error
}

type keyValueManager struct {
	js    natspkg.JetStreamContext
	jsNew jetstream.JetStream
}

func newKeyValueManager(conn *natspkg.Conn, js natspkg.JetStreamContext) KeyValueManager {
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
