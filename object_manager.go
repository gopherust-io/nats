package nats

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"time"

	natspkg "github.com/nats-io/nats.go"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

// ObjectStoreConfig configures a JetStream Object Store bucket.
type ObjectStoreConfig struct {
	Bucket      string
	Description string
	TTL         time.Duration
	MaxBytes    int64
	Storage     StorageType
	Replicas    int
}

// ObjectBucketStatus summarizes an Object Store bucket.
type ObjectBucketStatus struct {
	Bucket      string
	Description string
	Size        uint64
}

// ObjectEntry is an object payload plus metadata.
type ObjectEntry struct {
	Modified time.Time
	Bucket   string
	Name     string
	Data     []byte
	Size     uint64
}

// ObjectStoreManager manages JetStream Object Store buckets and objects.
type ObjectStoreManager interface {
	ListBuckets(ctx context.Context) ([]ObjectBucketStatus, error)
	Create(ctx context.Context, cfg ObjectStoreConfig) (ObjectBucketStatus, error)
	CreateRaw(ctx context.Context, cfg *natspkg.ObjectStoreConfig) (ObjectBucketStatus, error)
	BucketInfo(ctx context.Context, bucket string) (ObjectBucketStatus, error)
	Delete(ctx context.Context, bucket string) error
	ListObjects(ctx context.Context, bucket string, offset, limit int) ([]string, int, error)
	Get(ctx context.Context, bucket, name string) (*ObjectEntry, error)
	Put(ctx context.Context, bucket, name string, data []byte) (*ObjectEntry, error)
	DeleteObject(ctx context.Context, bucket, name string) error
}

type objectStoreManager struct {
	js natspkg.JetStreamContext
}

func newObjectStoreManager(js natspkg.JetStreamContext) ObjectStoreManager {
	return &objectStoreManager{js: js}
}

func toNatsObjectStoreConfig(cfg ObjectStoreConfig) *natspkg.ObjectStoreConfig {
	oc := &natspkg.ObjectStoreConfig{
		Bucket:      cfg.Bucket,
		Description: cfg.Description,
		TTL:         cfg.TTL,
		MaxBytes:    cfg.MaxBytes,
		Replicas:    cfg.Replicas,
	}
	if cfg.Storage != 0 {
		oc.Storage = cfg.Storage
	}

	return oc
}

func objectBucketStatusFrom(status natspkg.ObjectStoreStatus) ObjectBucketStatus {
	return ObjectBucketStatus{
		Bucket:      status.Bucket(),
		Description: status.Description(),
		Size:        status.Size(),
	}
}

func (m *objectStoreManager) ListBuckets(_ context.Context) ([]ObjectBucketStatus, error) {
	out := make([]ObjectBucketStatus, 0)

	// ObjectStoreNames returns stream names (OBJ_<bucket>); ObjectStores yields
	// statuses with the bucket name already normalized.
	for status := range m.js.ObjectStores() {
		out = append(out, objectBucketStatusFrom(status))
	}

	return out, nil
}

func (m *objectStoreManager) Create(_ context.Context, cfg ObjectStoreConfig) (ObjectBucketStatus, error) {
	if err := ValidateBucketName(cfg.Bucket); err != nil {
		return ObjectBucketStatus{}, err
	}

	return m.createRaw(toNatsObjectStoreConfig(cfg))
}

func (m *objectStoreManager) CreateRaw(_ context.Context, cfg *natspkg.ObjectStoreConfig) (ObjectBucketStatus, error) {
	if cfg == nil {
		return ObjectBucketStatus{}, fmt.Errorf("create object bucket: %w", ErrEmptyConfigNotAllowed)
	}

	if err := ValidateBucketName(cfg.Bucket); err != nil {
		return ObjectBucketStatus{}, err
	}

	return m.createRaw(cfg)
}

func (m *objectStoreManager) createRaw(cfg *natspkg.ObjectStoreConfig) (ObjectBucketStatus, error) {
	os, err := m.js.CreateObjectStore(cfg)
	if err != nil {
		return ObjectBucketStatus{}, fmt.Errorf("create object bucket %q: %w", cfg.Bucket, err)
	}

	status, err := os.Status()
	if err != nil {
		return ObjectBucketStatus{}, fmt.Errorf("create object bucket %q: status: %w", cfg.Bucket, err)
	}

	return objectBucketStatusFrom(status), nil
}

func (m *objectStoreManager) BucketInfo(_ context.Context, bucket string) (ObjectBucketStatus, error) {
	if err := ValidateBucketName(bucket); err != nil {
		return ObjectBucketStatus{}, err
	}

	os, err := m.js.ObjectStore(bucket)
	if err != nil {
		return ObjectBucketStatus{}, fmt.Errorf("object bucket info %q: %w", bucket, err)
	}

	status, err := os.Status()
	if err != nil {
		return ObjectBucketStatus{}, fmt.Errorf("object bucket info %q: status: %w", bucket, err)
	}

	return objectBucketStatusFrom(status), nil
}

func (m *objectStoreManager) Delete(_ context.Context, bucket string) error {
	if err := ValidateBucketName(bucket); err != nil {
		return err
	}

	if err := m.js.DeleteObjectStore(bucket); err != nil {
		return fmt.Errorf("delete object bucket %q: %w", bucket, err)
	}

	return nil
}

func (m *objectStoreManager) ListObjects(_ context.Context, bucket string, offset, limit int) ([]string, int, error) {
	if err := ValidateBucketName(bucket); err != nil {
		return nil, 0, err
	}

	os, err := m.js.ObjectStore(bucket)
	if err != nil {
		return nil, 0, fmt.Errorf("list objects bucket=%q: %w", bucket, err)
	}

	infos, err := os.List()
	if errors.Is(err, natspkg.ErrNoObjectsFound) {
		page, total := pageSlice([]string{}, offset, limit)

		return page, total, nil
	}
	if err != nil {
		return nil, 0, fmt.Errorf("list objects bucket=%q: %w", bucket, err)
	}

	names := make([]string, 0, len(infos))
	for _, info := range infos {
		names = append(names, info.Name)
	}

	page, total := pageSlice(names, offset, limit)

	return page, total, nil
}

func (m *objectStoreManager) Get(_ context.Context, bucket, name string) (*ObjectEntry, error) {
	if err := ValidateBucketName(bucket); err != nil {
		return nil, err
	}

	if bytesconv.IsEmpty(name) {
		return nil, fmt.Errorf("get object bucket=%q: empty object name", bucket)
	}

	os, err := m.js.ObjectStore(bucket)
	if err != nil {
		return nil, fmt.Errorf("get object bucket=%q name=%q: %w", bucket, name, err)
	}

	info, err := os.GetInfo(name)
	if err != nil {
		return nil, fmt.Errorf("get object info bucket=%q name=%q: %w", bucket, name, err)
	}

	const maxObjectGetBytes = 64 << 20 // 64 MiB
	if info.Size > maxObjectGetBytes {
		return nil, fmt.Errorf("get object bucket=%q name=%q: size %d exceeds %d byte limit", bucket, name, info.Size, maxObjectGetBytes)
	}

	result, err := os.Get(name)
	if err != nil {
		return nil, fmt.Errorf("get object bucket=%q name=%q: %w", bucket, name, err)
	}
	defer func() { _ = result.Close() }()

	data := make([]byte, info.Size)
	if info.Size > 0 {
		if _, err := io.ReadFull(result, data); err != nil {
			return nil, fmt.Errorf("get object read bucket=%q name=%q: %w", bucket, name, err)
		}
	}

	return &ObjectEntry{
		Bucket:   bucket,
		Name:     name,
		Size:     info.Size,
		Data:     data,
		Modified: info.ModTime,
	}, nil
}

func (m *objectStoreManager) Put(_ context.Context, bucket, name string, data []byte) (*ObjectEntry, error) {
	if err := ValidateBucketName(bucket); err != nil {
		return nil, err
	}

	if bytesconv.IsEmpty(name) {
		return nil, fmt.Errorf("put object bucket=%q: empty object name", bucket)
	}

	os, err := m.js.ObjectStore(bucket)
	if err != nil {
		return nil, fmt.Errorf("put object bucket=%q name=%q: %w", bucket, name, err)
	}

	info, err := os.PutBytes(name, data)
	if err != nil {
		return nil, fmt.Errorf("put object bucket=%q name=%q: %w", bucket, name, err)
	}

	return &ObjectEntry{
		Bucket:   bucket,
		Name:     name,
		Size:     info.Size,
		Data:     bytes.Clone(data),
		Modified: info.ModTime,
	}, nil
}

func (m *objectStoreManager) DeleteObject(_ context.Context, bucket, name string) error {
	if err := ValidateBucketName(bucket); err != nil {
		return err
	}

	if bytesconv.IsEmpty(name) {
		return fmt.Errorf("delete object bucket=%q: empty object name", bucket)
	}

	os, err := m.js.ObjectStore(bucket)
	if err != nil {
		return fmt.Errorf("delete object bucket=%q name=%q: %w", bucket, name, err)
	}

	if err := os.Delete(name); err != nil {
		return fmt.Errorf("delete object bucket=%q name=%q: %w", bucket, name, err)
	}

	return nil
}
