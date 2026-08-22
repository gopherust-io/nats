package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/nats-io/nuid"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

// Record is a pending publish stored in a KV outbox (DB-commit-then-publish pattern).
//
// goalign:ignore
type Record struct {
	CreatedAt time.Time         `json:"createdAt"`
	Headers   map[string]string `json:"headers,omitempty"`
	ID        string            `json:"id"`
	Subject   string            `json:"subject"`
	Data      []byte            `json:"data"`
}

// Store persists outbox records (typically a JetStream KV bucket).
type Store interface {
	Put(ctx context.Context, key string, value []byte) error
	Get(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
	Keys(ctx context.Context) ([]string, error)
}

// Publisher publishes outbox payloads.
type Publisher interface {
	PublishRaw(ctx context.Context, subject string, data []byte, headers map[string]string) error
}

// Enqueue stores a pending publish. Call after a successful local DB commit.
func Enqueue(ctx context.Context, store Store, subject string, data []byte, headers map[string]string) (*Record, error) {
	if store == nil {
		return nil, fmt.Errorf("outbox enqueue: store is nil")
	}
	if bytesconv.IsEmpty(subject) {
		return nil, fmt.Errorf("outbox enqueue: subject is required")
	}
	rec := &Record{
		ID:        nuid.Next(),
		Subject:   subject,
		Data:      append([]byte(nil), data...),
		Headers:   headers,
		CreatedAt: time.Now().UTC(),
	}
	raw, err := json.Marshal(rec)
	if err != nil {
		return nil, err
	}
	if err := store.Put(ctx, rec.ID, raw); err != nil {
		return nil, fmt.Errorf("outbox enqueue put: %w", err)
	}

	return rec, nil
}

// Flush publishes pending records and deletes them on success (at-least-once).
func Flush(ctx context.Context, store Store, pub Publisher, limit int) (int, error) {
	if store == nil || pub == nil {
		return 0, fmt.Errorf("outbox flush: store and publisher are required")
	}
	if limit <= 0 {
		limit = 100
	}
	keys, err := store.Keys(ctx)
	if err != nil {
		return 0, err
	}
	flushed := 0
	for _, key := range keys {
		if flushed >= limit {
			break
		}
		if err := ctx.Err(); err != nil {
			return flushed, err
		}
		raw, gerr := store.Get(ctx, key)
		if gerr != nil {
			continue
		}
		var rec Record
		if uerr := json.Unmarshal(raw, &rec); uerr != nil {
			continue
		}
		if err := pub.PublishRaw(ctx, rec.Subject, rec.Data, rec.Headers); err != nil {
			return flushed, fmt.Errorf("outbox flush publish id=%s: %w", rec.ID, err)
		}
		_ = store.Delete(ctx, key)
		flushed++
	}

	return flushed, nil
}
