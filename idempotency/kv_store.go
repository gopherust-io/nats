package idempotency

import (
	"context"
	"errors"
	"fmt"

	natspkg "github.com/nats-io/nats.go"

	libnats "github.com/gopherust-io/nats"
)

var markedValue = []byte{1}

type kvStore struct {
	kv natspkg.KeyValue
}

// NewKVStore returns a DedupStore (and ClaimStore) backed by a JetStream KV bucket.
// Prefer a bucket with TTL (and optionally MaxBytes) so keys expire and
// storage cannot grow without bound.
//
// Message IDs must be valid NATS KV keys (see libnats.ValidateKVKey).
// WithHandler uses Claim/Release for this store (claim-before-process).
func NewKVStore(kv natspkg.KeyValue) DedupStore {
	return &kvStore{kv: kv}
}

func (s *kvStore) Seen(_ context.Context, id string) (bool, error) {
	if err := libnats.ValidateKVKey(id); err != nil {
		return false, err
	}

	_, err := s.kv.Get(id)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, natspkg.ErrKeyNotFound) {
		return false, nil
	}

	return false, fmt.Errorf("kv dedup seen %q: %w", id, err)
}

func (s *kvStore) Mark(_ context.Context, id string) error {
	if err := libnats.ValidateKVKey(id); err != nil {
		return err
	}

	_, err := s.kv.Create(id, markedValue)
	if err == nil || errors.Is(err, natspkg.ErrKeyExists) {
		return nil
	}

	return fmt.Errorf("kv dedup mark %q: %w", id, err)
}

func (s *kvStore) Claim(_ context.Context, id string) (bool, error) {
	if err := libnats.ValidateKVKey(id); err != nil {
		return false, err
	}

	_, err := s.kv.Create(id, markedValue)
	if err == nil {
		return true, nil
	}

	if errors.Is(err, natspkg.ErrKeyExists) {
		return false, nil
	}

	return false, fmt.Errorf("kv dedup claim %q: %w", id, err)
}

func (s *kvStore) Release(_ context.Context, id string) error {
	if err := libnats.ValidateKVKey(id); err != nil {
		return err
	}

	if err := s.kv.Purge(id); err != nil && !errors.Is(err, natspkg.ErrKeyNotFound) {
		return fmt.Errorf("kv dedup release %q: %w", id, err)
	}

	return nil
}

var _ ClaimStore = (*kvStore)(nil)
