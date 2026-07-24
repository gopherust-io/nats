package idempotency

import (
	"context"
	"hash/fnv"
	"sync/atomic"
)

const (
	defaultBloomBits   = 1 << 20 // 1 Mib bits ≈ 128 KiB
	defaultBloomHashes = 7
)

// BloomStore is an in-memory probabilistic Seen filter with optional async Mark to a backend.
// False positives skip work (safe for at-least-once with idempotent handlers);
// false negatives only occur before Mark completes when using a backend.
//
// The bitset is lock-free: bits only transition 0→1 via atomic OR, which is safe
// for Bloom semantics under concurrent Seen/Mark.
type BloomStore struct {
	backend DedupStore // optional; Mark also updates backend when set
	bits    []uint64
	hashes  int
}

// NewBloomStore creates a Bloom filter. bits must be > 0 (rounded up to 64); hashes defaults to 7.
func NewBloomStore(bits, hashes int) *BloomStore {
	if bits <= 0 {
		bits = defaultBloomBits
	}
	if hashes <= 0 {
		hashes = defaultBloomHashes
	}
	nWords := (bits + 63) / 64

	return &BloomStore{
		bits:   make([]uint64, nWords),
		hashes: hashes,
	}
}

// WithBackend returns the store configured to forward Mark/Seen to backend after the bloom check.
func (b *BloomStore) WithBackend(backend DedupStore) *BloomStore {
	b.backend = backend

	return b
}

func (b *BloomStore) Seen(_ context.Context, id string) (bool, error) {
	return b.test(id), nil
}

func (b *BloomStore) Mark(ctx context.Context, id string) error {
	b.set(id)

	if b.backend != nil {
		return b.backend.Mark(ctx, id)
	}

	return nil
}

func (b *BloomStore) bitLen() uint64 {
	return uint64(len(b.bits) * 64)
}

func (b *BloomStore) test(id string) bool {
	n := b.bitLen()
	for i := 0; i < b.hashes; i++ {
		h := bloomHash(id, i) % n
		word, bit := h/64, h%64
		if atomic.LoadUint64(&b.bits[word])&(1<<bit) == 0 {
			return false
		}
	}

	return true
}

func (b *BloomStore) set(id string) {
	n := b.bitLen()
	for i := 0; i < b.hashes; i++ {
		h := bloomHash(id, i) % n
		word, bit := h/64, h%64
		atomic.OrUint64(&b.bits[word], 1<<bit)
	}
}

func bloomHash(id string, salt int) uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(id))
	_, _ = h.Write([]byte{byte(salt), byte(salt >> 8)})

	return h.Sum64()
}
