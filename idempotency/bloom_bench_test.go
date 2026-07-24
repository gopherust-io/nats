package idempotency

import (
	"context"
	"testing"
)

func BenchmarkBloomSeen(b *testing.B) {
	store := NewBloomStore(1<<16, 7)
	ctx := context.Background()
	_ = store.Mark(ctx, "bench-id")

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Seen(ctx, "bench-id")
	}
}

func BenchmarkBloomMark(b *testing.B) {
	store := NewBloomStore(1<<16, 7)
	ctx := context.Background()

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = store.Mark(ctx, "bench-id")
	}
}
