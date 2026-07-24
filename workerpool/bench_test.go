package workerpool

import (
	"context"
	"testing"

	"github.com/nats-io/nats.go"
)

func BenchmarkWorkerPool_Publish(b *testing.B) {
	ctx := context.Background()
	pool := New(ctx, 4, 128, func(_ context.Context, _ *nats.Msg) error {
		return nil
	})
	pool.Consume()
	defer pool.GracefulStop()

	msg := &nats.Msg{Subject: "bench"}

	b.ReportAllocs()

	for b.Loop() {
		pool.Publish(ctx, msg, false, nil)
	}
}

func BenchmarkWorkerPool_PublishParallel(b *testing.B) {
	ctx := context.Background()
	pool := New(ctx, 8, 256, func(_ context.Context, _ *nats.Msg) error {
		return nil
	})
	pool.Consume()
	defer pool.GracefulStop()

	msg := &nats.Msg{Subject: "bench"}

	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			pool.Publish(ctx, msg, false, nil)
		}
	})
}

func BenchmarkWorkerPool_TryPublish(b *testing.B) {
	ctx := context.Background()
	pool := New(ctx, 4, 128, func(_ context.Context, _ *nats.Msg) error {
		return nil
	})
	pool.Consume()
	defer pool.GracefulStop()

	msg := &nats.Msg{Subject: "bench"}

	b.ReportAllocs()

	for b.Loop() {
		_ = pool.TryPublish(ctx, msg, false, nil)
	}
}

func BenchmarkWorkerPool_PublishApplyFn(b *testing.B) {
	ctx := context.Background()
	pool := New(ctx, 4, 128, func(_ context.Context, _ *nats.Msg) error {
		return nil
	})
	pool.Consume()
	defer pool.GracefulStop()

	msg := &nats.Msg{Subject: "bench"}
	customFn := func(_ context.Context, _ *nats.Msg) error { return nil }

	b.ReportAllocs()

	for b.Loop() {
		pool.Publish(ctx, msg, true, customFn)
	}
}

func BenchmarkWorkerPool_QueueDepth(b *testing.B) {
	ctx := context.Background()
	pool := New(ctx, 4, 128, func(_ context.Context, _ *nats.Msg) error {
		return nil
	})
	pool.Consume()
	defer pool.GracefulStop()

	b.ReportAllocs()

	for b.Loop() {
		_ = pool.QueueDepth()
	}
}
