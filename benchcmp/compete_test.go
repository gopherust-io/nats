// Package benchcmp compares gopherust-io/nats thin-wrapper overhead against
// equivalent legacy nats.go JetStreamContext calls on the same embedded broker.
package benchcmp_test

import (
	"context"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natspkg "github.com/nats-io/nats.go"

	libnats "github.com/gopherust-io/nats"
)

var sharedURL string

// maxThroughputConfig mirrors the documented max-QPS recipe on DefaultConfig.
func maxThroughputConfig() libnats.Config {
	cfg := libnats.DefaultConfig()
	cfg.RuntimeConsumer.WorkerPoolEnabled = true
	cfg.RuntimeConsumer.WorkerPoolSize = 8
	cfg.RuntimeConsumer.WorkerBufferSize = 256
	cfg.RuntimeConsumer.AckWait = 45 * time.Second
	cfg.RuntimeConsumer.PendingMsgLimit = 1000
	cfg.RuntimeConsumer.PendingMsgBuffer = 10 << 20
	cfg.Backpressure.Mode = libnats.BackpressureNak
	cfg.Backpressure.MaxAckPending = 1000
	cfg.PublisherConfig.AllowMetrics = false
	cfg.PublisherConfig.AllowTracing = false
	cfg.PublisherConfig.SkipSubjectValidation = true
	cfg.RequesterConfig.AllowMetrics = false
	cfg.RequesterConfig.AllowTracing = false
	cfg.RequesterConfig.SkipSubjectValidation = true
	cfg.ResponderConfig.AllowMetrics = false
	cfg.ResponderConfig.AllowTracing = false
	cfg.RuntimeConsumer.AllowMetrics = false
	cfg.RuntimeConsumer.AllowTracing = false
	cfg.Conn.AllowMetrics = false
	cfg.Metrics.AllowMetrics = false
	cfg.Metrics.AllowTracing = false
	cfg.Metrics.CollectInterval = 60 * time.Second
	cfg.Metrics.Lite = true
	cfg.Metrics.FixedCardinality = true
	cfg.Metrics.TrackedStreams = nil
	cfg.Metrics.TrackedConsumers = nil
	return cfg
}

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "nats-benchcmp-*")
	if err != nil {
		panic(err)
	}
	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  dir,
		NoLog:     true,
		NoSigs:    true,
	}
	s, err := server.NewServer(opts)
	if err != nil {
		_ = os.RemoveAll(dir)
		panic(err)
	}
	go s.Start()
	if !s.ReadyForConnections(2 * time.Second) {
		s.Shutdown()
		_ = os.RemoveAll(dir)
		panic("nats server not ready")
	}
	sharedURL = s.ClientURL()
	code := m.Run()
	s.Shutdown()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func ensureStream(tb testing.TB, js natspkg.JetStreamContext, name, subject string) {
	tb.Helper()
	_, err := js.AddStream(&natspkg.StreamConfig{
		Name:      name,
		Subjects:  []string{subject},
		Storage:   natspkg.MemoryStorage,
		Retention: natspkg.LimitsPolicy,
	})
	if err != nil {
		tb.Fatal(err)
	}
}

func gopherustClient(tb testing.TB, stream, subject string) (libnats.Client, context.Context) {
	tb.Helper()
	ctx := context.Background()
	cfg := maxThroughputConfig()
	cfg.Conn.Address = sharedURL
	// Direct handler path — fairer vs raw nats.go QueueSubscribe (no pool).
	cfg.RuntimeConsumer.WorkerPoolEnabled = false
	client, err := libnats.NewClient(ctx, &cfg)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(func() { _ = client.Connector().Shutdown() })
	_, err = client.Streams().CreateOrUpdateStream(ctx, libnats.StreamConfig{
		Name:      stream,
		Subjects:  []string{subject},
		Storage:   libnats.MemoryStorage,
		Retention: libnats.LimitsPolicy,
	})
	if err != nil {
		tb.Fatal(err)
	}
	return client, ctx
}

func natsGoJS(tb testing.TB, stream, subject string) (natspkg.JetStreamContext, *natspkg.Conn) {
	tb.Helper()
	nc, err := natspkg.Connect(sharedURL)
	if err != nil {
		tb.Fatal(err)
	}
	tb.Cleanup(nc.Close)
	js, err := nc.JetStream()
	if err != nil {
		tb.Fatal(err)
	}
	ensureStream(tb, js, stream, subject)
	return js, nc
}

func BenchmarkCmp_PublishSync(b *testing.B) {
	payload := []byte(`{"id":"bench"}`)

	b.Run("gopherust_PublishBytes", func(b *testing.B) {
		client, ctx := gopherustClient(b, "CMP_PUB", "cmp.pub.>")
		b.ReportAllocs()
		for b.Loop() {
			if err := client.Publisher().PublishBytes(ctx, "cmp.pub.sync", payload); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("natsgo_Publish", func(b *testing.B) {
		js, _ := natsGoJS(b, "CMP_PUB_NG", "cmp.pubng.>")
		b.ReportAllocs()
		for b.Loop() {
			if _, err := js.Publish("cmp.pubng.sync", payload); err != nil {
				b.Fatal(err)
			}
		}
	})
}

func BenchmarkCmp_PublishAsync(b *testing.B) {
	payload := []byte(`{"id":"bench"}`)

	b.Run("gopherust_PublishAsyncBytes", func(b *testing.B) {
		client, ctx := gopherustClient(b, "CMP_ASYNC", "cmp.async.>")
		b.ReportAllocs()
		for b.Loop() {
			future, err := client.Publisher().PublishAsyncBytes(ctx, "cmp.async.bytes", payload)
			if err != nil {
				b.Fatal(err)
			}
			select {
			case <-future.Ok():
			case err := <-future.Err():
				if err != nil {
					b.Fatal(err)
				}
			case <-ctx.Done():
				b.Fatal(ctx.Err())
			}
		}
	})

	b.Run("natsgo_PublishAsync", func(b *testing.B) {
		js, _ := natsGoJS(b, "CMP_ASYNC_NG", "cmp.asyncng.>")
		b.ReportAllocs()
		for b.Loop() {
			future, err := js.PublishAsync("cmp.asyncng.bytes", payload)
			if err != nil {
				b.Fatal(err)
			}
			select {
			case <-future.Ok():
			case err := <-future.Err():
				if err != nil {
					b.Fatal(err)
				}
			}
		}
	})
}

func BenchmarkCmp_PushConsumeAck(b *testing.B) {
	payload := []byte(`{"id":"bench"}`)

	b.Run("gopherust_QueueSubscribeBound", func(b *testing.B) {
		client, ctx := gopherustClient(b, "CMP_PUSH", "cmp.push.>")
		var wg sync.WaitGroup
		handler := func(_ context.Context, msg *natspkg.Msg) error {
			wg.Done()
			return nil // Ack
		}
		// QueueSubscribeBound creates a push durable via BindStream + Durable.
		if _, err := client.Consumer().QueueSubscribeBound(ctx, "CMP_PUSH", "cmp-push", "cmp-q", "cmp.push.>", handler); err != nil {
			b.Fatal(err)
		}
		time.Sleep(50 * time.Millisecond)
		b.ReportAllocs()
		for b.Loop() {
			wg.Add(1)
			if err := client.Publisher().PublishBytes(ctx, "cmp.push.msg", payload); err != nil {
				b.Fatal(err)
			}
			waitWG(b, &wg)
		}
	})

	b.Run("natsgo_QueueSubscribe", func(b *testing.B) {
		js, _ := natsGoJS(b, "CMP_PUSH_NG", "cmp.pushng.>")
		var wg sync.WaitGroup
		_, err := js.QueueSubscribe("cmp.pushng.>", "cmp-q", func(msg *natspkg.Msg) {
			_ = msg.Ack()
			wg.Done()
		}, natspkg.Durable("cmp-push-ng"), natspkg.BindStream("CMP_PUSH_NG"), natspkg.ManualAck())
		if err != nil {
			b.Fatal(err)
		}
		// Let push interest settle before timing.
		time.Sleep(50 * time.Millisecond)
		b.ReportAllocs()
		for b.Loop() {
			wg.Add(1)
			if _, err := js.Publish("cmp.pushng.msg", payload); err != nil {
				b.Fatal(err)
			}
			waitWG(b, &wg)
		}
	})
}

func waitWG(b *testing.B, wg *sync.WaitGroup) {
	b.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		b.Fatal("timeout waiting for consume+ack")
	}
}

func BenchmarkCmp_PullFetchAck(b *testing.B) {
	payload := []byte(`{"id":"bench"}`)

	b.Run("gopherust_PullFetch", func(b *testing.B) {
		client, ctx := gopherustClient(b, "CMP_PULL", "cmp.pull.>")
		if _, err := client.Consumers().CreateOrUpdateConsumer(ctx, "CMP_PULL", libnats.DurableConsumerConfig{
			Durable:       "cmp-pull",
			FilterSubject: "cmp.pull.>",
			AckPolicy:     libnats.AckExplicit,
			HasAckPolicy:  true,
		}); err != nil {
			b.Fatal(err)
		}
		pull, err := client.Consumer().Pull("CMP_PULL", "cmp-pull")
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = pull.Close() })
		b.ReportAllocs()
		for b.Loop() {
			if err := client.Publisher().PublishBytes(ctx, "cmp.pull.msg", payload); err != nil {
				b.Fatal(err)
			}
			msgs, err := pull.Fetch(ctx, 1, libnats.WithFetchMaxWait(2*time.Second))
			if err != nil {
				b.Fatal(err)
			}
			if len(msgs) != 1 {
				b.Fatalf("want 1 msg, got %d", len(msgs))
			}
			if err := msgs[0].Ack(); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("natsgo_PullSubscribe", func(b *testing.B) {
		js, _ := natsGoJS(b, "CMP_PULL_NG", "cmp.pullng.>")
		sub, err := js.PullSubscribe("cmp.pullng.>", "cmp-pull-ng", natspkg.BindStream("CMP_PULL_NG"))
		if err != nil {
			b.Fatal(err)
		}
		b.Cleanup(func() { _ = sub.Unsubscribe() })
		b.ReportAllocs()
		for b.Loop() {
			if _, err := js.Publish("cmp.pullng.msg", payload); err != nil {
				b.Fatal(err)
			}
			msgs, err := sub.Fetch(1, natspkg.MaxWait(2*time.Second))
			if err != nil {
				b.Fatal(err)
			}
			if len(msgs) != 1 {
				b.Fatalf("want 1 msg, got %d", len(msgs))
			}
			if err := msgs[0].Ack(); err != nil {
				b.Fatal(err)
			}
		}
	})
}
