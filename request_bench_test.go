package nats

import (
	"context"
	"testing"

	natspkg "github.com/nats-io/nats.go"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/gopherust-io/tel"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

// Core request/reply must not use subjects captured by a JetStream stream:
// the server PubAck is delivered to the reply inbox and looks like the response.

func benchmarkRequestClient(b *testing.B, withMetrics bool) (Client, context.Context) {
	b.Helper()
	ctx := context.Background()
	cfg := DefaultConfig()
	cfg.Conn.Address = startTestNATSServer(b)
	if !withMetrics {
		cfg.Metrics.AllowMetrics = false
		cfg.Conn.AllowMetrics = false
		cfg.PublisherConfig.AllowMetrics = false
		cfg.RequesterConfig.AllowMetrics = false
		cfg.ResponderConfig.AllowMetrics = false
		cfg.RuntimeConsumer.AllowMetrics = false
	} else {
		telem := tel.NewWithConfig(tel.DefaultDebugConfig())
		if err := telem.Start(ctx); err != nil {
			b.Fatal(err)
		}
		ctx = tel.WrapContext(ctx, telem)
		cfg.Metrics.AllowMetrics = true
	}
	client, err := NewClient(ctx, &cfg)
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(func() { _ = client.Connector().Shutdown() })
	return client, ctx
}

func setupRequestBench(b *testing.B, withMetrics bool, subject string) (Client, context.Context) {
	b.Helper()
	client, ctx := benchmarkRequestClient(b, withMetrics)
	_, err := client.Responder().Subscribe(ctx, subject, func(_ context.Context, msg *natspkg.Msg) error {
		return RespondBytes(msg, msg.Data)
	})
	if err != nil {
		b.Fatal(err)
	}
	return client, ctx
}

func BenchmarkRequestBytes(b *testing.B) {
	subject := "rr.bench.bytes"
	client, ctx := setupRequestBench(b, false, subject)
	payload := bytesconv.StringToBytes(`ping`)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		reply, err := client.Requester().RequestBytes(ctx, subject, payload)
		if err != nil {
			b.Fatal(err)
		}
		if bytesconv.BytesToString(reply.Data) != "ping" {
			b.Fatalf("got %q", reply.Data)
		}
	}
}

func BenchmarkRequestJSON(b *testing.B) {
	subject := "rr.bench.json"
	client, ctx := setupRequestBench(b, false, subject)
	req := map[string]any{"id": "1"}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var resp map[string]any
		if err := client.Requester().RequestJSONInto(ctx, subject, req, &resp); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRequestMsgPack(b *testing.B) {
	subject := "rr.bench.msgpack"
	client, ctx := setupRequestBench(b, false, subject)
	type payload struct {
		ID string `msgpack:"id"`
	}
	req := payload{ID: "1"}
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var resp payload
		if err := client.Requester().RequestMsgPackInto(ctx, subject, req, &resp); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRequestProto(b *testing.B) {
	subject := "rr.bench.proto"
	client, ctx := setupRequestBench(b, false, subject)
	req := wrapperspb.String("ping")
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		var resp wrapperspb.StringValue
		if err := client.Requester().RequestProtoInto(ctx, subject, req, &resp); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRequestBytesWithMetrics(b *testing.B) {
	subject := "rr.bench.bytes.metrics"
	client, ctx := setupRequestBench(b, true, subject)
	payload := bytesconv.StringToBytes(`ping`)
	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		if _, err := client.Requester().RequestBytes(ctx, subject, payload); err != nil {
			b.Fatal(err)
		}
	}
}
