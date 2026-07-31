package nats

import (
	"context"
	"testing"

	natspkg "github.com/nats-io/nats.go"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/gopherust-io/tel"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

func BenchmarkPublishJSON(b *testing.B) {
	client, ctx := benchmarkClient(b)
	defer func() { _ = client.Connector().Shutdown() }()

	payload := map[string]string{"id": "bench"}
	b.ReportAllocs()

	for b.Loop() {
		_ = client.Publisher().PublishMessage(ctx, "bench.subject", Message{
			Data:        payload,
			MessageType: JSON,
		})
	}
}

func BenchmarkPublishJSONNoMetrics(b *testing.B) {
	client, ctx := benchmarkClientNoMetrics(b)
	defer func() { _ = client.Connector().Shutdown() }()

	payload := map[string]string{"id": "bench"}
	b.ReportAllocs()

	for b.Loop() {
		_ = client.Publisher().PublishMessage(ctx, "bench.subject", Message{
			Data:        payload,
			MessageType: JSON,
		})
	}
}

func BenchmarkProcessMessageMetrics(b *testing.B) {
	metrics := newClientMetrics(context.Background(), MetricsConfig{
		AllowMetrics: true,
		Prefix:       "bench",
	})
	metrics.registry.AttrCache().SubjectOpts("bench.subject")

	c := &consumer{metrics: metrics}
	msg := &natspkg.Msg{Subject: "bench.subject", Data: bytesconv.StringToBytes(`{"id":"1"}`)}
	handler := func(context.Context, *natspkg.Msg) error { return nil }

	b.ReportAllocs()

	for b.Loop() {
		_ = c.recordMessageMetrics(context.Background(), msg, nil)
		_ = handler(context.Background(), msg)
	}
}

func BenchmarkAttrCacheSubjectOpts(b *testing.B) {
	telem := tel.NewWithConfig(tel.DefaultDebugConfig())
	cache := telem.Registry().AttrCache()
	cache.SubjectOpts("orders.created")

	b.ReportAllocs()

	for b.Loop() {
		_ = cache.SubjectOpts("orders.created")
	}
}

func benchmarkClient(b *testing.B) (Client, context.Context) {
	b.Helper()
	ctx := context.Background()
	telem := tel.NewWithConfig(tel.DefaultDebugConfig())
	if err := telem.Start(ctx); err != nil {
		b.Fatal(err)
	}
	ctx = tel.WrapContext(ctx, telem)

	cfg := DefaultConfig()
	cfg.Conn.Address = startTestNATSServer(b)
	cfg.Metrics.AllowMetrics = true
	client, err := NewClient(ctx, &cfg)
	if err != nil {
		b.Fatal(err)
	}
	_, err = client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name:     "BENCH",
		Subjects: []string{"bench.>"},
	})
	if err != nil {
		b.Fatal(err)
	}
	return client, ctx
}

func benchmarkClientNoMetrics(b *testing.B) (Client, context.Context) {
	b.Helper()
	ctx := context.Background()
	cfg := DefaultConfig()
	cfg.Conn.Address = startTestNATSServer(b)
	cfg.Metrics.AllowMetrics = false
	cfg.Conn.AllowMetrics = false
	cfg.PublisherConfig.AllowMetrics = false
	cfg.RuntimeConsumer.AllowMetrics = false
	client, err := NewClient(ctx, &cfg)
	if err != nil {
		b.Fatal(err)
	}
	_, err = client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name:     "BENCH",
		Subjects: []string{"bench.>"},
	})
	if err != nil {
		b.Fatal(err)
	}
	return client, ctx
}

func BenchmarkEncodeJSON(b *testing.B) {
	msg := Message{Data: map[string]string{"id": "bench"}, MessageType: JSON}
	b.ReportAllocs()

	for b.Loop() {
		_, _ = Encode(msg)
	}
}

func BenchmarkEncodeProto(b *testing.B) {
	msg := Message{Data: wrapperspb.String("bench"), MessageType: Proto}
	b.ReportAllocs()

	for b.Loop() {
		_, _ = Encode(msg)
	}
}

func BenchmarkEncodeMessagePack(b *testing.B) {
	msg := Message{Data: map[string]int{"count": 1}, MessageType: MessagePack}
	b.ReportAllocs()

	for b.Loop() {
		_, _ = Encode(msg)
	}
}

func BenchmarkDecodeJSON(b *testing.B) {
	data, err := Encode(Message{Data: map[string]string{"id": "bench"}, MessageType: JSON})
	if err != nil {
		b.Fatal(err)
	}
	var dst map[string]string
	b.ReportAllocs()

	for b.Loop() {
		_ = Decode(data, JSON, &dst)
	}
}

func BenchmarkMessageTypeFromHeader(b *testing.B) {
	h := natspkg.Header{HeaderContentType: []string{ContentTypeJSON}}
	b.ReportAllocs()

	for b.Loop() {
		_ = MessageTypeFromHeader(h)
	}
}

func BenchmarkNeedsContentTypeHeader(b *testing.B) {
	msg := Message{Data: map[string]string{"id": "1"}, MessageType: JSON}
	b.ReportAllocs()

	for b.Loop() {
		_ = needsContentTypeHeader(msg)
	}
}

func BenchmarkValidMessageType(b *testing.B) {
	b.ReportAllocs()

	for b.Loop() {
		_ = validMessageType(JSON)
	}
}

func BenchmarkPublishProto(b *testing.B) {
	client, ctx := benchmarkClientNoMetrics(b)
	defer func() { _ = client.Connector().Shutdown() }()

	payload := wrapperspb.String("bench")
	b.ReportAllocs()

	for b.Loop() {
		_ = client.Publisher().PublishProto(ctx, "bench.proto", payload)
	}
}

func BenchmarkRecordMessageMetricsNoRedelivery(b *testing.B) {
	metrics := newClientMetrics(context.Background(), MetricsConfig{
		AllowMetrics: true,
		Prefix:       "bench",
	})
	metrics.registry.AttrCache().SubjectOpts("bench.subject")

	c := &consumer{metrics: metrics}
	msg := &natspkg.Msg{Subject: "bench.subject", Data: bytesconv.StringToBytes(`{"id":"1"}`)}

	b.ReportAllocs()

	for b.Loop() {
		_ = c.recordMessageMetrics(context.Background(), msg, nil)
	}
}

func BenchmarkAttrCacheSubjectRecordOpts(b *testing.B) {
	telem := tel.NewWithConfig(tel.DefaultDebugConfig())
	cache := telem.Registry().AttrCache()
	cache.SubjectRecordOpts("orders.created")

	b.ReportAllocs()

	for b.Loop() {
		_ = cache.SubjectRecordOpts("orders.created")
	}
}

func BenchmarkPublishWithMsgID(b *testing.B) {
	client, ctx := benchmarkClientNoMetrics(b)
	defer func() { _ = client.Connector().Shutdown() }()

	b.ReportAllocs()

	for b.Loop() {
		_ = client.Publisher().PublishWithMsgID(ctx, "bench.subject", "msg-id", Message{
			Data: map[string]string{"id": "bench"}, MessageType: JSON,
		})
	}
}

func BenchmarkPublishBytes(b *testing.B) {
	client, ctx := benchmarkClientNoMetrics(b)
	defer func() { _ = client.Connector().Shutdown() }()

	payload := bytesconv.StringToBytes(`{"id":"bench"}`)
	b.ReportAllocs()

	for b.Loop() {
		_ = client.Publisher().PublishBytes(ctx, "bench.subject", payload)
	}
}

func BenchmarkPublishAsyncBytes(b *testing.B) {
	client, ctx := benchmarkClientNoMetrics(b)
	defer func() { _ = client.Connector().Shutdown() }()

	payload := bytesconv.StringToBytes(`{"id":"bench"}`)
	b.ReportAllocs()

	for b.Loop() {
		future, err := client.Publisher().PublishAsyncBytes(ctx, "bench.subject", payload)
		if err != nil {
			b.Fatal(err)
		}
		// Wait for the ack so in-flight pending stays below MaxAsyncPending (default 1024).
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
}

func BenchmarkPublishJSONTracingNotRecording(b *testing.B) {
	client, ctx := benchmarkClient(b)
	defer func() { _ = client.Connector().Shutdown() }()

	payload := map[string]string{"id": "bench"}
	b.ReportAllocs()

	for b.Loop() {
		_ = client.Publisher().PublishJSON(ctx, "bench.subject", payload)
	}
}

func BenchmarkHandlerTyped(b *testing.B) {
	type payload struct {
		ID string `json:"id"`
	}
	data, err := Encode(Message{Data: payload{ID: "1"}, MessageType: JSON})
	if err != nil {
		b.Fatal(err)
	}
	msg := &natspkg.Msg{
		Subject: "bench.subject",
		Data:    data,
		Header:  natspkg.Header{HeaderContentType: []string{ContentTypeJSON}},
	}
	handler := HandlerTyped[payload](JSON, func(_ context.Context, _ string, _ payload) error { return nil })

	b.ReportAllocs()

	for b.Loop() {
		_ = handler(context.Background(), msg)
	}
}
