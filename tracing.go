package nats

import (
	"context"
	"encoding/hex"
	"sync"

	natspkg "github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	tracenoop "go.opentelemetry.io/otel/trace/noop"

	"github.com/gopherust-io/tel"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

var headerCarrierPool = sync.Pool{
	New: func() any {
		m := make(map[string][]string, 8)

		return &m
	},
}

func acquireHeaderCarrier() map[string][]string {
	p := headerCarrierPool.Get().(*map[string][]string)
	m := *p
	for k := range m {
		delete(m, k)
	}

	return m
}

func releaseHeaderCarrier(m map[string][]string) {
	if m == nil {
		return
	}
	headerCarrierPool.Put(&m)
}

func startPublishSpan(ctx context.Context, subject string, allowTracing bool) (context.Context, trace.Span) {
	if !allowTracing {
		// Never return a parent span — endSpan would end the caller's span.
		return ctx, tracenoop.Span{}
	}

	telem := tel.FromCtx(ctx)

	// Stack-backed attrs avoid a heap []attribute.KeyValue per publish.
	attrs := [...]attribute.KeyValue{
		tel.MessagingSystem(),
		tel.MessagingOperationPublish(),
		tel.MessagingSubject(subject),
	}

	return telem.StartSpan(ctx, "nats.publish", trace.WithAttributes(attrs[:]...))
}

func startRequestSpan(ctx context.Context, subject string, allowTracing bool) (context.Context, trace.Span) {
	if !allowTracing {
		return ctx, tracenoop.Span{}
	}

	telem := tel.FromCtx(ctx)
	attrs := [...]attribute.KeyValue{
		tel.MessagingSystem(),
		attribute.String("messaging.operation", "request"),
		tel.MessagingSubject(subject),
	}

	return telem.StartSpan(ctx, "nats.request", trace.WithAttributes(attrs[:]...))
}

func startReplySpan(ctx context.Context, msg *natspkg.Msg, allowTracing bool) (context.Context, trace.Span) {
	if !allowTracing {
		return ctx, tracenoop.Span{}
	}

	carrier := acquireHeaderCarrier()
	if msg != nil && msg.Header != nil {
		for k, v := range msg.Header {
			carrier[k] = v
		}
	}
	ctx = tel.ExtractContext(ctx, carrier)
	releaseHeaderCarrier(carrier)

	telem := tel.FromCtx(ctx)
	subject := empty
	if msg != nil {
		subject = msg.Subject
	}
	attrs := [...]attribute.KeyValue{
		tel.MessagingSystem(),
		attribute.String("messaging.operation", "reply"),
		tel.MessagingSubject(subject),
	}

	return telem.StartSpan(ctx, "nats.reply", trace.WithAttributes(attrs[:]...))
}

func injectTraceContext(ctx context.Context, msg *Message) {
	span := trace.SpanFromContext(ctx)
	if !span.IsRecording() {
		return
	}

	headers := tel.InjectContext(ctx, msg.Header)

	sc := span.SpanContext()
	if sc.HasTraceID() {
		if headers == nil {
			headers = make(map[string][]string, 1)
		}
		setHeaderValue(headers, HeaderTraceID, traceIDString(sc.TraceID()))
	}

	msg.Header = headers
}

// traceIDString hex-encodes id into a heap buffer and returns a string sharing
// that buffer (one alloc; no extra string copy from []byte).
func traceIDString(id trace.TraceID) string {
	buf := make([]byte, hex.EncodedLen(len(id)))
	hex.Encode(buf, id[:])

	return bytesconv.BytesToString(buf)
}

func setHeaderValue(headers map[string][]string, key, value string) {
	if vals := headers[key]; len(vals) == 1 {
		vals[0] = value

		return
	}
	if vals := headers[key]; cap(vals) >= 1 {
		headers[key] = append(vals[:0], value)

		return
	}

	headers[key] = []string{value}
}

// TraceIDFromHeader returns the explicit Trace-Id header value, if present.
func TraceIDFromHeader(h natspkg.Header) string {
	if h == nil {
		return empty
	}

	return h.Get(HeaderTraceID)
}

// jetStreamMetadata returns parsed JetStream ack metadata, or nil when unavailable.
// Skips parsing when Reply is empty (non-JS / test messages) to avoid Metadata allocs.
func jetStreamMetadata(msg *natspkg.Msg) *natspkg.MsgMetadata {
	if msg == nil || bytesconv.IsEmpty(msg.Reply) {
		return nil
	}

	meta, err := msg.Metadata()
	if err != nil || meta == nil {
		return nil
	}

	return meta
}

func startProcessSpan(ctx context.Context, msg *natspkg.Msg, allowTracing bool, meta *natspkg.MsgMetadata) (context.Context, trace.Span) {
	if !allowTracing {
		return ctx, tracenoop.Span{}
	}

	carrier := acquireHeaderCarrier()
	if msg != nil && msg.Header != nil {
		for k, v := range msg.Header {
			carrier[k] = v
		}
	}

	ctx = tel.ExtractContext(ctx, carrier)
	releaseHeaderCarrier(carrier)

	telem := tel.FromCtx(ctx)

	subject := empty
	if msg != nil {
		subject = msg.Subject
	}

	// Stack buffer sized for base attrs + optional JetStream metadata.
	var attrs [5]attribute.KeyValue
	attrs[0] = tel.MessagingSystem()
	attrs[1] = tel.MessagingOperationProcess()
	attrs[2] = tel.MessagingSubject(subject)
	n := 3
	if meta != nil {
		attrs[3] = attribute.Int64("messaging.nats.stream_sequence", int64(meta.Sequence.Stream))
		attrs[4] = attribute.Int64("messaging.nats.delivery_count", int64(meta.NumDelivered))
		n = 5
	}

	return telem.StartSpan(ctx, "nats.process", trace.WithAttributes(attrs[:n]...))
}

func endSpan(span trace.Span, err error) {
	if span != nil && span.IsRecording() {
		tel.EndSpan(span, err)
	}
}

// endSpanPtr ends the span using the final error pointed at by errp.
// Safe for defer without allocating a closure: defer endSpanPtr(span, &err).
func endSpanPtr(span trace.Span, errp *error) {
	var err error
	if errp != nil {
		err = *errp
	}
	endSpan(span, err)
}
