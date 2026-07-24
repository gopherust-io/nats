package nats

import (
	"context"
	"sync"

	natspkg "github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"

	"github.com/gopherust-io/tel"
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
		return ctx, trace.SpanFromContext(ctx)
	}

	telem := tel.FromCtx(ctx)

	return telem.StartSpan(ctx, "nats.publish",
		trace.WithAttributes(
			tel.MessagingSystem(), tel.MessagingOperationPublish(), tel.MessagingSubject(subject),
		),
	)
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

		headers[HeaderTraceID] = []string{sc.TraceID().String()}
	}

	msg.Header = headers
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
	if msg == nil || msg.Reply == "" {
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
		return ctx, trace.SpanFromContext(ctx)
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

	attrs := []attribute.KeyValue{
		tel.MessagingSystem(), tel.MessagingOperationProcess(), tel.MessagingSubject(subject),
	}
	if meta != nil {
		attrs = append(attrs,
			attribute.Int64("messaging.nats.stream_sequence", int64(meta.Sequence.Stream)),
			attribute.Int64("messaging.nats.delivery_count", int64(meta.NumDelivered)),
		)
	}

	return telem.StartSpan(ctx, "nats.process", trace.WithAttributes(attrs...))
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
