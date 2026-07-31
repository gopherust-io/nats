package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/gopherust-io/nats/internal/bytesconv"
	natspkg "github.com/nats-io/nats.go"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

// PubAckFuture is a future for an async JetStream publish ack.
type PubAckFuture = natspkg.PubAckFuture

// PubAck is a JetStream publish acknowledgement.
type PubAck = natspkg.PubAck

type Publisher interface {
	PublishMessage(ctx context.Context, subject string, msg Message) error
	PublishProto(ctx context.Context, subject string, payload proto.Message) error
	PublishJSON(ctx context.Context, subject string, data any) error
	PublishMsgPack(ctx context.Context, subject string, data any) error
	PublishBytes(ctx context.Context, subject string, data []byte) error
	PublishBytesWithMsgID(ctx context.Context, subject, id string, data []byte) error
	PublishWithMsgID(ctx context.Context, subject, id string, msg Message) error
	PublishAsync(ctx context.Context, subject string, msg Message) (PubAckFuture, error)
	PublishAsyncBytes(ctx context.Context, subject string, data []byte) (PubAckFuture, error)
	PublishAsyncComplete(ctx context.Context) error
}

// goalign:ignore
type publisher struct {
	js                    natspkg.JetStreamContext
	ctx                   context.Context
	conn                  *natspkg.Conn
	cancel                context.CancelFunc
	metrics               *clientMetrics
	maxAsyncPending       int
	allowMetrics          bool
	allowTracing          bool
	skipSubjectValidation bool
	reconnectBufDisabled  bool
}

func newPublisher(
	ctx context.Context,
	cfg PublisherConfig,
	js natspkg.JetStreamContext,
	conn *natspkg.Conn,
	reconnectBufSize int,
	metrics *clientMetrics,
	allowTracing bool,
) *publisher {
	maxAsync := cfg.MaxAsyncPending
	if maxAsync == 0 {
		maxAsync = defaultMaxAsyncPending
	}

	pub := &publisher{
		js:                    js,
		conn:                  conn,
		metrics:               metrics,
		allowMetrics:          cfg.AllowMetrics,
		allowTracing:          allowTracing && cfg.AllowTracing,
		skipSubjectValidation: cfg.SkipSubjectValidation,
		maxAsyncPending:       maxAsync,
		reconnectBufDisabled:  reconnectBufSize < 0,
	}
	pub.ctx, pub.cancel = context.WithCancel(ctx)

	return pub
}

func (p *publisher) stop() {
	if p.cancel != nil {
		p.cancel()
	}
}

func (p *publisher) guardConnected() error {
	if !p.reconnectBufDisabled {
		return nil
	}

	if p.conn == nil || !p.conn.IsConnected() {
		return ErrNatsConnectionNotEstablished
	}

	return nil
}

func (p *publisher) validateSubject(subject string) error {
	return validateOutboundSubject(subject, "publish", p.skipSubjectValidation)
}

type preparedPublish struct {
	msg  Message
	data []byte
}

func (p *publisher) preparePublish(ctx context.Context, subject string, msg Message) (preparedPublish, trace.Span, error) {
	if err := p.guardConnected(); err != nil {
		return preparedPublish{}, nil, err
	}

	if err := p.validateSubject(subject); err != nil {
		return preparedPublish{}, nil, err
	}

	if !validMessageType(msg.MessageType) {
		return preparedPublish{}, nil, fmt.Errorf("publish subject=%q: %w", subject, ErrInvalidMessageType)
	}

	spanCtx, span := startPublishSpan(ctx, subject, p.allowTracing)

	if p.allowTracing {
		injectTraceContext(spanCtx, &msg)
	}

	data, err := Encode(msg)
	if err != nil {
		endSpan(span, err)
		p.recordError(ctx, subject)

		return preparedPublish{}, span, fmt.Errorf("publish encode subject=%q: %w", subject, err)
	}

	if needsContentTypeHeader(msg) {
		applyContentTypeHeader(&msg)
	}

	return preparedPublish{data: data, msg: msg}, span, nil
}

func (p *publisher) publishMsg(subject string, prep preparedPublish) error {
	pubOpts := publishOptsFromMessage(prep.msg)

	if len(prep.msg.Header) == 0 && len(pubOpts) == 0 {
		_, err := p.js.Publish(subject, prep.data)

		return err
	}

	hdr := natspkg.Header(prep.msg.Header)
	if hdr == nil {
		hdr = make(natspkg.Header)
	}

	if id := hdr.Get(HeaderMsgID); !bytesconv.IsEmpty(id) {
		pubOpts = append(pubOpts, natspkg.MsgId(id))
	}

	_, err := p.js.PublishMsg(&natspkg.Msg{
		Subject: subject,
		Data:    prep.data,
		Header:  hdr,
	}, pubOpts...)

	return err
}

func (p *publisher) publishAsyncMsg(subject string, prep preparedPublish) (PubAckFuture, error) {
	pubOpts := publishOptsFromMessage(prep.msg)

	if len(prep.msg.Header) == 0 && len(pubOpts) == 0 {
		return p.js.PublishAsync(subject, prep.data)
	}

	hdr := natspkg.Header(prep.msg.Header)
	if hdr == nil {
		hdr = make(natspkg.Header)
	}

	if id := hdr.Get(HeaderMsgID); !bytesconv.IsEmpty(id) {
		pubOpts = append(pubOpts, natspkg.MsgId(id))
	}

	return p.js.PublishMsgAsync(&natspkg.Msg{
		Subject: subject,
		Data:    prep.data,
		Header:  hdr,
	}, pubOpts...)
}

func publishOptsFromMessage(msg Message) []natspkg.PubOpt {
	if msg.Expect == nil {
		return nil
	}

	e := msg.Expect
	opts := make([]natspkg.PubOpt, 0, 4)
	if !bytesconv.IsEmpty(e.Stream) {
		opts = append(opts, natspkg.ExpectStream(e.Stream))
	}
	if !bytesconv.IsEmpty(e.LastMsgID) {
		opts = append(opts, natspkg.ExpectLastMsgId(e.LastMsgID))
	}
	if e.LastSeq != nil {
		opts = append(opts, natspkg.ExpectLastSequence(*e.LastSeq))
	}
	if e.LastSeqPerSubject != nil {
		opts = append(opts, natspkg.ExpectLastSequencePerSubject(*e.LastSeqPerSubject))
	}

	return opts
}

func (p *publisher) guardAsyncPending() error {
	if p.maxAsyncPending < 0 {
		return nil
	}

	if p.js.PublishAsyncPending() >= p.maxAsyncPending {
		return ErrAsyncPublishPendingLimit
	}

	return nil
}

func (p *publisher) publish(ctx context.Context, subject string, msg Message) error {
	return p.PublishMessage(ctx, subject, msg)
}

func (p *publisher) PublishMessage(ctx context.Context, subject string, msg Message) error {
	prep, span, err := p.preparePublish(ctx, subject, msg)
	if err != nil {
		return err
	}

	var publishErr error

	defer endSpanPtr(span, &publishErr)

	var start int64
	if p.allowMetrics && p.metrics != nil {
		start = time.Now().UnixNano()
	}

	err = p.publishMsg(subject, prep)
	if err != nil {
		publishErr = err
		p.recordError(ctx, subject)

		return fmt.Errorf("publish subject=%q: %w", subject, err)
	}

	if p.allowMetrics && p.metrics != nil {
		elapsed := time.Duration(time.Now().UnixNano() - start)
		p.recordSuccess(ctx, subject, len(prep.data), elapsed)
	}

	return nil
}

func (p *publisher) publishAsync(ctx context.Context, subject string, msg Message) (PubAckFuture, error) {
	if err := p.guardAsyncPending(); err != nil {
		return nil, err
	}

	prep, span, err := p.preparePublish(ctx, subject, msg)
	if err != nil {
		return nil, err
	}

	future, err := p.publishAsyncMsg(subject, prep)
	if err != nil {
		endSpan(span, err)
		p.recordError(ctx, subject)

		return nil, fmt.Errorf("publish async subject=%q: %w", subject, err)
	}

	if p.allowMetrics && p.metrics != nil {
		p.recordAsyncAccepted(ctx, subject, len(prep.data))
	}

	endSpan(span, nil)

	return future, nil
}

func (p *publisher) PublishProto(ctx context.Context, subject string, payload proto.Message) error {
	return p.publish(ctx, subject, Message{
		Data:        payload,
		MessageType: Proto,
	})
}

func (p *publisher) PublishJSON(ctx context.Context, subject string, data any) error {
	return p.publish(ctx, subject, Message{
		Data:        data,
		MessageType: JSON,
	})
}

func (p *publisher) PublishMsgPack(ctx context.Context, subject string, data any) error {
	return p.publish(ctx, subject, Message{
		Data:        data,
		MessageType: MessagePack,
	})
}

func (p *publisher) PublishBytes(ctx context.Context, subject string, data []byte) error {
	return p.publish(ctx, subject, Message{
		Data:        data,
		MessageType: Raw,
	})
}

func (p *publisher) PublishRaw(ctx context.Context, subject string, data []byte, headers map[string]string) (*PubAck, error) {
	if err := p.guardConnected(); err != nil {
		return nil, err
	}

	if err := p.validateSubject(subject); err != nil {
		return nil, err
	}

	msg := &natspkg.Msg{Subject: subject, Data: data}
	for k, v := range headers {
		if msg.Header == nil {
			msg.Header = natspkg.Header{}
		}

		msg.Header.Set(k, v)
	}

	ack, err := p.js.PublishMsg(msg)
	if err != nil {
		p.recordError(ctx, subject)

		return nil, fmt.Errorf("publish raw subject=%q: %w", subject, err)
	}

	return ack, nil
}

func (p *publisher) PublishBytesWithMsgID(ctx context.Context, subject, id string, data []byte) error {
	return p.publish(ctx, subject, Message{
		Data:        data,
		MessageType: Raw,
	}.WithMsgID(id))
}

func (p *publisher) PublishWithMsgID(ctx context.Context, subject, id string, msg Message) error {
	return p.publish(ctx, subject, msg.WithMsgID(id))
}

func (p *publisher) PublishAsync(ctx context.Context, subject string, msg Message) (PubAckFuture, error) {
	return p.publishAsync(ctx, subject, msg)
}

func (p *publisher) PublishAsyncBytes(ctx context.Context, subject string, data []byte) (PubAckFuture, error) {
	return p.publishAsync(ctx, subject, Message{
		Data:        data,
		MessageType: Raw,
	})
}

func (p *publisher) metricSubject(subject string) string {
	return metricSubjectLabel(p.metrics, subject)
}

func (p *publisher) recordError(ctx context.Context, subject string) {
	if !p.allowMetrics || p.metrics == nil {
		return
	}
	p.metrics.addCounter(ctx, p.metrics.publishErrors, subject)
}

func (p *publisher) recordSuccess(ctx context.Context, subject string, bytes int, elapsed time.Duration) {
	if !p.allowMetrics || p.metrics == nil {
		return
	}
	p.metrics.addCounter(ctx, p.metrics.publishTotal, subject)
	p.metrics.recordBytesLatency(ctx, p.metrics.publishBytes, p.metrics.publishLatency, subject, bytes, elapsed)
}

func (p *publisher) recordAsyncAccepted(ctx context.Context, subject string, bytes int) {
	if !p.allowMetrics || p.metrics == nil {
		return
	}
	p.metrics.addCounter(ctx, p.metrics.publishTotal, subject)
	p.metrics.recordBytesLatency(ctx, p.metrics.publishBytes, nil, subject, bytes, 0)
}

// PublishAsyncComplete waits until all outstanding async publishes have completed
// or ctx is cancelled. Call after a burst of PublishAsync* to drain futures.
func (p *publisher) PublishAsyncComplete(ctx context.Context) error {
	done := p.js.PublishAsyncComplete()
	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-p.ctx.Done():
		return p.ctx.Err()
	}
}
