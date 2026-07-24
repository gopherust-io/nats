package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// Requester issues core NATS request/reply calls (not JetStream).
type Requester interface {
	RequestBytes(ctx context.Context, subject string, data []byte) (*natspkg.Msg, error)
	RequestMessage(ctx context.Context, subject string, msg Message) (*natspkg.Msg, error)

	RequestJSON(ctx context.Context, subject string, req any) (*natspkg.Msg, error)
	RequestJSONInto(ctx context.Context, subject string, req, resp any) error

	RequestMsgPack(ctx context.Context, subject string, req any) (*natspkg.Msg, error)
	RequestMsgPackInto(ctx context.Context, subject string, req, resp any) error

	RequestProto(ctx context.Context, subject string, req proto.Message) (*natspkg.Msg, error)
	RequestProtoInto(ctx context.Context, subject string, req, resp proto.Message) error
}

type requester struct {
	conn                  *natspkg.Conn
	metrics               *clientMetrics
	timeout               time.Duration
	allowMetrics          bool
	allowTracing          bool
	skipSubjectValidation bool
}

func newRequester(cfg RequesterConfig, conn *natspkg.Conn, metrics *clientMetrics, allowTracing bool) *requester {
	timeout := cfg.Timeout
	if timeout <= 0 {
		timeout = defaultRequestTimeout
	}

	return &requester{
		conn:                  conn,
		metrics:               metrics,
		timeout:               timeout,
		allowMetrics:          cfg.AllowMetrics,
		allowTracing:          allowTracing && cfg.AllowTracing,
		skipSubjectValidation: cfg.SkipSubjectValidation,
	}
}

func (r *requester) validateSubject(subject string) error {
	if r.skipSubjectValidation {
		return nil
	}

	if err := ValidatePublishSubject(subject); err != nil {
		if errors.Is(err, ErrInvalidSubject) && subject == empty {
			return fmt.Errorf("request subject=%q: %w", subject, ErrEmptySubjectNotAllowed)
		}

		return fmt.Errorf("request subject=%q: %w", subject, err)
	}

	return nil
}

func (r *requester) withTimeout(ctx context.Context) (context.Context, context.CancelFunc) {
	if _, ok := ctx.Deadline(); ok {
		return ctx, func() {}
	}

	return context.WithTimeout(ctx, r.timeout)
}

func (r *requester) metricSubject(subject string) string {
	if r.metrics != nil && r.metrics.fixedCardinality {
		return empty
	}

	return subject
}

func (r *requester) RequestBytes(ctx context.Context, subject string, data []byte) (*natspkg.Msg, error) {
	return r.RequestMessage(ctx, subject, Message{Data: data, MessageType: Raw})
}

func (r *requester) RequestJSON(ctx context.Context, subject string, req any) (*natspkg.Msg, error) {
	return r.RequestMessage(ctx, subject, Message{Data: req, MessageType: JSON})
}

func (r *requester) RequestMsgPack(ctx context.Context, subject string, req any) (*natspkg.Msg, error) {
	return r.RequestMessage(ctx, subject, Message{Data: req, MessageType: MessagePack})
}

func (r *requester) RequestProto(ctx context.Context, subject string, req proto.Message) (*natspkg.Msg, error) {
	return r.RequestMessage(ctx, subject, Message{Data: req, MessageType: Proto})
}

func (r *requester) RequestJSONInto(ctx context.Context, subject string, req, resp any) error {
	reply, err := r.RequestJSON(ctx, subject, req)
	if err != nil {
		return err
	}

	return Decode(reply.Data, JSON, resp)
}

func (r *requester) RequestMsgPackInto(ctx context.Context, subject string, req, resp any) error {
	reply, err := r.RequestMsgPack(ctx, subject, req)
	if err != nil {
		return err
	}

	return Decode(reply.Data, MessagePack, resp)
}

func (r *requester) RequestProtoInto(ctx context.Context, subject string, req, resp proto.Message) error {
	reply, err := r.RequestProto(ctx, subject, req)
	if err != nil {
		return err
	}

	return DecodeProto(reply.Data, resp)
}

func (r *requester) RequestMessage(ctx context.Context, subject string, msg Message) (*natspkg.Msg, error) {
	if r.conn == nil {
		return nil, ErrNatsConnectionNotEstablished
	}

	if err := r.validateSubject(subject); err != nil {
		return nil, err
	}

	if msg.MessageType == 0 {
		msg.MessageType = JSON
	}
	if !validMessageType(msg.MessageType) {
		return nil, fmt.Errorf("request subject=%q: %w", subject, ErrInvalidMessageType)
	}

	reqCtx, cancel := r.withTimeout(ctx)
	defer cancel()

	spanCtx, span := startRequestSpan(reqCtx, subject, r.allowTracing)
	var reqErr error
	defer endSpanPtr(span, &reqErr)

	if r.allowTracing {
		injectTraceContext(spanCtx, &msg)
	}

	data, err := Encode(msg)
	if err != nil {
		reqErr = err
		r.recordError(spanCtx, subject)

		return nil, fmt.Errorf("request encode subject=%q: %w", subject, err)
	}

	if needsContentTypeHeader(msg) || msg.MessageType == Proto || msg.MessageType == MessagePack {
		applyContentTypeHeader(&msg)
	}

	nmsg := &natspkg.Msg{
		Subject: subject,
		Data:    data,
		Header:  natspkg.Header(msg.Header),
	}

	var start int64
	if r.allowMetrics && r.metrics != nil {
		start = time.Now().UnixNano()
	}

	reply, err := r.conn.RequestMsgWithContext(spanCtx, nmsg)
	if err != nil {
		reqErr = err
		r.recordError(spanCtx, subject)

		return nil, fmt.Errorf("request subject=%q: %w", subject, err)
	}

	if r.allowMetrics && r.metrics != nil {
		elapsed := time.Duration(time.Now().UnixNano() - start)
		r.recordSuccess(spanCtx, subject, len(data), elapsed)
	}

	return reply, nil
}

func (r *requester) recordSuccess(ctx context.Context, subject string, nbytes int, elapsed time.Duration) {
	if !r.allowMetrics || r.metrics == nil {
		return
	}

	label := r.metricSubject(subject)

	if r.metrics.requestTotal != nil {
		r.metrics.requestTotal.AddWith(ctx, 1, label)
	}
	if r.metrics.requestBytes != nil {
		r.metrics.requestBytes.RecordWith(ctx, float64(nbytes), label)
	}
	if r.metrics.requestLatency != nil {
		r.metrics.requestLatency.RecordWith(ctx, elapsed.Seconds(), label)
	}
}

func (r *requester) recordError(ctx context.Context, subject string) {
	if !r.allowMetrics || r.metrics == nil || r.metrics.requestErrors == nil {
		return
	}

	r.metrics.requestErrors.AddWith(ctx, 1, r.metricSubject(subject))
}
