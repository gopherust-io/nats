package nats

import (
	"context"
	"fmt"

	natspkg "github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// Responder registers core NATS reply handlers (not JetStream; no auto Ack/Nak).
type Responder interface {
	Subscribe(ctx context.Context, subject string, handler MsgHandler) (Subscription, error)
	QueueSubscribe(ctx context.Context, queue, subject string, handler MsgHandler) (Subscription, error)
}

// goalign:ignore
type responder struct {
	conn         *natspkg.Conn
	metrics      *clientMetrics
	allowMetrics bool
	allowTracing bool
}

func newResponder(cfg ResponderConfig, conn *natspkg.Conn, metrics *clientMetrics, allowTracing bool) *responder {
	return &responder{
		conn:         conn,
		metrics:      metrics,
		allowMetrics: cfg.AllowMetrics,
		allowTracing: allowTracing && cfg.AllowTracing,
	}
}

func (r *responder) Subscribe(ctx context.Context, subject string, handler MsgHandler) (Subscription, error) {
	if r.conn == nil {
		return nil, ErrNatsConnectionNotEstablished
	}
	if err := ValidateSubject(subject); err != nil {
		return nil, fmt.Errorf("responder subscribe subject=%q: %w", subject, err)
	}
	if handler == nil {
		return nil, fmt.Errorf("responder subscribe subject=%q: nil handler", subject)
	}

	sub, err := r.conn.Subscribe(subject, r.wrap(ctx, handler))
	if err != nil {
		return nil, fmt.Errorf("responder subscribe subject=%q: %w", subject, err)
	}

	wrapped := &subscription{sub: sub}
	drainOnCancel(ctx, wrapped)

	return wrapped, nil
}

func (r *responder) QueueSubscribe(ctx context.Context, queue, subject string, handler MsgHandler) (Subscription, error) {
	if r.conn == nil {
		return nil, ErrNatsConnectionNotEstablished
	}
	if err := ValidateSubject(subject); err != nil {
		return nil, fmt.Errorf("responder queue subscribe subject=%q: %w", subject, err)
	}
	if err := ValidateQueueName(queue); err != nil {
		return nil, fmt.Errorf("responder queue subscribe queue=%q: %w", queue, err)
	}
	if handler == nil {
		return nil, fmt.Errorf("responder queue subscribe subject=%q: nil handler", subject)
	}

	sub, err := r.conn.QueueSubscribe(subject, queue, r.wrap(ctx, handler))
	if err != nil {
		return nil, fmt.Errorf("responder queue subscribe subject=%q queue=%q: %w", subject, queue, err)
	}

	wrapped := &subscription{sub: sub}
	drainOnCancel(ctx, wrapped)

	return wrapped, nil
}

func (r *responder) wrap(parent context.Context, handler MsgHandler) natspkg.MsgHandler {
	return func(msg *natspkg.Msg) {
		ctx := parent
		spanCtx, span := startReplySpan(ctx, msg, r.allowTracing)
		var handlerErr error
		defer endSpanPtr(span, &handlerErr)

		handlerErr = handler(spanCtx, msg)
		if handlerErr != nil {
			r.recordError(spanCtx, msg.Subject)

			return
		}
		r.recordHandled(spanCtx, msg.Subject)
	}
}

func (r *responder) metricSubject(subject string) string {
	if r.metrics != nil && r.metrics.fixedCardinality {
		return empty
	}

	return subject
}

func (r *responder) recordHandled(ctx context.Context, subject string) {
	if !r.allowMetrics || r.metrics == nil || r.metrics.replyHandled == nil {
		return
	}
	r.metrics.replyHandled.AddWith(ctx, 1, r.metricSubject(subject))
}

func (r *responder) recordError(ctx context.Context, subject string) {
	if !r.allowMetrics || r.metrics == nil || r.metrics.replyErrors == nil {
		return
	}
	r.metrics.replyErrors.AddWith(ctx, 1, r.metricSubject(subject))
}

// RespondBytes replies with raw bytes.
func RespondBytes(msg *natspkg.Msg, data []byte) error {
	if msg == nil {
		return fmt.Errorf("respond bytes: nil message")
	}

	return msg.Respond(data)
}

// RespondJSON encodes v as JSON and replies (sets content-type header).
func RespondJSON(msg *natspkg.Msg, v any) error {
	return respondEncoded(msg, Message{Data: v, MessageType: JSON})
}

// RespondMsgPack encodes v as MessagePack and replies.
func RespondMsgPack(msg *natspkg.Msg, v any) error {
	return respondEncoded(msg, Message{Data: v, MessageType: MessagePack})
}

// RespondProto encodes v as protobuf and replies.
func RespondProto(msg *natspkg.Msg, v proto.Message) error {
	return respondEncoded(msg, Message{Data: v, MessageType: Proto})
}

func respondEncoded(msg *natspkg.Msg, replyMsg Message) error {
	if msg == nil {
		return fmt.Errorf("respond: nil message")
	}

	data, err := Encode(replyMsg)
	if err != nil {
		return fmt.Errorf("respond encode: %w", err)
	}

	applyContentTypeHeader(&replyMsg)
	reply := &natspkg.Msg{
		Data:   data,
		Header: natspkg.Header(replyMsg.Header),
	}

	return msg.RespondMsg(reply)
}
