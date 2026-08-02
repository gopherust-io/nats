package nats

import (
	"context"
	"fmt"

	natspkg "github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
)

// Responder registers core NATS reply handlers (not JetStream; no auto Ack/Nak).
// Respond* methods apply ResponderConfig.PayloadCompression; package-level
// Respond* helpers leave payloads uncompressed.
type Responder interface {
	Subscribe(ctx context.Context, subject string, handler MsgHandler) (Subscription, error)
	QueueSubscribe(ctx context.Context, queue, subject string, handler MsgHandler) (Subscription, error)

	RespondBytes(msg *natspkg.Msg, data []byte) error
	RespondJSON(msg *natspkg.Msg, v any) error
	RespondMsgPack(msg *natspkg.Msg, v any) error
	RespondProto(msg *natspkg.Msg, v proto.Message) error
}

// goalign:ignore
type responder struct {
	conn                 *natspkg.Conn
	metrics              *clientMetrics
	allowMetrics         bool
	allowTracing         bool
	payloadCompression   PayloadCompressionMode
	payloadDecompression bool
}

func newResponder(cfg ResponderConfig, conn *natspkg.Conn, metrics *clientMetrics, allowTracing bool) *responder {
	return &responder{
		conn:                 conn,
		metrics:              metrics,
		allowMetrics:         cfg.AllowMetrics,
		allowTracing:         allowTracing && cfg.AllowTracing,
		payloadCompression:   cfg.PayloadCompression,
		payloadDecompression: cfg.PayloadDecompression,
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

		if r.payloadDecompression {
			if err := maybeDecompressMsg(msg); err != nil {
				handlerErr = err
				r.recordError(spanCtx, msg.Subject)

				return
			}
		}

		handlerErr = invokeMsgHandler(spanCtx, msg, handler)
		if handlerErr != nil {
			r.recordError(spanCtx, msg.Subject)

			return
		}
		r.recordHandled(spanCtx, msg.Subject)
	}
}

func (r *responder) recordHandled(ctx context.Context, subject string) {
	if !r.allowMetrics || r.metrics == nil {
		return
	}
	r.metrics.addCounter(ctx, r.metrics.replyHandled, subject)
}

func (r *responder) recordError(ctx context.Context, subject string) {
	if !r.allowMetrics || r.metrics == nil {
		return
	}
	r.metrics.addCounter(ctx, r.metrics.replyErrors, subject)
}

func (r *responder) RespondBytes(msg *natspkg.Msg, data []byte) error {
	return respondBytes(msg, data, r.payloadCompression)
}

func (r *responder) RespondJSON(msg *natspkg.Msg, v any) error {
	return respondEncoded(msg, Message{Data: v, MessageType: JSON}, r.payloadCompression)
}

func (r *responder) RespondMsgPack(msg *natspkg.Msg, v any) error {
	return respondEncoded(msg, Message{Data: v, MessageType: MessagePack}, r.payloadCompression)
}

func (r *responder) RespondProto(msg *natspkg.Msg, v proto.Message) error {
	return respondEncoded(msg, Message{Data: v, MessageType: Proto}, r.payloadCompression)
}

// RespondBytes replies with raw bytes (no compression).
func RespondBytes(msg *natspkg.Msg, data []byte) error {
	return respondBytes(msg, data, PayloadCompressionOff)
}

// RespondJSON encodes v as JSON and replies (sets content-type header; no compression).
func RespondJSON(msg *natspkg.Msg, v any) error {
	return respondEncoded(msg, Message{Data: v, MessageType: JSON}, PayloadCompressionOff)
}

// RespondMsgPack encodes v as MessagePack and replies (no compression).
func RespondMsgPack(msg *natspkg.Msg, v any) error {
	return respondEncoded(msg, Message{Data: v, MessageType: MessagePack}, PayloadCompressionOff)
}

// RespondProto encodes v as protobuf and replies (no compression).
func RespondProto(msg *natspkg.Msg, v proto.Message) error {
	return respondEncoded(msg, Message{Data: v, MessageType: Proto}, PayloadCompressionOff)
}

func respondBytes(msg *natspkg.Msg, data []byte, mode PayloadCompressionMode) error {
	if msg == nil {
		return fmt.Errorf("respond bytes: nil message")
	}
	if mode == PayloadCompressionOff {
		return msg.Respond(data)
	}

	data, hdr := applyPayloadCompression(mode, data, nil)
	if len(hdr) == 0 {
		return msg.Respond(data)
	}

	return msg.RespondMsg(&natspkg.Msg{
		Data:   data,
		Header: natspkg.Header(hdr),
	})
}

func respondEncoded(msg *natspkg.Msg, replyMsg Message, mode PayloadCompressionMode) error {
	if msg == nil {
		return fmt.Errorf("respond: nil message")
	}

	data, err := Encode(replyMsg)
	if err != nil {
		return fmt.Errorf("respond encode: %w", err)
	}

	applyContentTypeHeader(&replyMsg)
	data, replyMsg.Header = applyPayloadCompression(mode, data, replyMsg.Header)
	reply := &natspkg.Msg{
		Data:   data,
		Header: natspkg.Header(replyMsg.Header),
	}

	return msg.RespondMsg(reply)
}
