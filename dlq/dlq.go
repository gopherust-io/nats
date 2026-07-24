package dlq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"

	natspkg "github.com/nats-io/nats.go"
)

// Header keys attached when routing a poison message.
const (
	HeaderOriginalSubject = "X-NATS-Original-Subject"
	HeaderStream          = "X-NATS-Stream"
	HeaderSequence        = "X-NATS-Stream-Sequence"
	HeaderNumDelivered    = "X-NATS-Num-Delivered"
	HeaderReason          = "X-NATS-DLQ-Reason"
	HeaderConsumer        = "X-NATS-Consumer"

	HeaderAutopsyError = "X-NATS-Autopsy-Error"
	HeaderAutopsyHash  = "X-NATS-Autopsy-Hash"
	HeaderAutopsyStack = "X-NATS-Autopsy-Stack"

	headerTraceID     = "Trace-Id"
	headerMsgID       = "Nats-Msg-Id"
	headerContentType = "Nats-Content-Type"
)

const (
	defaultAutopsyMaxErrorBytes = 512
	defaultAutopsyMaxStackBytes = 2048
)

// Handler is a message handler (same signature as nats.MsgHandler).
type Handler func(ctx context.Context, msg *natspkg.Msg) error

// RawPublish is a raw JetStream publish used for DLQ passthrough.
type RawPublish struct {
	Header map[string][]string
	Data   []byte
}

// Publisher publishes poison messages to the DLQ subject.
type Publisher interface {
	PublishRaw(ctx context.Context, subject string, msg RawPublish) error
}

// Recorder receives DLQ incident notifications (optional).
type Recorder interface {
	RecordDLQ(subject, stream, consumer, reason string, seq uint64)
	RecordDLQAutopsy(subject, stream, consumer, reason, errStr string, seq uint64)
}

// AutopsyConfig enriches DLQ publishes with forensic headers.
type AutopsyConfig struct {
	Enabled       bool
	IncludeStack  bool
	MaxErrorBytes int
	MaxStackBytes int
}

func (c AutopsyConfig) withDefaults() AutopsyConfig {
	out := c
	if out.MaxErrorBytes <= 0 {
		out.MaxErrorBytes = defaultAutopsyMaxErrorBytes
	}
	if out.MaxStackBytes <= 0 {
		out.MaxStackBytes = defaultAutopsyMaxStackBytes
	}

	return out
}

// Config configures With.
type Config struct {
	Publisher  Publisher
	Recorder   Recorder
	Subject    string
	Reason     string
	Autopsy    AutopsyConfig
	MaxDeliver uint64
}

// ErrSendToDLQ may be returned from a handler to force DLQ routing + Term.
var ErrSendToDLQ = errors.New("send message to dlq")

// ErrDLQRouted is returned after a message has been published to the DLQ and Term'd.
// Consumers treat this as success (skip Ack/Nak).
var ErrDLQRouted = errors.New("message routed to dlq")

type autopsyInfo struct {
	Err   string
	Stack string
}

// With wraps a handler so poison messages are published to a DLQ subject and Term'd.
//
// Triggers:
//   - handler returns ErrSendToDLQ (or wraps it)
//   - metadata.NumDelivered >= cfg.MaxDeliver when MaxDeliver > 0
func With(cfg Config, handler Handler) Handler {
	if cfg.Publisher == nil {
		return handler
	}
	if cfg.Subject == "" {
		return handler
	}
	if cfg.Reason == "" {
		cfg.Reason = "max_deliver"
	}
	cfg.Autopsy = cfg.Autopsy.withDefaults()

	return func(ctx context.Context, msg *natspkg.Msg) error {
		if cfg.MaxDeliver > 0 {
			if meta, err := msg.Metadata(); err == nil && meta != nil && meta.NumDelivered >= cfg.MaxDeliver {
				info := &autopsyInfo{Err: cfg.Reason}
				if routeErr := route(ctx, cfg, msg, cfg.Reason, info); routeErr != nil {
					return routeErr
				}

				return ErrDLQRouted
			}
		}

		err := handler(ctx, msg)
		if err == nil {
			return nil
		}
		if !errors.Is(err, ErrSendToDLQ) {
			return err
		}

		reason := "handler_requested"
		info := &autopsyInfo{Err: err.Error()}
		if routeErr := route(ctx, cfg, msg, reason, info); routeErr != nil {
			return fmt.Errorf("dlq after handler request: %w", routeErr)
		}

		return ErrDLQRouted
	}
}

func route(ctx context.Context, cfg Config, msg *natspkg.Msg, reason string, autopsy *autopsyInfo) error {
	headers := map[string][]string{
		HeaderOriginalSubject: {msg.Subject},
		HeaderReason:          {reason},
	}
	if msg.Header != nil {
		if tid := msg.Header.Get(headerTraceID); tid != "" {
			headers[headerTraceID] = []string{tid}
		}
		if mid := msg.Header.Get(headerMsgID); mid != "" {
			headers[headerMsgID] = []string{mid}
		}
		if ct := msg.Header.Get(headerContentType); ct != "" {
			headers[headerContentType] = []string{ct}
		}
	}

	if meta, err := msg.Metadata(); err == nil && meta != nil {
		headers[HeaderStream] = []string{meta.Stream}
		headers[HeaderSequence] = []string{fmt.Sprintf("%d", meta.Sequence.Stream)}
		headers[HeaderNumDelivered] = []string{fmt.Sprintf("%d", meta.NumDelivered)}
		headers[HeaderConsumer] = []string{meta.Consumer}
	}

	if cfg.Autopsy.Enabled {
		applyAutopsyHeaders(headers, cfg.Autopsy, msg.Data, autopsy)
	}

	pubErr := cfg.Publisher.PublishRaw(ctx, cfg.Subject, RawPublish{
		Data:   msg.Data,
		Header: headers,
	})
	if pubErr != nil {
		return fmt.Errorf("publish to dlq subject=%q: %w", cfg.Subject, pubErr)
	}

	if termErr := msg.Term(); termErr != nil {
		return fmt.Errorf("term after dlq subject=%q: %w", msg.Subject, termErr)
	}

	if cfg.Recorder != nil {
		var (
			stream, consumer string
			seq              uint64
		)
		if meta, err := msg.Metadata(); err == nil && meta != nil {
			stream = meta.Stream
			consumer = meta.Consumer
			seq = meta.Sequence.Stream
		}
		if cfg.Autopsy.Enabled {
			errStr := ""
			if autopsy != nil {
				errStr = autopsy.Err
			}
			cfg.Recorder.RecordDLQAutopsy(msg.Subject, stream, consumer, reason, errStr, seq)
		} else {
			cfg.Recorder.RecordDLQ(msg.Subject, stream, consumer, reason, seq)
		}
	}

	return nil
}

func applyAutopsyHeaders(headers map[string][]string, cfg AutopsyConfig, data []byte, info *autopsyInfo) {
	sum := sha256.Sum256(data)
	headers[HeaderAutopsyHash] = []string{hex.EncodeToString(sum[:])}

	if info == nil {
		return
	}
	if info.Err != "" {
		headers[HeaderAutopsyError] = []string{truncateString(info.Err, cfg.MaxErrorBytes)}
	}
	if cfg.IncludeStack && info.Stack != "" {
		headers[HeaderAutopsyStack] = []string{truncateString(info.Stack, cfg.MaxStackBytes)}
	}
}

func truncateString(s string, maxBytes int) string {
	if maxBytes <= 0 || len(s) <= maxBytes {
		return s
	}

	return s[:maxBytes]
}
