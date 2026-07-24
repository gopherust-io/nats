package nats

import (
	"context"

	"github.com/gopherust-io/nats/dlq"
)

// DLQ header keys (re-exported from nats/dlq).
const (
	HeaderDLQOriginalSubject = dlq.HeaderOriginalSubject
	HeaderDLQStream          = dlq.HeaderStream
	HeaderDLQSequence        = dlq.HeaderSequence
	HeaderDLQNumDelivered    = dlq.HeaderNumDelivered
	HeaderDLQReason          = dlq.HeaderReason
	HeaderDLQConsumer        = dlq.HeaderConsumer

	HeaderAutopsyError = dlq.HeaderAutopsyError
	HeaderAutopsyHash  = dlq.HeaderAutopsyHash
	HeaderAutopsyStack = dlq.HeaderAutopsyStack
)

// AutopsyConfig enriches DLQ publishes with forensic headers.
type AutopsyConfig = dlq.AutopsyConfig

// DLQConfig configures WithDLQ.
type DLQConfig struct {
	Publisher Publisher
	// Recorder receives an IncidentDLQ event after a successful route (optional).
	Recorder *FlightRecorder
	// Subject is the dead-letter publish subject (e.g. "orders.dlq").
	Subject string
	// Reason is stored in HeaderDLQReason when auto-routing on MaxDeliver.
	Reason string
	// Autopsy adds forensic headers (error, payload hash, optional stack).
	Autopsy AutopsyConfig
	// MaxDeliver routes to DLQ when metadata.NumDelivered >= MaxDeliver (0 disables auto route).
	MaxDeliver uint64
}

// ErrSendToDLQ may be returned from a handler to force DLQ routing + Term.
var ErrSendToDLQ = dlq.ErrSendToDLQ

// ErrDLQRouted is returned (and treated as success by processMessage) after a
// message has been published to the DLQ and Term'd.
var ErrDLQRouted = dlq.ErrDLQRouted

type dlqPublisherAdapter struct {
	p Publisher
}

func (a dlqPublisherAdapter) PublishRaw(ctx context.Context, subject string, msg dlq.RawPublish) error {
	return a.p.PublishMessage(ctx, subject, Message{
		Data:        msg.Data,
		Header:      msg.Header,
		MessageType: Raw,
	})
}

// WithDLQ wraps a handler so poison messages are published to a DLQ subject and Term'd.
// Prefer importing github.com/gopherust-io/nats/dlq for new code.
func WithDLQ(cfg DLQConfig, handler MsgHandler) MsgHandler {
	dcfg := dlq.Config{
		Subject:    cfg.Subject,
		MaxDeliver: cfg.MaxDeliver,
		Reason:     cfg.Reason,
		Autopsy:    cfg.Autopsy,
	}
	if cfg.Publisher != nil {
		dcfg.Publisher = dlqPublisherAdapter{p: cfg.Publisher}
	}
	if cfg.Recorder != nil {
		dcfg.Recorder = cfg.Recorder
	}

	return MsgHandler(dlq.With(dcfg, dlq.Handler(handler)))
}
