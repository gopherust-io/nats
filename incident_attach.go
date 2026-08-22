package nats

import (
	"context"

	natspkg "github.com/nats-io/nats.go"
)

// AttachToHandler wraps handler so handler errors trigger an incident capsule capture.
// Prefer CapsuleAuto + dlq.With for production DLQ/anomaly hooks; this is the one-line
// Incidents wiring helper for ad-hoc handlers.
func (i *incidents) AttachToHandler(stream, durable string, cfg CapsuleAutoConfig, handler MsgHandler) MsgHandler {
	if handler == nil || i == nil {
		return handler
	}
	if cfg.Window <= 0 {
		cfg.Window = defaultCapsuleWindow
	}
	if cfg.StoreBucket == "" {
		cfg.StoreBucket = DefaultIncidentCapsuleBucket
	}
	if cfg.IndexBucket == "" {
		cfg.IndexBucket = DefaultIncidentIndexBucket
	}
	if cfg.Redact == nil {
		cfg.Redact = defaultCapsuleRedact
	}
	if !cfg.Enabled {
		return handler
	}

	return func(ctx context.Context, msg *natspkg.Msg) error {
		err := handler(ctx, msg)
		if err == nil {
			return nil
		}
		captureStream, captureDurable := stream, durable
		var seq uint64
		if meta, metaErr := msg.Metadata(); metaErr == nil && meta != nil {
			seq = meta.Sequence.Stream
			if captureStream == "" {
				captureStream = meta.Stream
			}
			if captureDurable == "" {
				captureDurable = meta.Consumer
			}
		}
		capsule, cerr := i.Capture(ctx, IncidentCapture{
			Stream:         captureStream,
			Consumer:       captureDurable,
			Trigger:        TriggerManual,
			FailingSeq:     seq,
			Window:         cfg.Window,
			Subject:        msg.Subject,
			Reason:         err.Error(),
			StoreBucket:    cfg.StoreBucket,
			IndexBucket:    cfg.IndexBucket,
			FlightRecorder: cfg.FlightRecorder,
			Redact:         cfg.Redact,
		})
		if cerr == nil && capsule != nil && cfg.OnReady != nil {
			cfg.OnReady(capsule)
		}

		return err
	}
}

// AttachIncidentsHandler wraps handler using any Incidents implementation.
func AttachIncidentsHandler(inc Incidents, stream, durable string, cfg CapsuleAutoConfig, handler MsgHandler) MsgHandler {
	impl, ok := inc.(*incidents)
	if !ok || impl == nil {
		return handler
	}

	return impl.AttachToHandler(stream, durable, cfg, handler)
}
