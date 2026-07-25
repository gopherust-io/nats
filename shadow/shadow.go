package shadow

import (
	"context"
	"fmt"
	"math/rand/v2"

	_ "github.com/gopherust-io/tel"
	natspkg "github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
)

// Handler is a message handler (same signature as nats.MsgHandler).
type Handler func(ctx context.Context, msg *natspkg.Msg) error

// Recorder receives shadow incident notifications (optional).
type Recorder interface {
	RecordShadow(detail, subject, err string)
}

// Metrics records shadow canary counters (optional).
type Metrics interface {
	ShadowError(ctx context.Context)
	ShadowMismatch(ctx context.Context)
}

// Config configures With.
type Config struct {
	// Recorder receives events on panic / mismatch (optional).
	Recorder Recorder
	// Metrics records canary counters (optional).
	Metrics Metrics
	// Compare returns true when primary and shadow outcomes match.
	// If nil, outcomes match when both succeed or both fail (nil-ness only).
	Compare func(primaryErr, shadowErr error) bool
	// SampleRate is the fraction of messages sent to the shadow in (0,1].
	// Zero (default) means 0.1 (10% canary). Set explicitly to 1 for always-on dual-run.
	SampleRate float64
}

func (c Config) withDefaults() Config {
	out := c
	if out.SampleRate <= 0 {
		out.SampleRate = 0.1
	}
	if out.SampleRate > 1 {
		out.SampleRate = 1
	}

	return out
}

// With runs primary for delivery fate and optionally a shadow handler on a
// cloned message (no Reply) so Ack/Nak/Term from shadow cannot affect JetStream.
func With(cfg Config, primary, shadow Handler) Handler {
	if primary == nil {
		return primary
	}
	if shadow == nil {
		return primary
	}

	cfg = cfg.withDefaults()

	compare := cfg.Compare
	if compare == nil {
		compare = func(primaryErr, shadowErr error) bool {
			return (primaryErr == nil) == (shadowErr == nil)
		}
	}

	return func(ctx context.Context, msg *natspkg.Msg) error {
		primaryErr := primary(ctx, msg)

		if cfg.SampleRate < 1 && rand.Float64() >= cfg.SampleRate {
			return primaryErr
		}

		shadowErr := run(ctx, shadow, cloneMsg(msg))
		if shadowErr != nil {
			if cfg.Metrics != nil {
				cfg.Metrics.ShadowError(ctx)
			}
			if cfg.Recorder != nil {
				cfg.Recorder.RecordShadow("shadow_error", msg.Subject, shadowErr.Error())
			}
		}

		if !compare(primaryErr, shadowErr) {
			if cfg.Metrics != nil {
				cfg.Metrics.ShadowMismatch(ctx)
			}
			if cfg.Recorder != nil {
				cfg.Recorder.RecordShadow("shadow_mismatch", msg.Subject,
					fmt.Sprintf("primary=%v shadow=%v", primaryErr, shadowErr))
			}
		}

		return primaryErr
	}
}

func run(ctx context.Context, shadow Handler, msg *natspkg.Msg) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			err = fmt.Errorf("shadow panic: %v", rec)
			zerolog.Ctx(ctx).Error().Any("panic", rec).Msg("shadow handler panic")
		}
	}()

	return shadow(ctx, msg)
}

func cloneMsg(msg *natspkg.Msg) *natspkg.Msg {
	if msg == nil {
		return nil
	}

	out := &natspkg.Msg{
		Subject: msg.Subject,
		Data:    append([]byte(nil), msg.Data...),
	}
	if msg.Header != nil {
		out.Header = make(natspkg.Header, len(msg.Header))
		for k, vs := range msg.Header {
			out.Header[k] = append([]string(nil), vs...)
		}
	}

	return out
}
