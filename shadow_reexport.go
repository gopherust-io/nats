package nats

import (
	"context"

	"github.com/gopherust-io/nats/shadow"
)

// ShadowConfig configures WithShadow.
type ShadowConfig struct {
	// Compare returns true when primary and shadow outcomes match.
	// If nil, outcomes match when both succeed or both fail (nil-ness only).
	Compare func(primaryErr, shadowErr error) bool
	// Recorder receives IncidentShadow events on panic / mismatch (optional).
	Recorder *FlightRecorder

	metrics *clientMetrics
	// SampleRate is the fraction of messages sent to the shadow in (0,1].
	// Zero (default) means 0.1 (10% canary). Set explicitly to 1 for always-on dual-run.
	SampleRate float64
}

type shadowRecorderAdapter struct {
	r *FlightRecorder
}

func (a shadowRecorderAdapter) RecordShadow(detail, subject, err string) {
	a.r.Record(IncidentEvent{
		Kind:    IncidentShadow,
		Detail:  detail,
		Subject: subject,
		Err:     err,
	})
}

type shadowMetricsAdapter struct {
	m *clientMetrics
}

func (a shadowMetricsAdapter) ShadowError(ctx context.Context) {
	if a.m != nil && a.m.shadowErrorTotal != nil {
		a.m.shadowErrorTotal.Add(ctx, 1)
	}
}

func (a shadowMetricsAdapter) ShadowMismatch(ctx context.Context) {
	if a.m != nil && a.m.shadowMismatchTotal != nil {
		a.m.shadowMismatchTotal.Add(ctx, 1)
	}
}

// WithShadow runs primary for delivery fate and optionally a shadow handler.
// Prefer importing github.com/gopherust-io/nats/shadow for new code.
func WithShadow(cfg ShadowConfig, primary, shadowHandler MsgHandler) MsgHandler {
	scfg := shadow.Config{
		Compare:    cfg.Compare,
		SampleRate: cfg.SampleRate,
	}
	if cfg.Recorder != nil {
		scfg.Recorder = shadowRecorderAdapter{r: cfg.Recorder}
	}
	if cfg.metrics != nil {
		scfg.Metrics = shadowMetricsAdapter{m: cfg.metrics}
	}

	return MsgHandler(shadow.With(scfg, shadow.Handler(primary), shadow.Handler(shadowHandler)))
}

// WithShadow attaches client metrics to ShadowConfig then wraps the handlers.
func (c *client) WithShadow(cfg ShadowConfig, primary, shadowHandler MsgHandler) MsgHandler {
	cfg.metrics = c.metrics

	return WithShadow(cfg, primary, shadowHandler)
}
