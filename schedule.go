package nats

import (
	"context"
	"fmt"
	"time"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

// SchedulePublishConfig delays delivery via a schedule subject convention.
// Default: publish to "{subject}.delay.{ms}" so an ops-owned stream/consumer
// can forward after the delay (or use a server-side scheduler when available).
//
// goalign:ignore
type SchedulePublishConfig struct {
	// SubjectTemplate uses %s for base subject and %d for delay milliseconds.
	// Default: "%s.delay.%d"
	SubjectTemplate string
}

// PublishDelayedJSON publishes JSON to a delay subject derived from subject + delay.
func (p *publisher) PublishDelayedJSON(
	ctx context.Context,
	subject string,
	delay time.Duration,
	data any,
	cfg SchedulePublishConfig,
) error {
	dest, err := delayedSubject(subject, delay, cfg)
	if err != nil {
		return err
	}

	return p.PublishJSON(ctx, dest, data)
}

// PublishDelayedJSON is exposed on Client via Publisher type assertion helpers.
func (c *client) PublishDelayedJSON(ctx context.Context, subject string, delay time.Duration, data any, cfg SchedulePublishConfig) error {
	return c.publisher.PublishDelayedJSON(ctx, subject, delay, data, cfg)
}

func delayedSubject(subject string, delay time.Duration, cfg SchedulePublishConfig) (string, error) {
	if err := ValidateSubject(subject); err != nil {
		return "", err
	}
	if delay < 0 {
		return "", fmt.Errorf("delayed publish: negative delay")
	}
	tpl := cfg.SubjectTemplate
	if bytesconv.IsEmpty(tpl) {
		tpl = "%s.delay.%d"
	}
	ms := delay.Milliseconds()
	if ms < 0 {
		ms = 0
	}

	return fmt.Sprintf(tpl, subject, ms), nil
}
