package nats

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/gopherust-io/tel"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

// metricSubjectLabel blanks the subject when fixedCardinality metrics are enabled.
func metricSubjectLabel(m *clientMetrics, subject string) string {
	if m != nil && m.fixedCardinality {
		return empty
	}

	return subject
}

func validateOutboundSubject(subject, op string, skip bool) error {
	if skip {
		return nil
	}
	if err := ValidatePublishSubject(subject); err != nil {
		if errors.Is(err, ErrInvalidSubject) && bytesconv.IsEmpty(subject) {
			return fmt.Errorf("%s subject=%q: %w", op, subject, ErrEmptySubjectNotAllowed)
		}

		return fmt.Errorf("%s subject=%q: %w", op, subject, err)
	}

	return nil
}

func (m *clientMetrics) addCounter(ctx context.Context, c *tel.FastCounter, subject string) {
	if m == nil || c == nil {
		return
	}
	c.AddWith(ctx, 1, metricSubjectLabel(m, subject))
}

func (m *clientMetrics) recordBytesLatency(
	ctx context.Context,
	bytesHist, latencyHist *tel.FastHistogram,
	subject string,
	nbytes int,
	elapsed time.Duration,
) {
	if m == nil {
		return
	}
	label := metricSubjectLabel(m, subject)
	if bytesHist != nil {
		bytesHist.RecordWith(ctx, float64(nbytes), label)
	}
	if latencyHist != nil {
		latencyHist.RecordWith(ctx, elapsed.Seconds(), label)
	}
}

// trySend non-blocks sending ev on ch (drops when full).
func trySend[T any](ch chan T, ev T) {
	select {
	case ch <- ev:
	default:
	}
}
