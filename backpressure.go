package nats

import (
	"context"
	"fmt"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
)

func (c *consumer) handlePoolBackpressure(ctx context.Context, msg *natspkg.Msg) error {
	if c.workerPool == nil {
		return nil
	}

	depth := c.workerPool.QueueDepth()

	capacity := c.cfg.WorkerBufferSize
	if capacity <= 0 || depth < capacity {
		return nil
	}

	mode := c.backpressure.Mode
	if mode == 0 {
		mode = BackpressureBlock
	}

	switch mode {
	case BackpressureBlock:
		return ErrPoolFull
	case BackpressureNak:
		if c.metrics != nil && c.metrics.slowConsumerEvents != nil {
			c.metrics.slowConsumerEvents.Add(ctx, 1)
		}

		delay := time.Duration(0)
		if c.adaptive != nil {
			depth := 0
			if c.workerPool != nil {
				depth = c.workerPool.QueueDepth()
			}
			d := c.adaptive.Observe(AdaptivePressureInput{
				PoolDepth:     depth,
				PoolCapacity:  c.cfg.WorkerBufferSize,
				MaxAckPending: c.backpressure.MaxAckPending,
			})
			delay = d.NakDelay
		}
		var err error
		if delay > 0 {
			err = msg.NakWithDelay(delay)
		} else {
			err = msg.Nak()
		}
		if err != nil {
			return fmt.Errorf("backpressure nak subject=%q: %w", msg.Subject, err)
		}

		return ErrBackpressureHandled
	case BackpressureTerm:
		if c.metrics != nil && c.metrics.slowConsumerEvents != nil {
			c.metrics.slowConsumerEvents.Add(ctx, 1)

			if c.metrics.termTotal != nil {
				c.metrics.termTotal.Add(ctx, 1)
			}
		}

		if err := msg.Term(); err != nil {
			return fmt.Errorf("backpressure term subject=%q: %w", msg.Subject, err)
		}

		return ErrBackpressureHandled
	case BackpressureDrop:
		// True drop: Term so the message leaves Ack-pending (log-only left messages stuck).
		if c.metrics != nil && c.metrics.slowConsumerEvents != nil {
			c.metrics.slowConsumerEvents.Add(ctx, 1)

			if c.metrics.termTotal != nil {
				c.metrics.termTotal.Add(ctx, 1)
			}
		}

		zerolog.Ctx(ctx).Warn().
			Str("subject", msg.Subject).
			Msg("terminating message due to backpressure drop")

		if err := msg.Term(); err != nil {
			return fmt.Errorf("backpressure drop term subject=%q: %w", msg.Subject, err)
		}

		return ErrBackpressureHandled
	default:
		return ErrPoolFull
	}
}
