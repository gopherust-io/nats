package nats

import (
	"context"
	"fmt"
	"log/slog"

	natspkg "github.com/nats-io/nats.go"
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
		if c.metrics != nil {
			c.metrics.slowConsumerEvents.Add(ctx, 1)
		}

		if err := msg.Nak(); err != nil {
			return fmt.Errorf("backpressure nak subject=%q: %w", msg.Subject, err)
		}

		return ErrBackpressureHandled
	case BackpressureTerm:
		if c.metrics != nil {
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
		if c.metrics != nil {
			c.metrics.slowConsumerEvents.Add(ctx, 1)
		}

		slog.WarnContext(ctx, "dropping message due to backpressure",
			slog.String("subject", msg.Subject))

		return ErrBackpressureHandled
	default:
		return ErrPoolFull
	}
}
