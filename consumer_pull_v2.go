package nats

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"github.com/rs/zerolog"

	"github.com/gopherust-io/nats/workerpool"
)

// pullConsumerV2 binds a durable via the new jetstream API (domain-aware, multi-filter).
type pullConsumerV2 struct {
	parent  *consumer
	cons    jetstream.Consumer
	metrics *clientMetrics
	mu      sync.Mutex
	closed  bool
}

func newPullConsumerV2(ctx context.Context, parent *consumer, stream, durable string) (*pullConsumerV2, error) {
	if parent == nil || parent.jsNew == nil {
		return nil, ErrJetStreamV2Required
	}
	if ctx == nil {
		ctx = context.Background()
	}
	cons, err := parent.jsNew.Consumer(ctx, stream, durable)
	if err != nil {
		return nil, fmt.Errorf("pull v2 consumer stream=%q durable=%q: %w", stream, durable, err)
	}

	return &pullConsumerV2{parent: parent, cons: cons, metrics: parent.metrics}, nil
}

func (p *pullConsumerV2) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.closed = true

	return nil
}

func (p *pullConsumerV2) ensureOpen() error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return fmt.Errorf("pull: subscription closed")
	}

	return nil
}

func (p *pullConsumerV2) Fetch(ctx context.Context, batch int, opts ...FetchOpt) ([]*natspkg.Msg, error) {
	if err := p.ensureOpen(); err != nil {
		return nil, err
	}
	cfg := fetchConfig{maxWait: defaultFetchMaxWait}
	for _, opt := range opts {
		opt(&cfg)
	}
	if ctx == nil {
		ctx = context.Background()
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	// FetchContext and FetchMaxWait are mutually exclusive in nats.go; fold maxWait into ctx.
	fetchCtx := ctx
	var cancel context.CancelFunc
	if cfg.maxWait > 0 {
		fetchCtx, cancel = context.WithTimeout(ctx, cfg.maxWait)
		defer cancel()
	}

	fetchOpts := make([]jetstream.FetchOpt, 0, 2)
	fetchOpts = append(fetchOpts, jetstream.FetchContext(fetchCtx))
	if cfg.heartbeat > 0 {
		hb := cfg.heartbeat
		if cfg.maxWait > 0 && hb >= cfg.maxWait {
			hb = cfg.maxWait / 2
		}
		if hb > 0 {
			fetchOpts = append(fetchOpts, jetstream.FetchHeartbeat(hb))
		}
	}

	start := time.Now()
	batchResult, err := p.cons.Fetch(batch, fetchOpts...)
	if p.metrics != nil && p.metrics.fetchWaitTime != nil {
		p.metrics.fetchWaitTime.Record(ctx, time.Since(start).Seconds())
	}
	if err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return nil, err
		}

		return nil, fmt.Errorf("pull v2 fetch batch=%d: %w", batch, err)
	}

	out := make([]*natspkg.Msg, 0, batch)
	for msg := range batchResult.Messages() {
		if ctx.Err() != nil {
			return out, ctx.Err()
		}
		out = append(out, jetstreamMsgToNATS(msg))
	}
	if ferr := batchResult.Error(); ferr != nil && len(out) == 0 {
		if isFetchIdleErr(ferr) {
			return out, nil
		}
		if errors.Is(ferr, context.Canceled) || errors.Is(ferr, context.DeadlineExceeded) {
			return out, ferr
		}

		return out, fmt.Errorf("pull v2 fetch batch=%d: %w", batch, ferr)
	}
	if p.metrics != nil && p.metrics.fetchBatchSize != nil {
		p.metrics.fetchBatchSize.Record(ctx, float64(len(out)))
	}

	return out, nil
}

func (p *pullConsumerV2) FetchNoWait(batch int) ([]*natspkg.Msg, error) {
	return p.Fetch(context.Background(), batch, WithFetchMaxWait(time.Millisecond))
}

func (p *pullConsumerV2) Process(ctx context.Context, handler MsgHandler, opts ...ProcessOpt) error {
	cfg := &processConfig{}
	for _, opt := range opts {
		opt(cfg)
	}
	if cfg.batch <= 0 {
		cfg.batch = defaultFetchBatch
	}
	if cfg.maxWait <= 0 {
		cfg.maxWait = defaultFetchMaxWait
	}
	if cfg.heartbeat <= 0 && p.parent != nil && p.parent.cfg.IdleHeartbeat > 0 {
		cfg.heartbeat = p.parent.cfg.IdleHeartbeat
	}

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("pull process: %w", ctx.Err())
		default:
		}

		fetchOpts := []FetchOpt{WithFetchMaxWait(cfg.maxWait)}
		if cfg.heartbeat > 0 {
			fetchOpts = append(fetchOpts, WithFetchHeartbeat(cfg.heartbeat))
		}

		fetchBatch := cfg.batch
		if p.parent != nil && p.parent.adaptive != nil {
			depth, poolCap := 0, 0
			if p.parent.workerPool != nil {
				depth = p.parent.workerPool.QueueDepth()
				poolCap = p.parent.cfg.WorkerBufferSize
			}
			decision := p.parent.adaptive.Observe(AdaptivePressureInput{
				PoolDepth:     depth,
				PoolCapacity:  poolCap,
				CurrentBatch:  fetchBatch,
				MaxAckPending: p.parent.backpressure.MaxAckPending,
			})
			if decision.FetchBatch > 0 {
				fetchBatch = decision.FetchBatch
			}
			if p.parent.circuit != nil && p.parent.circuit.ShouldPause(decision) {
				select {
				case <-ctx.Done():
					return fmt.Errorf("pull process: %w", ctx.Err())
				case <-time.After(p.parent.circuit.PauseDuration()):
					continue
				}
			}
		}

		messages, err := p.Fetch(ctx, fetchBatch, fetchOpts...)
		if err != nil {
			if errors.Is(err, context.Canceled) {
				if len(messages) > 0 {
					nakRemaining(messages)
				}

				return fmt.Errorf("pull process: %w", err)
			}
			if errors.Is(err, context.DeadlineExceeded) || isFetchIdleErr(err) {
				continue
			}

			return fmt.Errorf("pull process fetch: %w", err)
		}
		if len(messages) == 0 {
			continue
		}

		if err := p.processBatch(ctx, messages, handler, cfg); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if errors.Is(err, workerpool.ErrPoolStopped) {
				return err
			}
			zerolog.Ctx(ctx).Error().Err(err).Msg("pull v2 process batch error; continuing")
		}
	}
}

func (p *pullConsumerV2) processBatch(ctx context.Context, messages []*natspkg.Msg, handler MsgHandler, cfg *processConfig) error {
	if len(messages) == 0 {
		return nil
	}
	batchStart := time.Now()
	legacy := &pullConsumer{parent: p.parent, metrics: p.metrics}
	if p.parent != nil && p.parent.cfg.WorkerPoolEnabled {
		if err := legacy.processBatchWithPool(ctx, messages, handler); err != nil {
			nakRemaining(messages)

			return err
		}
	} else if cfg.concurrency > 1 {
		if err := legacy.processBatchConcurrent(ctx, messages, handler, cfg.concurrency); err != nil {
			return err
		}
	} else {
		for i, msg := range messages {
			if ctx.Err() != nil {
				nakRemaining(messages[i:])

				return fmt.Errorf("pull process: %w", ctx.Err())
			}
			if err := p.parent.processMessage(ctx, msg, handler); err != nil {
				zerolog.Ctx(ctx).Error().Err(err).Str("subject", msg.Subject).Msg("pull v2 process message")
			}
		}
	}
	if p.metrics != nil && p.metrics.pullBatchProcessTime != nil {
		p.metrics.pullBatchProcessTime.Record(ctx, time.Since(batchStart).Seconds())
	}

	return nil
}

func isFetchIdleErr(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}
	msg := err.Error()

	return strings.Contains(strings.ToLower(msg), "timeout") ||
		strings.Contains(msg, "no messages") ||
		strings.Contains(msg, "404") ||
		strings.Contains(msg, "408")
}

// jetstreamMsgToNATS builds a *nats.Msg that can Ack/Nak via the reply subject.
func jetstreamMsgToNATS(msg jetstream.Msg) *natspkg.Msg {
	if msg == nil {
		return nil
	}

	return &natspkg.Msg{
		Subject: msg.Subject(),
		Reply:   msg.Reply(),
		Header:  msg.Headers(),
		Data:    msg.Data(),
	}
}
