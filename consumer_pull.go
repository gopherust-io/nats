package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/rs/zerolog"

	"github.com/gopherust-io/nats/internal/bytesconv"
	"github.com/gopherust-io/nats/workerpool"
)

type PullConsumer interface {
	Fetch(ctx context.Context, batch int, opts ...FetchOpt) ([]*natspkg.Msg, error)
	FetchNoWait(batch int) ([]*natspkg.Msg, error)
	Process(ctx context.Context, handler MsgHandler, opts ...ProcessOpt) error
	Close() error
}

type (
	FetchOpt   func(*fetchConfig)
	ProcessOpt func(*processConfig)
)

type fetchConfig struct {
	maxWait   time.Duration
	heartbeat time.Duration
}

type processConfig struct {
	batch       int
	maxWait     time.Duration
	heartbeat   time.Duration
	concurrency int
}

func WithFetchMaxWait(d time.Duration) FetchOpt {
	return func(c *fetchConfig) { c.maxWait = d }
}

// WithFetchHeartbeat sets a PullHeartbeat for the fetch request.
// Must be less than MaxWait; on miss, Fetch returns ErrNoHeartbeat.
func WithFetchHeartbeat(d time.Duration) FetchOpt {
	return func(c *fetchConfig) { c.heartbeat = d }
}

func WithFetchBatch(batch int) ProcessOpt {
	return func(c *processConfig) { c.batch = batch }
}

func WithProcessMaxWait(d time.Duration) ProcessOpt {
	return func(c *processConfig) { c.maxWait = d }
}

// WithProcessHeartbeat sets PullHeartbeat used by Process fetch loops.
// When unset, Process uses RuntimeConsumerConfig.IdleHeartbeat if > 0.
func WithProcessHeartbeat(d time.Duration) ProcessOpt {
	return func(c *processConfig) { c.heartbeat = d }
}

// WithProcessConcurrency sets parallel handler goroutines per fetched batch (default 1).
func WithProcessConcurrency(n int) ProcessOpt {
	return func(c *processConfig) { c.concurrency = n }
}

type pullConsumer struct {
	js      natspkg.JetStreamContext
	sub     *natspkg.Subscription
	metrics *clientMetrics
	parent  *consumer
	mu      sync.Mutex
}

func (c *consumer) Pull(stream, durable string) (PullConsumer, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, fmt.Errorf("pull stream=%q: %w", stream, err)
	}

	var (
		sub *natspkg.Subscription
		err error
	)

	if !bytesconv.IsEmpty(durable) {
		if verr := ValidateDurableName(durable); verr != nil {
			return nil, fmt.Errorf("pull stream=%q durable=%q: %w", stream, durable, verr)
		}

		info, infoErr := c.js.ConsumerInfo(stream, durable)
		if infoErr != nil {
			return nil, fmt.Errorf("pull consumer info stream=%q durable=%q: %w", stream, durable, infoErr)
		}

		subject := consumerFilterSubject(info.Config)
		sub, err = c.js.PullSubscribe(subject, durable, natspkg.BindStream(stream))
	} else {
		sub, err = c.js.PullSubscribe(">", "", natspkg.BindStream(stream))
	}

	if err != nil {
		return nil, fmt.Errorf("pull subscribe stream=%q durable=%q: %w", stream, durable, err)
	}

	return &pullConsumer{js: c.js, sub: sub, metrics: c.metrics, parent: c}, nil
}

// Close unsubscribes the underlying pull subscription.
func (p *pullConsumer) Close() error {
	if p == nil {
		return nil
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sub == nil {
		return nil
	}
	err := p.sub.Unsubscribe()
	p.sub = nil
	return err
}

func (p *pullConsumer) subscription() (*natspkg.Subscription, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.sub == nil {
		return nil, fmt.Errorf("pull: subscription closed")
	}

	return p.sub, nil
}

func (p *pullConsumer) Fetch(ctx context.Context, batch int, opts ...FetchOpt) ([]*natspkg.Msg, error) {
	cfg := fetchConfig{maxWait: defaultFetchMaxWait}
	for _, opt := range opts {
		opt(&cfg)
	}

	if ctx == nil {
		ctx = context.Background()
	}

	fetchCtx := ctx
	var cancel context.CancelFunc
	if cfg.maxWait > 0 {
		fetchCtx, cancel = context.WithTimeout(ctx, cfg.maxWait)
		defer cancel()
	}

	// nats.go forbids Context and MaxWait together; timeout is via ctx deadline.
	fetchOpts := []natspkg.PullOpt{natspkg.Context(fetchCtx)}
	if cfg.heartbeat > 0 {
		hb := cfg.heartbeat
		if cfg.maxWait > 0 && hb >= cfg.maxWait {
			hb = cfg.maxWait / 2
		}
		if hb > 0 {
			fetchOpts = append(fetchOpts, natspkg.PullHeartbeat(hb))
		}
	}

	sub, subErr := p.subscription()
	if subErr != nil {
		return nil, subErr
	}

	start := time.Now()

	messages, err := sub.Fetch(batch, fetchOpts...)
	if p.metrics != nil {
		if p.metrics.fetchWaitTime != nil {
			p.metrics.fetchWaitTime.Record(ctx, time.Since(start).Seconds())
		}

		if p.metrics.fetchBatchSize != nil {
			p.metrics.fetchBatchSize.Record(ctx, float64(len(messages)))
		}

		if err != nil && errors.Is(err, natspkg.ErrNoHeartbeat) && p.metrics.idleHeartbeatMisses != nil {
			p.metrics.idleHeartbeatMisses.Add(ctx, 1)
		}
	}

	if err != nil {
		return messages, fmt.Errorf("pull fetch batch=%d: %w", batch, err)
	}

	return messages, nil
}

func (p *pullConsumer) FetchNoWait(batch int) ([]*natspkg.Msg, error) {
	sub, subErr := p.subscription()
	if subErr != nil {
		return nil, subErr
	}
	messages, err := sub.Fetch(batch, natspkg.MaxWait(0))
	if err != nil {
		return messages, fmt.Errorf("pull fetch no-wait batch=%d: %w", batch, err)
	}

	return messages, nil
}

func (p *pullConsumer) Process(ctx context.Context, handler MsgHandler, opts ...ProcessOpt) error {
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
		}

		messages, err := p.Fetch(ctx, fetchBatch, fetchOpts...)
		if err != nil {
			if errors.Is(err, natspkg.ErrTimeout) ||
				errors.Is(err, natspkg.ErrNoHeartbeat) ||
				errors.Is(err, context.DeadlineExceeded) {
				continue
			}

			return fmt.Errorf("pull process fetch: %w", err)
		}

		// Per-message handler errors are Nak'd inside processMessage; keep fetching.
		if err := p.processBatch(ctx, messages, handler, cfg); err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return err
			}
			if errors.Is(err, workerpool.ErrPoolStopped) {
				return err
			}
			zerolog.Ctx(ctx).Error().Err(err).Msg("pull process batch error; continuing")
		}
	}
}

func (p *pullConsumer) processBatch(ctx context.Context, messages []*natspkg.Msg, handler MsgHandler, cfg *processConfig) error {
	if len(messages) == 0 {
		return nil
	}

	batchStart := time.Now()

	if p.parent != nil && p.parent.cfg.WorkerPoolEnabled {
		if err := p.processBatchWithPool(ctx, messages, handler); err != nil {
			nakRemaining(messages)
			return err
		}
	} else if cfg.concurrency > 1 {
		if err := p.processBatchConcurrent(ctx, messages, handler, cfg.concurrency); err != nil {
			return err
		}
	} else {
		for i, msg := range messages {
			if ctx.Err() != nil {
				nakRemaining(messages[i:])
				return fmt.Errorf("pull process: %w", ctx.Err())
			}
			if err := p.parent.processMessage(ctx, msg, handler); err != nil {
				// Message already Nak'd; continue remaining of the batch.
				zerolog.Ctx(ctx).Error().Err(err).Str("subject", msg.Subject).Msg("pull process message")
			}
		}
	}

	if p.metrics != nil && p.metrics.pullBatchProcessTime != nil {
		p.metrics.pullBatchProcessTime.Record(ctx, time.Since(batchStart).Seconds())
	}

	return nil
}

func nakRemaining(messages []*natspkg.Msg) {
	for _, msg := range messages {
		if msg == nil {
			continue
		}
		if err := msg.Nak(); err != nil && !errors.Is(err, natspkg.ErrMsgAlreadyAckd) {
			// best-effort
			_ = err
		}
	}
}

func (p *pullConsumer) processBatchWithPool(ctx context.Context, messages []*natspkg.Msg, handler MsgHandler) error {
	p.parent.initWorkerPool()

	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error

	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	enqueue := func(msg *natspkg.Msg) error {
		wg.Add(1)
		accepted, err := p.parent.workerPool.TryPublishNonBlocking(ctx, msg, true, func(msgCtx context.Context, m *natspkg.Msg) error {
			defer wg.Done()

			if procErr := p.parent.processMessage(msgCtx, m, handler); procErr != nil {
				recordErr(procErr)

				return procErr
			}

			return nil
		})
		if err != nil {
			wg.Done()

			return err
		}
		if accepted {
			return nil
		}
		wg.Done()

		bpErr := p.parent.handlePoolBackpressure(ctx, msg)
		switch {
		case bpErr == nil, errors.Is(bpErr, ErrPoolFull):
			wg.Add(1)
			if pubErr := p.parent.workerPool.TryPublish(ctx, msg, true, func(msgCtx context.Context, m *natspkg.Msg) error {
				defer wg.Done()

				if procErr := p.parent.processMessage(msgCtx, m, handler); procErr != nil {
					recordErr(procErr)

					return procErr
				}

				return nil
			}); pubErr != nil {
				wg.Done()
				recordErr(pubErr)

				return pubErr
			}

			return nil
		case errors.Is(bpErr, ErrBackpressureHandled):
			return nil
		default:
			return bpErr
		}
	}

	for _, msg := range messages {
		if ctx.Err() != nil {
			recordErr(fmt.Errorf("pull process: %w", ctx.Err()))

			break
		}

		if err := enqueue(msg); err != nil {
			recordErr(err)

			break
		}
	}

	wg.Wait()

	if firstErr != nil {
		return firstErr
	}

	return nil
}

func (p *pullConsumer) processBatchConcurrent(ctx context.Context, messages []*natspkg.Msg, handler MsgHandler, concurrency int) error {
	if concurrency <= 1 {
		for _, msg := range messages {
			if err := p.parent.processMessage(ctx, msg, handler); err != nil {
				return fmt.Errorf("pull process subject=%q: %w", msg.Subject, err)
			}
		}

		return nil
	}

	if concurrency > len(messages) {
		concurrency = len(messages)
	}

	jobs := make(chan *natspkg.Msg, len(messages))
	var wg sync.WaitGroup
	var errMu sync.Mutex
	var firstErr error
	var inflight atomic.Int32

	recordErr := func(err error) {
		if err == nil {
			return
		}
		errMu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		errMu.Unlock()
	}

	worker := func() {
		defer wg.Done()
		for msg := range jobs {
			if ctx.Err() != nil {
				recordErr(ctx.Err())
				_ = msg.Nak()

				continue
			}

			current := inflight.Add(1)
			if p.metrics != nil && p.metrics.pullBatchInflight != nil {
				p.metrics.pullBatchInflight.Record(ctx, int64(current))
			}

			err := p.parent.processMessage(ctx, msg, handler)
			inflight.Add(-1)
			if err != nil {
				recordErr(fmt.Errorf("pull process subject=%q: %w", msg.Subject, err))
			}
		}
	}

	wg.Add(concurrency)
	for range concurrency {
		go worker()
	}

	for i, msg := range messages {
		errMu.Lock()
		hasErr := firstErr != nil
		errMu.Unlock()
		if hasErr || ctx.Err() != nil {
			nakRemaining(messages[i:])
			break
		}
		jobs <- msg
	}
	close(jobs)
	wg.Wait()

	// Per-message errors are already Nak'd; do not fail the whole Process loop.
	if ctx.Err() != nil {
		return fmt.Errorf("pull process: %w", ctx.Err())
	}

	return nil
}
