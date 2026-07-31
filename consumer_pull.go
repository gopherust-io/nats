package nats

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gopherust-io/nats/internal/bytesconv"
	natspkg "github.com/nats-io/nats.go"
)

type PullConsumer interface {
	Fetch(ctx context.Context, batch int, opts ...FetchOpt) ([]*natspkg.Msg, error)
	FetchNoWait(batch int) ([]*natspkg.Msg, error)
	Process(ctx context.Context, handler MsgHandler, opts ...ProcessOpt) error
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
		sub, err = c.js.PullSubscribe(stream, "")
	}

	if err != nil {
		return nil, fmt.Errorf("pull subscribe stream=%q durable=%q: %w", stream, durable, err)
	}

	return &pullConsumer{js: c.js, sub: sub, metrics: c.metrics, parent: c}, nil
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

	start := time.Now()

	messages, err := p.sub.Fetch(batch, fetchOpts...)
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
	messages, err := p.sub.Fetch(batch, natspkg.MaxWait(0))
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

		messages, err := p.Fetch(ctx, cfg.batch, fetchOpts...)
		if err != nil {
			if errors.Is(err, natspkg.ErrTimeout) ||
				errors.Is(err, natspkg.ErrNoHeartbeat) ||
				errors.Is(err, context.DeadlineExceeded) {
				continue
			}

			return fmt.Errorf("pull process fetch: %w", err)
		}

		if err := p.processBatch(ctx, messages, handler, cfg); err != nil {
			return err
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
			return err
		}
	} else if cfg.concurrency > 1 {
		if err := p.processBatchConcurrent(ctx, messages, handler, cfg.concurrency); err != nil {
			return err
		}
	} else {
		for _, msg := range messages {
			if err := p.parent.processMessage(ctx, msg, handler); err != nil {
				return fmt.Errorf("pull process subject=%q: %w", msg.Subject, err)
			}
		}
	}

	if p.metrics != nil && p.metrics.pullBatchProcessTime != nil {
		p.metrics.pullBatchProcessTime.Record(ctx, time.Since(batchStart).Seconds())
	}

	return nil
}

func (p *pullConsumer) processBatchWithPool(ctx context.Context, messages []*natspkg.Msg, handler MsgHandler) error {
	p.parent.initWorkerPool(ctx, handler)

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

	for _, msg := range messages {
		if ctx.Err() != nil {
			return fmt.Errorf("pull process: %w", ctx.Err())
		}

		wg.Add(1)
		msg := msg

		accepted, err := p.parent.workerPool.TryPublishNonBlocking(ctx, msg, true, func(msgCtx context.Context, m *natspkg.Msg) error {
			defer wg.Done()

			if err := p.parent.processMessage(msgCtx, m, handler); err != nil {
				recordErr(err)

				return err
			}

			return nil
		})
		if err != nil {
			wg.Done()
			return err
		}
		if !accepted {
			wg.Done()

			bpErr := p.parent.handlePoolBackpressure(ctx, msg)
			switch {
			case bpErr == nil:
				wg.Add(1)
				p.parent.workerPool.Publish(ctx, msg, true, func(msgCtx context.Context, m *natspkg.Msg) error {
					defer wg.Done()

					if err := p.parent.processMessage(msgCtx, m, handler); err != nil {
						recordErr(err)

						return err
					}

					return nil
				})
			case errors.Is(bpErr, ErrPoolFull):
				wg.Add(1)
				p.parent.workerPool.Publish(ctx, msg, true, func(msgCtx context.Context, m *natspkg.Msg) error {
					defer wg.Done()

					if err := p.parent.processMessage(msgCtx, m, handler); err != nil {
						recordErr(err)

						return err
					}

					return nil
				})
			case errors.Is(bpErr, ErrBackpressureHandled):
				continue
			default:
				return bpErr
			}
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

	for _, msg := range messages {
		errMu.Lock()
		hasErr := firstErr != nil
		errMu.Unlock()
		if hasErr || ctx.Err() != nil {
			break
		}
		jobs <- msg
	}
	close(jobs)
	wg.Wait()

	if firstErr != nil {
		return firstErr
	}

	if ctx.Err() != nil {
		return fmt.Errorf("pull process: %w", ctx.Err())
	}

	return nil
}
