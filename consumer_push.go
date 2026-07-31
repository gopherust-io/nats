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

	"github.com/gopherust-io/nats/workerpool"
)

type Consumer interface {
	Subscribe(ctx context.Context, subject string, handler MsgHandler, opts ...natspkg.SubOpt) (Subscription, error)
	QueueSubscribe(ctx context.Context, queue, subject string, handler MsgHandler, opts ...natspkg.SubOpt) (Subscription, error)
	SubscribeBound(ctx context.Context, stream, durable, subject string, handler MsgHandler) (Subscription, error)
	QueueSubscribeBound(ctx context.Context, stream, durable, queue, subject string, handler MsgHandler) (Subscription, error)
	Pull(stream, durable string) (PullConsumer, error)
}

type Subscription interface {
	Unsubscribe() error
	Drain() error
	IsValid() bool
	Subject() string
	SetPendingLimits(msgLimit, bytesLimit int) error
	ConsumerInfo() (*natspkg.ConsumerInfo, error)
	Type() natspkg.SubscriptionType
}

type MsgHandler func(context.Context, *natspkg.Msg) error

// goalign:ignore
type consumer struct {
	ctx         context.Context
	js          natspkg.JetStreamContext
	cancel      context.CancelFunc
	metrics     *clientMetrics
	workerPool  *workerpool.WorkerPool
	depthTicker *time.Ticker
	depthStop   chan struct{}

	activityListeners atomic.Pointer[[]func()]
	handledListeners  atomic.Pointer[[]func(time.Duration)]
	cfg               RuntimeConsumerConfig
	backpressure      BackpressureConfig
	poolOnce          sync.Once
	allowTracing      bool
}

// OnProcessSuccess registers a callback invoked after a successful Ack or DLQ Term.
func (c *consumer) OnProcessSuccess(fn func()) {
	if c == nil || fn == nil {
		return
	}

	for {
		old := c.activityListeners.Load()
		var next []func()
		if old != nil {
			next = make([]func(), 0, len(*old)+1)
			next = append(next, (*old)...)
		} else {
			next = make([]func(), 0, 1)
		}
		next = append(next, fn)
		if c.activityListeners.CompareAndSwap(old, &next) {
			return
		}
	}
}

func (c *consumer) notifyProcessSuccess() {
	listeners := c.activityListeners.Load()
	if listeners == nil {
		return
	}

	for _, fn := range *listeners {
		fn()
	}
}

// OnMessageHandled registers a callback invoked after every handler completion
// (success or error) with the handler elapsed duration.
func (c *consumer) OnMessageHandled(fn func(elapsed time.Duration)) {
	if c == nil || fn == nil {
		return
	}

	for {
		old := c.handledListeners.Load()
		var next []func(time.Duration)
		if old != nil {
			next = make([]func(time.Duration), 0, len(*old)+1)
			next = append(next, (*old)...)
		} else {
			next = make([]func(time.Duration), 0, 1)
		}
		next = append(next, fn)
		if c.handledListeners.CompareAndSwap(old, &next) {
			return
		}
	}
}

func (c *consumer) notifyMessageHandled(elapsed time.Duration) {
	listeners := c.handledListeners.Load()
	if listeners == nil {
		return
	}

	for _, fn := range *listeners {
		fn(elapsed)
	}
}

func newConsumer(
	ctx context.Context,
	cfg RuntimeConsumerConfig,
	bp BackpressureConfig,
	js natspkg.JetStreamContext,
	metrics *clientMetrics,
	allowTracing bool,
) *consumer {
	c := &consumer{
		cfg:          cfg,
		backpressure: bp,
		js:           js,
		metrics:      metrics,
		allowTracing: allowTracing && cfg.AllowTracing,
	}
	c.ctx, c.cancel = context.WithCancel(ctx)

	if cfg.AckWait <= 0 {
		c.cfg.AckWait = defaultAckWait
	}

	if cfg.WorkerPoolEnabled && metrics != nil && bp.QueueDepthSampleInterval > 0 {
		c.startQueueDepthSampler(bp.QueueDepthSampleInterval)
	}

	return c
}

func (c *consumer) startQueueDepthSampler(interval time.Duration) {
	c.depthTicker = time.NewTicker(interval)
	c.depthStop = make(chan struct{})

	go func() {
		for {
			select {
			case <-c.depthStop:
				return
			case <-c.depthTicker.C:
				if c.workerPool != nil && c.metrics != nil && c.metrics.workerQueueDepth != nil {
					c.metrics.workerQueueDepth.Record(c.ctx, int64(c.workerPool.QueueDepth()))
				}
			}
		}
	}()
}

func (c *consumer) Subscribe(ctx context.Context, subject string, handler MsgHandler, opts ...natspkg.SubOpt) (Subscription, error) {
	if err := ValidateSubject(subject); err != nil {
		return nil, fmt.Errorf("subscribe: %w", err)
	}

	opts = c.appendConsumerOpts(opts, false)
	wrappedHandler := c.wrapHandler(ctx, handler)

	sub, err := c.js.Subscribe(subject, subWrap(ctx, wrappedHandler), opts...)
	if err != nil {
		return nil, fmt.Errorf("subscribe: %w", err)
	}

	c.applyPendingLimits(sub, subject, ctx)

	return &subscription{sub: sub}, nil
}

func (c *consumer) QueueSubscribe(
	ctx context.Context,
	queue, subject string,
	handler MsgHandler,
	opts ...natspkg.SubOpt,
) (Subscription, error) {
	if err := ValidateQueueName(queue); err != nil {
		return nil, fmt.Errorf("queue subscribe: %w", err)
	}

	if err := ValidateSubject(subject); err != nil {
		return nil, fmt.Errorf("queue subscribe: %w", err)
	}

	opts = c.appendConsumerOpts(opts, true)
	wrappedHandler := c.wrapHandler(ctx, handler)

	sub, err := c.js.QueueSubscribe(subject, queue, subWrap(ctx, wrappedHandler), opts...)
	if err != nil {
		return nil, fmt.Errorf("queue subscribe: %w", err)
	}

	c.applyPendingLimits(sub, subject, ctx)

	return &subscription{sub: sub}, nil
}

func (c *consumer) SubscribeBound(ctx context.Context, stream, durable, subject string, handler MsgHandler) (Subscription, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, fmt.Errorf("subscribe bound: %w", err)
	}

	if err := ValidateDurableName(durable); err != nil {
		return nil, fmt.Errorf("subscribe bound: %w", err)
	}

	sub, err := c.Subscribe(ctx, subject, handler, natspkg.BindStream(stream), natspkg.Durable(durable))
	if err != nil {
		return nil, err
	}

	drainOnCancel(ctx, sub)

	return sub, nil
}

func (c *consumer) QueueSubscribeBound(
	ctx context.Context,
	stream, durable, queue, subject string,
	handler MsgHandler,
) (Subscription, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, fmt.Errorf("queue subscribe bound: %w", err)
	}

	if err := ValidateDurableName(durable); err != nil {
		return nil, fmt.Errorf("queue subscribe bound: %w", err)
	}

	sub, err := c.QueueSubscribe(ctx, queue, subject, handler, natspkg.BindStream(stream), natspkg.Durable(durable))
	if err != nil {
		return nil, err
	}

	drainOnCancel(ctx, sub)

	return sub, nil
}

func (c *consumer) appendConsumerOpts(opts []natspkg.SubOpt, forQueue bool) []natspkg.SubOpt {
	opts = append(opts, natspkg.ManualAck())
	if c.cfg.AckWait > 0 {
		opts = append(opts, natspkg.AckWait(c.cfg.AckWait))
	}

	// Queue groups cannot use idle heartbeat or flow control (nats.go).
	if !forQueue && c.cfg.IdleHeartbeat > 0 {
		opts = append(opts, natspkg.IdleHeartbeat(c.cfg.IdleHeartbeat))
		if c.cfg.FlowControl {
			opts = append(opts, natspkg.EnableFlowControl())
		}
	}

	return opts
}

func (c *consumer) applyPendingLimits(sub *natspkg.Subscription, subject string, ctx context.Context) {
	limit := c.backpressure.PendingMsgLimit

	buf := c.backpressure.PendingMsgBuffer
	if limit == 0 && c.cfg.PendingMsgLimit > 0 {
		limit = c.cfg.PendingMsgLimit
	}

	if limit == 0 && c.backpressure.MaxAckPending > 0 {
		limit = c.backpressure.MaxAckPending
	}

	if buf == 0 && c.cfg.PendingMsgBuffer > 0 {
		buf = c.cfg.PendingMsgBuffer
	}

	if limit > 0 || buf > 0 {
		err := sub.SetPendingLimits(limit, buf)
		if err != nil {
			zerolog.Ctx(ctx).Error().
				Str("subject", subject).
				Err(err).
				Msg("set pending limits")
		}
	}
}

func subWrap(ctx context.Context, handler MsgHandler) natspkg.MsgHandler {
	return func(msg *natspkg.Msg) {
		_ = handler(ctx, msg)
	}
}

func (c *consumer) initWorkerPool(ctx context.Context, handler MsgHandler) {
	c.poolOnce.Do(func() {
		size := c.cfg.WorkerPoolSize
		if size <= 0 {
			size = defaultWorkerPoolSize
		}

		buf := c.cfg.WorkerBufferSize
		c.workerPool = workerpool.New(ctx, size, buf, func(msgCtx context.Context, msg *natspkg.Msg) error {
			return c.processMessage(msgCtx, msg, handler)
		})
		c.workerPool.Consume()
	})
}

func (c *consumer) wrapHandler(ctx context.Context, handler MsgHandler) MsgHandler {
	if !c.cfg.WorkerPoolEnabled {
		return func(msgCtx context.Context, msg *natspkg.Msg) error {
			return c.processMessage(msgCtx, msg, handler)
		}
	}

	return func(msgCtx context.Context, msg *natspkg.Msg) error {
		c.initWorkerPool(ctx, handler)

		mode := c.backpressure.Mode
		if mode == 0 {
			mode = BackpressureBlock
		}

		if mode == BackpressureNak {
			accepted, err := c.workerPool.TryPublishNonBlocking(msgCtx, msg, false, nil)
			if err != nil {
				return err
			}
			if !accepted {
				return c.handlePoolBackpressure(msgCtx, msg)
			}

			return nil
		}

		err := c.handlePoolBackpressure(msgCtx, msg)
		switch {
		case err == nil:
			// proceed to enqueue
		case errors.Is(err, ErrPoolFull):
			// block mode: still publish and let channel block
		case errors.Is(err, ErrBackpressureHandled):
			return nil
		default:
			return err
		}

		c.workerPool.Publish(msgCtx, msg, false, nil)

		return nil
	}
}

func (c *consumer) processMessage(ctx context.Context, msg *natspkg.Msg, handler MsgHandler) error {
	subject := msg.Subject

	var meta *natspkg.MsgMetadata
	if c.allowTracing || (c.metrics != nil && c.metrics.redeliveryTotal != nil) {
		meta = jetStreamMetadata(msg)
	}

	spanCtx, span := startProcessSpan(ctx, msg, c.allowTracing, meta)

	var handlerErr error

	defer endSpanPtr(span, &handlerErr)

	start := c.recordMessageMetrics(spanCtx, msg, meta)
	if start == 0 {
		start = time.Now().UnixNano()
	}

	err := handler(spanCtx, msg)
	handlerErr = err

	elapsed := time.Duration(time.Now().UnixNano() - start)
	if c.metrics != nil && c.metrics.handlingTime != nil {
		c.metrics.handlingTime.RecordWith(spanCtx, elapsed.Seconds(), c.metricSubject(subject))
	}
	c.notifyMessageHandled(elapsed)

	if errors.Is(err, ErrDLQRouted) {
		handlerErr = nil
		if c.metrics != nil && c.metrics.termTotal != nil {
			c.metrics.termTotal.AddWith(ctx, 1, c.metricSubject(subject))
		}
		c.notifyProcessSuccess()

		return nil
	}

	if err != nil {
		c.recordProcessError(ctx, subject, msg, err)

		return fmt.Errorf("message processing failed subject=%q: %w", subject, err)
	}

	ackErr := msg.Ack()
	if ackErr != nil {
		zerolog.Ctx(ctx).Error().Err(ackErr).Msg("ack message")

		return fmt.Errorf("ack message subject=%q: %w", subject, ackErr)
	}

	if c.metrics != nil && c.metrics.ackTotal != nil {
		c.metrics.ackTotal.AddWith(ctx, 1, c.metricSubject(subject))
	}
	c.notifyProcessSuccess()

	return nil
}

func (c *consumer) metricSubject(subject string) string {
	return metricSubjectLabel(c.metrics, subject)
}

func (c *consumer) recordProcessError(ctx context.Context, subject string, msg *natspkg.Msg, err error) {
	if c.metrics != nil {
		label := c.metricSubject(subject)
		if c.metrics.messagesErrors != nil {
			c.metrics.messagesErrors.AddWith(ctx, 1, label)
		}

		if c.metrics.nakTotal != nil {
			c.metrics.nakTotal.AddWith(ctx, 1, label)
		}
	}

	zerolog.Ctx(ctx).Error().
		Str("subject", subject).
		Err(err).
		Msg("process message")

	nakErr := msg.Nak()
	if nakErr != nil {
		zerolog.Ctx(ctx).Error().Err(nakErr).Msg("nak message")
	}
}

func (c *consumer) recordMessageMetrics(ctx context.Context, msg *natspkg.Msg, meta *natspkg.MsgMetadata) int64 {
	if c.metrics == nil {
		return 0
	}

	var start int64
	if c.metrics.handlingTime != nil {
		start = time.Now().UnixNano()
	}

	subject := c.metricSubject(msg.Subject)
	if c.metrics.redeliveryTotal != nil && meta != nil && meta.NumDelivered > 1 {
		c.metrics.redeliveryTotal.AddWith(ctx, int64(meta.NumDelivered-1), subject)
	}

	if c.metrics.messageBytes != nil {
		c.metrics.messageBytes.RecordWith(ctx, float64(len(msg.Data)), subject)
	}

	if c.metrics.messagesReceived != nil {
		c.metrics.messagesReceived.AddWith(ctx, 1, subject)
	}

	return start
}

func (c *consumer) stop() {
	if c.depthTicker != nil {
		close(c.depthStop)
		c.depthTicker.Stop()
	}

	if c.workerPool != nil {
		c.workerPool.GracefulStop()
	}

	if c.cancel != nil {
		c.cancel()
	}
}

type subscription struct {
	sub *natspkg.Subscription
}

func (s *subscription) Unsubscribe() error {
	if s.sub == nil {
		return ErrInvalidSubscription
	}

	return s.sub.Unsubscribe()
}

func (s *subscription) IsValid() bool {
	return s.sub != nil && s.sub.IsValid()
}

func (s *subscription) Subject() string {
	if s.sub == nil {
		return empty
	}

	return s.sub.Subject
}

func (s *subscription) Drain() error {
	if s.sub == nil {
		return ErrInvalidSubscription
	}

	return s.sub.Drain()
}

func (s *subscription) SetPendingLimits(msgLimit, bytesLimit int) error {
	if s.sub == nil {
		return ErrInvalidSubscription
	}

	return s.sub.SetPendingLimits(msgLimit, bytesLimit)
}

func (s *subscription) ConsumerInfo() (*natspkg.ConsumerInfo, error) {
	if s.sub == nil {
		return nil, ErrInvalidSubscription
	}

	return s.sub.ConsumerInfo()
}

func (s *subscription) Type() natspkg.SubscriptionType {
	if s.sub == nil {
		return -1
	}

	return s.sub.Type()
}

func drainOnCancel(ctx context.Context, sub Subscription) {
	go func() {
		<-ctx.Done()

		_ = sub.Drain()
	}()
}

// HandlerTyped wraps a typed handler with decode logic.
func HandlerTyped[T any](mt MessageType, fn func(ctx context.Context, subject string, payload T) error) MsgHandler {
	return func(ctx context.Context, msg *natspkg.Msg) error {
		payload, err := DecodeTyped[T](msg, mt)
		if err != nil {
			return err
		}

		return fn(ctx, msg.Subject, payload)
	}
}

// DecodeTyped is a generic helper for typed message handlers.
func DecodeTyped[T any](msg *natspkg.Msg, typ MessageType) (T, error) {
	var zero T

	if typ == 0 {
		typ = MessageTypeFromHeader(msg.Header)
	}

	var dst T

	err := DecodeMsg(msg, typ, &dst)
	if err != nil {
		return zero, err
	}

	return dst, nil
}
