package nats

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	natspkg "github.com/nats-io/nats.go"
)

const (
	defaultSupervisorMaxRetries     = 10
	defaultSupervisorInitialBackoff = time.Second
	defaultSupervisorMaxBackoff     = 30 * time.Second
	defaultSupervisorCheckInterval  = time.Second
	supervisorEventBuffer           = 16
)

// SupervisorEventKind classifies supervisor lifecycle events.
type SupervisorEventKind uint8

const (
	SupervisorResubscribed SupervisorEventKind = iota + 1
	SupervisorGiveUp
	SupervisorInvalid
)

// SupervisorEvent is emitted when a supervised subscription is healed or abandoned.
// goalign:ignore
type SupervisorEvent struct {
	Err     error
	Kind    SupervisorEventKind
	Attempt int
}

type SupervisorConfig struct {
	OnEvent        func(SupervisorEvent)
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	CheckInterval  time.Duration
	// HealthyBackoffInterval is the maximum poll interval after consecutive healthy checks (0 disables).
	HealthyBackoffInterval time.Duration
	// HealthyChecksBeforeBackoff is the number of healthy checks before increasing the poll interval.
	HealthyChecksBeforeBackoff int
	// MaxRetries is resubscribe attempts after interest is lost.
	// 0 means default (10). -1 means unlimited.
	MaxRetries int
}

func (c SupervisorConfig) withDefaults() SupervisorConfig {
	out := c
	if out.MaxRetries == 0 {
		out.MaxRetries = defaultSupervisorMaxRetries
	}

	if out.InitialBackoff <= 0 {
		out.InitialBackoff = defaultSupervisorInitialBackoff
	}

	if out.MaxBackoff <= 0 {
		out.MaxBackoff = defaultSupervisorMaxBackoff
	}

	if out.CheckInterval <= 0 {
		out.CheckInterval = defaultSupervisorCheckInterval
	}

	return out
}

// SubscribeFn creates a push subscription (e.g. QueueSubscribeBound).
type SubscribeFn func(ctx context.Context) (Subscription, error)

// SupervisedSubscription is a push subscription that auto-resubscribes when invalid.
type SupervisedSubscription interface {
	Subscription
	Events() <-chan SupervisorEvent
	Stop() error
}

type supervisedSubscription struct {
	sub     Subscription
	events  chan SupervisorEvent
	stopCh  chan struct{}
	metrics *clientMetrics
	cfg     SupervisorConfig

	stopOnce sync.Once

	mu sync.Mutex
}

// Supervise runs subscribe and keeps the subscription alive by resubscribing when
// IsValid() becomes false (e.g. after idle-heartbeat miss / ErrConsumerNotActive).
func Supervise(
	ctx context.Context,
	cfg SupervisorConfig,
	metrics *clientMetrics,
	subscribe SubscribeFn,
) (SupervisedSubscription, error) {
	if subscribe == nil {
		return nil, fmt.Errorf("supervise: subscribe fn is nil")
	}

	cfg = cfg.withDefaults()

	sub, err := subscribe(ctx)
	if err != nil {
		return nil, fmt.Errorf("supervise initial subscribe: %w", err)
	}

	s := &supervisedSubscription{
		sub:     sub,
		cfg:     cfg,
		metrics: metrics,
		events:  make(chan SupervisorEvent, supervisorEventBuffer),
		stopCh:  make(chan struct{}),
	}

	go s.loop(ctx, subscribe)

	return s, nil
}

func (s *supervisedSubscription) loop(ctx context.Context, subscribe SubscribeFn) {
	baseInterval := s.cfg.CheckInterval
	currentInterval := baseInterval
	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	attempts := 0
	backoff := s.cfg.InitialBackoff
	healthyStreak := 0

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			s.mu.Lock()
			valid := s.sub != nil && s.sub.IsValid()
			s.mu.Unlock()

			if valid {
				attempts = 0
				backoff = s.cfg.InitialBackoff

				if s.cfg.HealthyBackoffInterval > 0 && s.cfg.HealthyChecksBeforeBackoff > 0 {
					healthyStreak++
					if healthyStreak >= s.cfg.HealthyChecksBeforeBackoff && currentInterval < s.cfg.HealthyBackoffInterval {
						next := currentInterval * 2
						if next > s.cfg.HealthyBackoffInterval {
							next = s.cfg.HealthyBackoffInterval
						}
						if next != currentInterval {
							currentInterval = next
							ticker.Reset(currentInterval)
						}
					}
				}

				continue
			}

			healthyStreak = 0
			if currentInterval != baseInterval {
				currentInterval = baseInterval
				ticker.Reset(currentInterval)
			}

			s.emit(SupervisorEvent{Kind: SupervisorInvalid, Attempt: attempts})

			if s.cfg.MaxRetries >= 0 && attempts >= s.cfg.MaxRetries {
				s.emit(SupervisorEvent{
					Kind:    SupervisorGiveUp,
					Attempt: attempts,
					Err:     ErrSupervisorGiveUp,
				})
				if s.metrics != nil && s.metrics.supervisorGiveUp != nil {
					s.metrics.supervisorGiveUp.Add(ctx, 1)
				}

				return
			}

			attempts++

			select {
			case <-ctx.Done():
				return
			case <-s.stopCh:
				return
			case <-time.After(backoff):
			}

			newSub, err := subscribe(ctx)
			if err != nil {
				slog.WarnContext(ctx, "supervisor resubscribe failed",
					slog.Int("attempt", attempts),
					slog.String("err", err.Error()))
				backoff = nextBackoff(backoff, s.cfg.MaxBackoff)

				continue
			}

			s.mu.Lock()
			old := s.sub
			s.sub = newSub
			s.mu.Unlock()

			if old != nil {
				_ = old.Unsubscribe()
			}

			if s.metrics != nil && s.metrics.resubscribeTotal != nil {
				s.metrics.resubscribeTotal.Add(ctx, 1)
			}

			s.emit(SupervisorEvent{Kind: SupervisorResubscribed, Attempt: attempts})
			backoff = s.cfg.InitialBackoff
		}
	}
}

func nextBackoff(current, maxBackoff time.Duration) time.Duration {
	next := current * 2
	if next > maxBackoff {
		return maxBackoff
	}

	return next
}

func (s *supervisedSubscription) emit(ev SupervisorEvent) {
	if s.cfg.OnEvent != nil {
		s.cfg.OnEvent(ev)
	}
	trySend(s.events, ev)
}

func (s *supervisedSubscription) Events() <-chan SupervisorEvent { return s.events }

func (s *supervisedSubscription) Stop() error {
	var err error

	s.stopOnce.Do(func() {
		close(s.stopCh)
		s.mu.Lock()
		sub := s.sub
		s.mu.Unlock()
		if sub != nil {
			err = sub.Unsubscribe()
		}
	})

	return err
}

func (s *supervisedSubscription) current() Subscription {
	s.mu.Lock()
	defer s.mu.Unlock()

	return s.sub
}

func (s *supervisedSubscription) Unsubscribe() error { return s.Stop() }

func (s *supervisedSubscription) Drain() error {
	if sub := s.current(); sub != nil {
		return sub.Drain()
	}

	return ErrInvalidSubscription
}

func (s *supervisedSubscription) IsValid() bool {
	sub := s.current()

	return sub != nil && sub.IsValid()
}

func (s *supervisedSubscription) Subject() string {
	if sub := s.current(); sub != nil {
		return sub.Subject()
	}

	return empty
}

func (s *supervisedSubscription) SetPendingLimits(msgLimit, bytesLimit int) error {
	if sub := s.current(); sub != nil {
		return sub.SetPendingLimits(msgLimit, bytesLimit)
	}

	return ErrInvalidSubscription
}

func (s *supervisedSubscription) ConsumerInfo() (*natspkg.ConsumerInfo, error) {
	if sub := s.current(); sub != nil {
		return sub.ConsumerInfo()
	}

	return nil, ErrInvalidSubscription
}

func (s *supervisedSubscription) Type() natspkg.SubscriptionType {
	if sub := s.current(); sub != nil {
		return sub.Type()
	}

	return natspkg.SubscriptionType(-1)
}

// SuperviseQueueSubscribeBound subscribes with auto-resubscribe on invalid interest.
func (c *client) SuperviseQueueSubscribeBound(
	ctx context.Context,
	stream, durable, queue, subject string,
	handler MsgHandler,
	cfg SupervisorConfig,
) (SupervisedSubscription, error) {
	return Supervise(ctx, cfg, c.metrics, func(subCtx context.Context) (Subscription, error) {
		return c.consumer.QueueSubscribeBound(subCtx, stream, durable, queue, subject, handler)
	})
}

// SuperviseSubscribeBound subscribes (non-queue) with auto-resubscribe on invalid interest.
func (c *client) SuperviseSubscribeBound(
	ctx context.Context,
	stream, durable, subject string,
	handler MsgHandler,
	cfg SupervisorConfig,
) (SupervisedSubscription, error) {
	return Supervise(ctx, cfg, c.metrics, func(subCtx context.Context) (Subscription, error) {
		return c.consumer.SubscribeBound(subCtx, stream, durable, subject, handler)
	})
}

// SupervisePullProcess runs Pull.Process in a loop, restarting after failures with backoff.
func SupervisePullProcess(
	ctx context.Context,
	cfg SupervisorConfig,
	metrics *clientMetrics,
	run func(context.Context) error,
) error {
	if run == nil {
		return fmt.Errorf("supervise pull: run fn is nil")
	}

	cfg = cfg.withDefaults()
	attempts := 0
	backoff := cfg.InitialBackoff

	for {
		err := run(ctx)
		if err == nil || errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		if cfg.MaxRetries >= 0 && attempts >= cfg.MaxRetries {
			if metrics != nil && metrics.supervisorGiveUp != nil {
				metrics.supervisorGiveUp.Add(ctx, 1)
			}
			if cfg.OnEvent != nil {
				cfg.OnEvent(SupervisorEvent{Kind: SupervisorGiveUp, Attempt: attempts, Err: err})
			}

			return fmt.Errorf("%w: %w", ErrSupervisorGiveUp, err)
		}

		attempts++
		if metrics != nil && metrics.resubscribeTotal != nil {
			metrics.resubscribeTotal.Add(ctx, 1)
		}
		if cfg.OnEvent != nil {
			cfg.OnEvent(SupervisorEvent{Kind: SupervisorResubscribed, Attempt: attempts, Err: err})
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}

		backoff = nextBackoff(backoff, cfg.MaxBackoff)
	}
}

// SupervisePullProcess is a client helper around Consumer().Pull(...).Process.
func (c *client) SupervisePullProcess(
	ctx context.Context,
	stream, durable string,
	handler MsgHandler,
	cfg SupervisorConfig,
	opts ...ProcessOpt,
) error {
	return SupervisePullProcess(ctx, cfg, c.metrics, func(runCtx context.Context) error {
		pull, err := c.consumer.Pull(stream, durable)
		if err != nil {
			return err
		}

		return pull.Process(runCtx, handler, opts...)
	})
}
