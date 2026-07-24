package nats

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"
)

const (
	defaultSoftLivenessPollInterval  = 2 * time.Second
	defaultSoftLivenessStallAfter    = 10 * time.Second
	defaultSoftLivenessRisingWindows = 3
	softLivenessEventBuffer          = 16
)

// SoftLivenessEvent is emitted when backlog grows without successful processing.
type SoftLivenessEvent struct {
	Err        error
	NumPending uint64
	StalledFor time.Duration
}

// SoftLivenessConfig controls WatchSoftLiveness.
type SoftLivenessConfig struct {
	OnStall func(SoftLivenessEvent)
	// PollInterval is how often ConsumerInfo is polled (default 2s).
	PollInterval time.Duration
	// StallAfter is the minimum time since last successful process before a stall can fire (default 10s).
	StallAfter time.Duration
	// RisingWindows is consecutive polls with rising NumPending required (default 3).
	RisingWindows int
	// CircuitStop stops the watcher after the first stall (apps may then restart the pod).
	CircuitStop bool
}

func (c SoftLivenessConfig) withDefaults() SoftLivenessConfig {
	out := c
	if out.PollInterval <= 0 {
		out.PollInterval = defaultSoftLivenessPollInterval
	}

	if out.StallAfter <= 0 {
		out.StallAfter = defaultSoftLivenessStallAfter
	}

	if out.RisingWindows <= 0 {
		out.RisingWindows = defaultSoftLivenessRisingWindows
	}

	return out
}

// ProcessActivity tracks the last successful message process (Ack or DLQ Term).
type ProcessActivity struct {
	lastUnixNano atomic.Int64
}

// NewProcessActivity creates a tracker seeded to now.
func NewProcessActivity() *ProcessActivity {
	a := &ProcessActivity{}
	a.Touch()

	return a
}

// Touch records a successful process at the current time.
func (a *ProcessActivity) Touch() {
	if a == nil {
		return
	}

	a.lastUnixNano.Store(time.Now().UnixNano())
}

// LastSuccess returns the time of the last successful process.
func (a *ProcessActivity) LastSuccess() time.Time {
	if a == nil {
		return time.Time{}
	}

	n := a.lastUnixNano.Load()
	if n == 0 {
		return time.Time{}
	}

	return time.Unix(0, n)
}

// SoftLiveness watches a subscription for backlog growth without processing activity.
type SoftLiveness struct {
	sub      Subscription
	events   chan SoftLivenessEvent
	stopCh   chan struct{}
	activity *ProcessActivity
	metrics  *clientMetrics
	cfg      SoftLivenessConfig
	stopOnce sync.Once
	stalled  atomic.Bool
}

// WatchSoftLiveness starts polling sub.ConsumerInfo for stall conditions.
// Register activity via SoftLiveness.Activity() and consumer.OnProcessSuccess, or call Touch manually.
func WatchSoftLiveness(
	ctx context.Context,
	sub Subscription,
	activity *ProcessActivity,
	cfg SoftLivenessConfig,
	metrics *clientMetrics,
) (*SoftLiveness, error) {
	if sub == nil {
		return nil, fmt.Errorf("soft liveness: subscription is nil")
	}

	if activity == nil {
		activity = NewProcessActivity()
	}

	cfg = cfg.withDefaults()
	sl := &SoftLiveness{
		sub:      sub,
		activity: activity,
		cfg:      cfg,
		metrics:  metrics,
		events:   make(chan SoftLivenessEvent, softLivenessEventBuffer),
		stopCh:   make(chan struct{}),
	}

	go sl.loop(ctx)

	return sl, nil
}

// Activity returns the process-activity tracker used by this watcher.
func (s *SoftLiveness) Activity() *ProcessActivity { return s.activity }

// Events returns stall notifications (buffered; overflows are dropped).
func (s *SoftLiveness) Events() <-chan SoftLivenessEvent { return s.events }

// Stalled reports whether a stall has been detected (sticky until Stop).
func (s *SoftLiveness) Stalled() bool { return s.stalled.Load() }

// Stop ends the watch loop.
func (s *SoftLiveness) Stop() {
	s.stopOnce.Do(func() { close(s.stopCh) })
}

func (s *SoftLiveness) loop(ctx context.Context) {
	ticker := time.NewTicker(s.cfg.PollInterval)
	defer ticker.Stop()

	var (
		prevPending uint64
		havePrev    bool
		rising      int
	)

	for {
		select {
		case <-ctx.Done():
			return
		case <-s.stopCh:
			return
		case <-ticker.C:
			info, err := s.sub.ConsumerInfo()
			if err != nil || info == nil {
				continue
			}

			pending := info.NumPending
			if havePrev && pending > prevPending {
				rising++
			} else {
				rising = 0
			}

			prevPending = pending
			havePrev = true

			last := s.activity.LastSuccess()
			idle := time.Since(last)
			if rising < s.cfg.RisingWindows || idle < s.cfg.StallAfter || pending == 0 {
				continue
			}

			ev := SoftLivenessEvent{
				NumPending: pending,
				StalledFor: idle,
				Err:        ErrConsumerStall,
			}
			s.stalled.Store(true)
			s.emit(ev)

			if s.metrics != nil && s.metrics.consumerStall != nil {
				s.metrics.consumerStall.Add(ctx, 1)
			}

			slog.WarnContext(ctx, "soft liveness stall",
				slog.Uint64("num_pending", pending),
				slog.Duration("stalled_for", idle))

			if s.cfg.CircuitStop {
				return
			}

			// Reset rising window so we don't spam every poll; still sticky Stalled().
			rising = 0
		}
	}
}

func (s *SoftLiveness) emit(ev SoftLivenessEvent) {
	if s.cfg.OnStall != nil {
		s.cfg.OnStall(ev)
	}

	select {
	case s.events <- ev:
	default:
	}
}

// WatchSoftLiveness is a client helper that also hooks process-success on the shared consumer.
func (c *client) WatchSoftLiveness(
	ctx context.Context,
	sub Subscription,
	cfg SoftLivenessConfig,
) (*SoftLiveness, error) {
	activity := NewProcessActivity()
	sl, err := WatchSoftLiveness(ctx, sub, activity, cfg, c.metrics)
	if err != nil {
		return nil, err
	}

	if c.consumer != nil {
		c.consumer.OnProcessSuccess(activity.Touch)
	}

	return sl, nil
}
