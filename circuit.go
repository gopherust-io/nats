package nats

import (
	"sync/atomic"
	"time"
)

// CircuitConfig enables degrade / pause modes when Adaptive Pressure hits critical.
//
// goalign:ignore
type CircuitConfig struct {
	Enabled       bool
	PauseDuration time.Duration
	// OpenOnLevel opens the circuit at this AdaptivePressureLevel (default PressureCritical).
	OpenOnLevel AdaptivePressureLevel
}

func (c CircuitConfig) withDefaults() CircuitConfig {
	out := c
	if out.PauseDuration <= 0 {
		out.PauseDuration = time.Second
	}
	if out.OpenOnLevel == 0 {
		out.OpenOnLevel = PressureCritical
	}

	return out
}

// CircuitBreaker pauses pull fetches when pressure is critical (metric: circuit_open).
type CircuitBreaker struct {
	cfg    CircuitConfig
	open   atomic.Bool
	opens  atomic.Int64
}

// NewCircuitBreaker returns a breaker or nil when disabled.
func NewCircuitBreaker(cfg CircuitConfig) *CircuitBreaker {
	if !cfg.Enabled {
		return nil
	}

	return &CircuitBreaker{cfg: cfg.withDefaults()}
}

// ShouldPause reports whether pull Process should delay the next fetch.
func (b *CircuitBreaker) ShouldPause(d AdaptivePressureDecision) bool {
	if b == nil {
		return false
	}
	open := d.Level >= b.cfg.OpenOnLevel
	was := b.open.Swap(open)
	if open && !was {
		b.opens.Add(1)
	}

	return open
}

// PauseDuration is how long Process sleeps while the circuit is open.
func (b *CircuitBreaker) PauseDuration() time.Duration {
	if b == nil {
		return 0
	}

	return b.cfg.PauseDuration
}

// Opens returns how many times the circuit transitioned to open.
func (b *CircuitBreaker) Opens() int64 {
	if b == nil {
		return 0
	}

	return b.opens.Load()
}

// IsOpen reports the last observed open state.
func (b *CircuitBreaker) IsOpen() bool {
	return b != nil && b.open.Load()
}
