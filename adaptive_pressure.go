package nats

import (
	"sync/atomic"
	"time"
)

// AdaptivePressureLevel is the control-loop severity (0=idle … 4=critical).
type AdaptivePressureLevel uint8

const (
	PressureIdle AdaptivePressureLevel = iota
	PressureWarm
	PressureElevated
	PressureHard
	PressureCritical
)

// AdaptivePressureConfig enables closed-loop fetch/backpressure tuning.
//
// goalign:ignore
type AdaptivePressureConfig struct {
	Enabled       bool
	MinFetchBatch int
	MaxFetchBatch int
	MinAckPending int
	NakDelayMin   time.Duration
	NakDelayMax   time.Duration
}

func (c AdaptivePressureConfig) withDefaults() AdaptivePressureConfig {
	out := c
	if out.MinFetchBatch <= 0 {
		out.MinFetchBatch = 1
	}
	if out.MaxFetchBatch <= 0 {
		out.MaxFetchBatch = 256
	}
	if out.MinAckPending <= 0 {
		out.MinAckPending = 50
	}
	if out.NakDelayMin <= 0 {
		out.NakDelayMin = 100 * time.Millisecond
	}
	if out.NakDelayMax <= 0 {
		out.NakDelayMax = 5 * time.Second
	}
	if out.MaxFetchBatch < out.MinFetchBatch {
		out.MaxFetchBatch = out.MinFetchBatch
	}

	return out
}

// AdaptivePressureInput is one observation sample for the controller.
//
// goalign:ignore
type AdaptivePressureInput struct {
	PoolDepth       int
	PoolCapacity    int
	HandlerLatency  time.Duration
	BaselineLatency time.Duration
	Lag             uint64
	Pending         uint64
	CurrentBatch    int
	CurrentAckPend  int
	MaxAckPending   int
}

// AdaptivePressureDecision is the actuator output.
//
// goalign:ignore
type AdaptivePressureDecision struct {
	Level      AdaptivePressureLevel
	FetchBatch int
	AckPending int
	NakDelay   time.Duration
}

// DecideAdaptivePressure is a pure control step (unit-testable).
func DecideAdaptivePressure(cfg AdaptivePressureConfig, in AdaptivePressureInput) AdaptivePressureDecision {
	cfg = cfg.withDefaults()
	level := PressureIdle

	fill := 0.0
	if in.PoolCapacity > 0 {
		fill = float64(in.PoolDepth) / float64(in.PoolCapacity)
	}
	latencyFactor := 1.0
	if in.BaselineLatency > 0 && in.HandlerLatency > 0 {
		latencyFactor = float64(in.HandlerLatency) / float64(in.BaselineLatency)
	}

	switch {
	case fill >= 0.95 || latencyFactor >= 5 || in.Lag > 10_000:
		level = PressureCritical
	case fill >= 0.7 || latencyFactor >= 3 || in.Pending > 5_000:
		level = PressureHard
	case fill >= 0.4 || latencyFactor >= 1.5 || in.Lag > 500:
		level = PressureElevated
	case fill >= 0.15 || in.Lag > 50:
		level = PressureWarm
	}

	batch := in.CurrentBatch
	if batch <= 0 {
		batch = cfg.MaxFetchBatch
	}
	ack := in.CurrentAckPend
	if ack <= 0 {
		ack = in.MaxAckPending
	}
	if ack <= 0 {
		ack = cfg.MinAckPending * 4
	}

	var nak time.Duration
	switch level {
	case PressureIdle:
		// Without a worker pool (or other load signal), do not ratchet batch upward.
		if in.PoolCapacity > 0 {
			batch = min(cfg.MaxFetchBatch, batch*2)
		}
		if batch < cfg.MinFetchBatch {
			batch = cfg.MinFetchBatch
		}
		if in.MaxAckPending > 0 {
			ack = in.MaxAckPending
		}
	case PressureWarm:
		// hold
	case PressureElevated:
		batch = max(cfg.MinFetchBatch, batch/2)
		nak = cfg.NakDelayMin
	case PressureHard:
		batch = cfg.MinFetchBatch
		ack = max(cfg.MinAckPending, ack/2)
		nak = cfg.NakDelayMin * 2
		if nak > cfg.NakDelayMax {
			nak = cfg.NakDelayMax
		}
	case PressureCritical:
		batch = cfg.MinFetchBatch
		ack = cfg.MinAckPending
		nak = cfg.NakDelayMax
	}

	if batch > cfg.MaxFetchBatch {
		batch = cfg.MaxFetchBatch
	}
	if batch < cfg.MinFetchBatch {
		batch = cfg.MinFetchBatch
	}

	return AdaptivePressureDecision{
		Level:      level,
		FetchBatch: batch,
		AckPending: ack,
		NakDelay:   nak,
	}
}

// AdaptivePressure holds the latest decision for pull/push actuators.
//
// goalign:ignore
type AdaptivePressure struct {
	cfg      AdaptivePressureConfig
	decision atomic.Value // AdaptivePressureDecision
}

func NewAdaptivePressure(cfg AdaptivePressureConfig) *AdaptivePressure {
	ap := &AdaptivePressure{cfg: cfg.withDefaults()}
	ap.decision.Store(AdaptivePressureDecision{
		Level:      PressureIdle,
		FetchBatch: ap.cfg.MaxFetchBatch,
		AckPending: 0,
	})

	return ap
}

func (a *AdaptivePressure) Observe(in AdaptivePressureInput) AdaptivePressureDecision {
	if a == nil || !a.cfg.Enabled {
		return AdaptivePressureDecision{}
	}
	d := DecideAdaptivePressure(a.cfg, in)
	a.decision.Store(d)

	return d
}

func (a *AdaptivePressure) Decision() AdaptivePressureDecision {
	if a == nil {
		return AdaptivePressureDecision{}
	}
	if v := a.decision.Load(); v != nil {
		if d, ok := v.(AdaptivePressureDecision); ok {
			return d
		}
	}

	return AdaptivePressureDecision{}
}

func (a *AdaptivePressure) FetchBatchOr(defaultBatch int) int {
	if a == nil || !a.cfg.Enabled {
		return defaultBatch
	}
	d := a.Decision()
	if d.FetchBatch > 0 {
		return d.FetchBatch
	}

	return defaultBatch
}
