package shadow

import (
	"context"
	"math"
	"sync"
	"sync/atomic"

	natspkg "github.com/nats-io/nats.go"
)

// GraduatePhase is the canary lifecycle state.
type GraduatePhase uint8

const (
	PhaseRamping GraduatePhase = iota + 1
	PhaseHolding
	PhasePromoteReady
	PhaseAborted
)

// GraduateConfig configures shadow sample-rate graduation.
//
// goalign:ignore
type GraduateConfig struct {
	OnAbort         func(reason string)
	OnPromoteReady  func()
	Compare         func(primaryErr, shadowErr error) bool
	Recorder        Recorder
	Metrics         Metrics
	StartRate       float64
	MaxRate         float64
	Step            float64
	MaxMismatchRate float64
	Window          int
}

func (c GraduateConfig) withDefaults() GraduateConfig {
	out := c
	if out.StartRate <= 0 {
		out.StartRate = 0.05
	}
	if out.MaxRate <= 0 {
		out.MaxRate = 1
	}
	if out.MaxRate > 1 {
		out.MaxRate = 1
	}
	if out.Step <= 0 {
		out.Step = 0.2
	}
	if out.Window <= 0 {
		out.Window = 500
	}
	if out.MaxMismatchRate <= 0 {
		out.MaxMismatchRate = 0.01
	}
	if out.StartRate > out.MaxRate {
		out.StartRate = out.MaxRate
	}

	return out
}

// GraduateStatus is a snapshot of canary progress.
//
// goalign:ignore
type GraduateStatus struct {
	Matches    uint64
	Mismatches uint64
	Samples    uint64
	Reason     string
	Rate       float64
	Phase      GraduatePhase
}

// Graduate dual-runs handlers and ramps SampleRate on healthy windows.
type Graduate struct {
	cfg        GraduateConfig
	handler    Handler
	rate       atomic.Uint64 // rate * 1e6
	matches    atomic.Uint64
	mismatches atomic.Uint64
	samples    atomic.Uint64
	phase      atomic.Uint32
	mu         sync.Mutex
	reason     string
}

// NewGraduate wraps primary/shadow with an adaptive sample rate.
func NewGraduate(cfg GraduateConfig, primary, shadowHandler Handler) *Graduate {
	cfg = cfg.withDefaults()
	grad := &Graduate{cfg: cfg}
	grad.rate.Store(uint64(cfg.StartRate * 1e6))
	grad.phase.Store(uint32(PhaseRamping))
	grad.handler = func(ctx context.Context, msg *natspkg.Msg) error {
		if GraduatePhase(grad.phase.Load()) == PhaseAborted {
			return primary(ctx, msg)
		}
		rate := float64(grad.rate.Load()) / 1e6

		return With(Config{
			Recorder:   grad.cfg.Recorder,
			Metrics:    grad.cfg.Metrics,
			Compare:    grad.observeCompare(cfg.Compare),
			SampleRate: rate,
		}, primary, shadowHandler)(ctx, msg)
	}

	return grad
}

func (g *Graduate) Handler() Handler { return g.handler }

func (g *Graduate) Status() GraduateStatus {
	g.mu.Lock()
	reason := g.reason
	g.mu.Unlock()

	return GraduateStatus{
		Phase:      GraduatePhase(g.phase.Load()),
		Rate:       float64(g.rate.Load()) / 1e6,
		Matches:    g.matches.Load(),
		Mismatches: g.mismatches.Load(),
		Samples:    g.samples.Load(),
		Reason:     reason,
	}
}

func (g *Graduate) Abort(reason string) {
	g.mu.Lock()
	g.reason = reason
	g.mu.Unlock()
	g.phase.Store(uint32(PhaseAborted))
	if g.cfg.OnAbort != nil {
		g.cfg.OnAbort(reason)
	}
}

func (g *Graduate) Promote() {
	if GraduatePhase(g.phase.Load()) != PhasePromoteReady {
		return
	}
	g.phase.Store(uint32(PhaseHolding))
}

func (g *Graduate) observeCompare(base func(primaryErr, shadowErr error) bool) func(primaryErr, shadowErr error) bool {
	if base == nil {
		base = func(primaryErr, shadowErr error) bool {
			return (primaryErr == nil) == (shadowErr == nil)
		}
	}

	return func(primaryErr, shadowErr error) bool {
		match := base(primaryErr, shadowErr)
		g.samples.Add(1)
		if match {
			g.matches.Add(1)
		} else {
			g.mismatches.Add(1)
		}
		g.evaluateWindow()

		return match
	}
}

func (g *Graduate) evaluateWindow() {
	phase := GraduatePhase(g.phase.Load())
	if phase == PhaseAborted || phase == PhasePromoteReady {
		return
	}

	samples := g.samples.Load()
	window := uint64(g.cfg.Window)
	if samples == 0 || samples%window != 0 {
		return
	}

	mismatchRate := float64(g.mismatches.Load()) / float64(samples)
	if mismatchRate > g.cfg.MaxMismatchRate {
		g.Abort("mismatch_rate")

		return
	}

	rate := float64(g.rate.Load()) / 1e6
	if rate+1e-9 >= g.cfg.MaxRate {
		g.phase.Store(uint32(PhasePromoteReady))
		if g.cfg.OnPromoteReady != nil {
			g.cfg.OnPromoteReady()
		}

		return
	}

	next := math.Min(g.cfg.MaxRate, rate+g.cfg.Step)
	g.rate.Store(uint64(next * 1e6))
	g.phase.Store(uint32(PhaseRamping))
}
