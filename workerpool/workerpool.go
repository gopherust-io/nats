package workerpool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	_ "github.com/gopherust-io/tel"
	"github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
)

const (
	stateRunning  uint32 = 1
	stateDraining uint32 = 2
	stateStopped  uint32 = 3
)

var ErrPoolStopped = errors.New("worker pool stopped")

type msgFn func(context.Context, *nats.Msg) error

// goalign:ignore
type task struct {
	ctx       context.Context
	msg       *nats.Msg
	processFn msgFn
	applyFn   bool
}

// WorkerPool is a fixed-size goroutine pool over a buffered channel.
// Publish is lock-free on the accept path: state CAS gates enqueue, and
// GracefulStop drains without closing the channel (avoids send-on-closed races).
//
// goalign:ignore
type WorkerPool struct {
	pool       sync.Pool
	ctx        context.Context
	cond       *sync.Cond
	registerFn msgFn
	cancel     context.CancelFunc
	input      chan *task
	wg         sync.WaitGroup
	len        int
	depth      atomic.Int64
	// inFlight counts handlers currently executing (depth is queue-only).
	inFlight atomic.Int64
	// publishers counts tryPublish callers past the accept gate until send/rollback completes.
	publishers atomic.Int64
	mu         sync.Mutex
	state      atomic.Uint32
}

func New(ctx context.Context, workerPoolLen, messageBufLen int, registerFn msgFn) *WorkerPool {
	if workerPoolLen < 1 {
		zerolog.Ctx(ctx).Warn().
			Int("requested", workerPoolLen).
			Msg("worker pool size must be >= 1, defaulting to 1")
		workerPoolLen = 1
	}

	if messageBufLen < 0 {
		zerolog.Ctx(ctx).Warn().
			Int("requested", messageBufLen).
			Msg("worker buffer size must be >= 0, defaulting to 0")
		messageBufLen = 0
	}

	poolCtx, cancel := context.WithCancel(ctx)
	pool := &WorkerPool{
		len:        workerPoolLen,
		input:      make(chan *task, messageBufLen),
		registerFn: registerFn,
		cancel:     cancel,
		ctx:        poolCtx,
	}
	pool.cond = sync.NewCond(&pool.mu)
	pool.state.Store(stateRunning)
	pool.pool.New = func() any {
		return &task{}
	}

	return pool
}

func (w *WorkerPool) Consume() {
	zerolog.Ctx(w.ctx).Info().Msg("starting worker pool")

	for range w.len {
		w.wg.Add(1)

		go w.runner()
	}
}

func (w *WorkerPool) runner() {
	defer w.wg.Done()

	for {
		select {
		case t := <-w.input:
			w.process(t)
		case <-w.ctx.Done():
			w.drain()

			return
		}
	}
}

// drain consumes remaining tasks after cancel until no publishers are in-flight
// and the queue/handler depth is zero.
func (w *WorkerPool) drain() {
	for {
		select {
		case t := <-w.input:
			w.process(t)

			continue
		default:
		}

		if w.publishers.Load() == 0 && w.depth.Load() == 0 && w.inFlight.Load() == 0 {
			return
		}

		w.mu.Lock()
		for w.publishers.Load() != 0 || w.depth.Load() != 0 || w.inFlight.Load() != 0 {
			// Re-check channel under wait cycles so newly queued tasks are taken.
			select {
			case t := <-w.input:
				w.mu.Unlock()
				w.process(t)
				w.mu.Lock()

				continue
			default:
			}
			if w.publishers.Load() == 0 && w.depth.Load() == 0 && w.inFlight.Load() == 0 {
				break
			}
			w.cond.Wait()
		}
		w.mu.Unlock()
	}
}

func (w *WorkerPool) signalDrain() {
	w.mu.Lock()
	w.cond.Broadcast()
	w.mu.Unlock()
}

func (w *WorkerPool) process(t *task) {
	w.depth.Add(-1)
	w.inFlight.Add(1)
	w.signalDrain()

	var err error
	if t.applyFn && t.processFn != nil {
		err = t.processFn(t.ctx, t.msg)
	} else {
		err = w.registerFn(t.ctx, t.msg)
	}

	if err != nil {
		zerolog.Ctx(w.ctx).Error().
			Str("subject", t.msg.Subject).
			Err(err).
			Msg("worker pool runner")
	}

	t.ctx = nil
	t.msg = nil
	t.applyFn = false
	t.processFn = nil
	w.pool.Put(t)

	w.inFlight.Add(-1)
	w.signalDrain()
}

func (w *WorkerPool) Publish(ctx context.Context, msg *nats.Msg, applyFn bool, processFn msgFn) {
	_, err := w.tryPublish(ctx, msg, applyFn, processFn, false)
	if err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("worker pool publish")
	}
}

func (w *WorkerPool) TryPublish(ctx context.Context, msg *nats.Msg, applyFn bool, processFn msgFn) error {
	_, err := w.tryPublish(ctx, msg, applyFn, processFn, false)

	return err
}

// TryPublishNonBlocking enqueues when capacity exists; returns accepted=false when the buffer is full.
func (w *WorkerPool) TryPublishNonBlocking(ctx context.Context, msg *nats.Msg, applyFn bool, processFn msgFn) (bool, error) {
	return w.tryPublish(ctx, msg, applyFn, processFn, true)
}

func (w *WorkerPool) tryPublish(ctx context.Context, msg *nats.Msg, applyFn bool, processFn msgFn, nonBlocking bool) (bool, error) {
	// Track in-flight publishers so GracefulStop cannot finish while a send may still land.
	w.publishers.Add(1)
	defer func() {
		w.publishers.Add(-1)
		w.signalDrain()
	}()

	if w.state.Load() != stateRunning {
		return false, ErrPoolStopped
	}

	raw := w.pool.Get()

	t, ok := raw.(*task)
	if !ok {
		return false, fmt.Errorf("worker pool: invalid pooled task type %T", raw)
	}

	t.ctx = ctx
	t.msg = msg
	t.applyFn = applyFn
	t.processFn = processFn

	w.depth.Add(1)

	if nonBlocking {
		select {
		case w.input <- t:
			w.signalDrain()

			return true, nil
		default:
			w.depth.Add(-1)

			t.ctx = nil
			t.msg = nil
			t.applyFn = false
			t.processFn = nil
			w.pool.Put(t)

			return false, nil
		}
	}

	select {
	case w.input <- t:
		w.signalDrain()

		return true, nil
	case <-w.ctx.Done():
		w.depth.Add(-1)

		t.ctx = nil
		t.msg = nil
		t.applyFn = false
		t.processFn = nil
		w.pool.Put(t)

		if w.state.Load() != stateRunning {
			return false, ErrPoolStopped
		}

		return false, w.ctx.Err()
	}
}

func (w *WorkerPool) QueueDepth() int {
	return int(w.depth.Load())
}

func (w *WorkerPool) GracefulStop() {
	if !w.state.CompareAndSwap(stateRunning, stateDraining) {
		return
	}

	zerolog.Ctx(w.ctx).Info().Msg("shutdown worker pool")

	// Reject new publishes (state=draining) and unblock senders waiting on a full buffer.
	// Workers drain remaining tasks; do not close input (avoids send-on-closed races).
	w.cancel()
	w.signalDrain()
	w.wg.Wait()
	w.state.Store(stateStopped)
}

// Stats is a point-in-time view of pool depth and worker count.
//
// goalign:ignore
type Stats struct {
	Depth   int
	Workers int
	State   uint32
}

func (w *WorkerPool) Stats() Stats {
	return Stats{
		Depth:   w.QueueDepth(),
		Workers: w.len,
		State:   w.state.Load(),
	}
}
