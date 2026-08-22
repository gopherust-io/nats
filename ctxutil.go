package nats

import "context"

// ctxReady returns ctx.Err() when the context is already done.
func ctxReady(ctx context.Context) error {
	if ctx == nil {
		return nil
	}

	return ctx.Err()
}

// runWithContext races a synchronous JetStream call against ctx cancellation.
// The underlying nats.go call may still complete in the background; the caller
// receives ctx.Err() promptly when cancelled.
func runWithContext(ctx context.Context, fn func() error) error {
	if err := ctxReady(ctx); err != nil {
		return err
	}
	if ctx == nil {
		return fn()
	}

	type result struct{ err error }
	ch := make(chan result, 1)
	go func() {
		ch <- result{err: fn()}
	}()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case r := <-ch:
		return r.err
	}
}

func runWithContextValue[T any](ctx context.Context, fn func() (T, error)) (T, error) {
	var zero T
	if err := ctxReady(ctx); err != nil {
		return zero, err
	}
	if ctx == nil {
		return fn()
	}

	type result struct {
		val T
		err error
	}
	ch := make(chan result, 1)
	go func() {
		v, err := fn()
		ch <- result{val: v, err: err}
	}()

	select {
	case <-ctx.Done():
		return zero, ctx.Err()
	case r := <-ch:
		return r.val, r.err
	}
}
