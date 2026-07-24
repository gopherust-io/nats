package idempotency

import (
	"context"

	natspkg "github.com/nats-io/nats.go"

	libnats "github.com/gopherust-io/nats"
)

type DedupStore interface {
	Seen(ctx context.Context, id string) (bool, error)
	Mark(ctx context.Context, id string) error
}

// ClaimStore is an optional DedupStore that supports claim-before-process.
// Claim acquires exclusive ownership of id (true if acquired).
// Release drops a failed claim so another worker can retry.
type ClaimStore interface {
	DedupStore
	Claim(ctx context.Context, id string) (acquired bool, err error)
	Release(ctx context.Context, id string) error
}

func MsgIDFromHeader(msg *natspkg.Msg) string {
	if msg == nil || msg.Header == nil {
		return ""
	}

	return msg.Header.Get(libnats.HeaderMsgID)
}

func WithHandler(store DedupStore, extractID func(*natspkg.Msg) string, handler libnats.MsgHandler) libnats.MsgHandler {
	return func(ctx context.Context, msg *natspkg.Msg) error {
		id := extractID(msg)
		if id == "" {
			return handler(ctx, msg)
		}

		if claimer, ok := store.(ClaimStore); ok {
			return withClaimHandler(ctx, claimer, id, msg, handler)
		}

		seen, err := store.Seen(ctx, id)
		if err != nil {
			return err
		}

		if seen {
			return nil
		}

		if err := handler(ctx, msg); err != nil {
			return err
		}

		return store.Mark(ctx, id)
	}
}

func withClaimHandler(
	ctx context.Context,
	store ClaimStore,
	id string,
	msg *natspkg.Msg,
	handler libnats.MsgHandler,
) error {
	acquired, err := store.Claim(ctx, id)
	if err != nil {
		return err
	}

	if !acquired {
		return nil
	}

	if err := handler(ctx, msg); err != nil {
		_ = store.Release(ctx, id)

		return err
	}

	return nil
}
