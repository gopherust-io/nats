package idempotency

import (
	"context"
	"errors"
	"fmt"

	natspkg "github.com/nats-io/nats.go"

	libnats "github.com/gopherust-io/nats"
	"github.com/gopherust-io/nats/internal/bytesconv"
)

// ErrClaimInFlight is returned when another worker holds a pending claim.
// Callers should Nak (not Ack). Ack would drop the message if the holder
// later fails and Releases. Completed claims still return nil (Ack).
var ErrClaimInFlight = errors.New("idempotency: claim held by another worker")

type DedupStore interface {
	Seen(ctx context.Context, id string) (bool, error)
	Mark(ctx context.Context, id string) error
}

// ClaimStore is an optional DedupStore that supports claim-before-process.
// Claim acquires exclusive ownership of id (true if acquired).
// Release drops a failed claim so another worker can retry.
// Seen must return true only for completed (done) claims, not pending ones.
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
		if bytesconv.IsEmpty(id) {
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
		done, seenErr := store.Seen(ctx, id)
		if seenErr != nil {
			return seenErr
		}
		if done {
			return nil
		}

		return fmt.Errorf("%w: id=%q", ErrClaimInFlight, id)
	}

	if err := handler(ctx, msg); err != nil {
		if relErr := store.Release(ctx, id); relErr != nil {
			return errors.Join(err, fmt.Errorf("idempotency release id=%q: %w", id, relErr))
		}

		return err
	}

	if err := store.Mark(ctx, id); err != nil {
		// Drop the pending claim so redelivery can reclaim instead of looping on ErrClaimInFlight.
		if relErr := store.Release(ctx, id); relErr != nil {
			return errors.Join(err, fmt.Errorf("idempotency release after mark id=%q: %w", id, relErr))
		}

		return err
	}

	return nil
}
