package nats

import (
	"errors"
	"fmt"
	"time"

	natspkg "github.com/nats-io/nats.go"
)

// Ack helpers wrap nats.go JetStream ack primitives for long-running and poison handlers.
// Prefer returning an error from MsgHandler for the library's default Nak path unless you
// need InProgress heartbeats or an explicit delayed Nak / term-with-reason.

// InProgress tells JetStream the handler is still working (extends AckWait).
func InProgress(msg *natspkg.Msg) error {
	if msg == nil {
		return errors.New("in progress: nil message")
	}

	if err := msg.InProgress(); err != nil {
		return fmt.Errorf("in progress subject=%q: %w", msg.Subject, err)
	}

	return nil
}

// NakWithDelay negatively acknowledges with a server-side redelivery delay.
func NakWithDelay(msg *natspkg.Msg, delay time.Duration) error {
	if msg == nil {
		return errors.New("nak with delay: nil message")
	}

	if err := msg.NakWithDelay(delay); err != nil {
		return fmt.Errorf("nak with delay subject=%q: %w", msg.Subject, err)
	}

	return nil
}

// TermWithReason terminates redelivery and attaches a reason (servers that support it).
// Empty reason falls back to Term().
func TermWithReason(msg *natspkg.Msg, reason string) error {
	if msg == nil {
		return errors.New("term with reason: nil message")
	}

	if reason == empty {
		if err := msg.Term(); err != nil {
			return fmt.Errorf("term subject=%q: %w", msg.Subject, err)
		}

		return nil
	}

	// Match jetstream TermWithReason wire format for classic Msg.
	if err := msg.Respond([]byte("+TERM " + reason)); err != nil {
		return fmt.Errorf("term with reason subject=%q: %w", msg.Subject, err)
	}

	return nil
}
