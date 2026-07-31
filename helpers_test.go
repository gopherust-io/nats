package nats

import (
	"errors"
	"github.com/gopherust-io/nats/internal/bytesconv"
	"testing"
)

func TestMetricSubjectLabel(t *testing.T) {
	t.Parallel()
	if got := metricSubjectLabel(nil, "orders.x"); got != "orders.x" {
		t.Fatalf("nil metrics: got %q", got)
	}
	m := &clientMetrics{}
	if got := metricSubjectLabel(m, "orders.x"); got != "orders.x" {
		t.Fatalf("cardinality off: got %q", got)
	}
	m.fixedCardinality = true
	if got := metricSubjectLabel(m, "orders.x"); !bytesconv.IsEmpty(got) {
		t.Fatalf("cardinality on: got %q want empty", got)
	}
}

func TestValidateOutboundSubject(t *testing.T) {
	t.Parallel()
	if err := validateOutboundSubject("orders.ok", "publish", false); err != nil {
		t.Fatalf("valid subject: %v", err)
	}
	if err := validateOutboundSubject("", "publish", true); err != nil {
		t.Fatalf("skip should allow empty: %v", err)
	}
	err := validateOutboundSubject("", "publish", false)
	if err == nil || !errors.Is(err, ErrEmptySubjectNotAllowed) {
		t.Fatalf("empty subject: got %v want ErrEmptySubjectNotAllowed", err)
	}
	err = validateOutboundSubject("bad.*.x", "request", false)
	if err == nil {
		t.Fatal("wildcard publish subject should fail")
	}
}

func TestTrySendDropsWhenFull(t *testing.T) {
	t.Parallel()
	ch := make(chan int, 1)
	trySend(ch, 1)
	trySend(ch, 2) // must not block
	if got := <-ch; got != 1 {
		t.Fatalf("got %d want 1", got)
	}
}
