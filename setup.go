package nats

import (
	"context"
	"fmt"

	natspkg "github.com/nats-io/nats.go"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

// WorkerSetup configures a one-shot stream + durable + queue subscribe.
type WorkerSetup struct {
	Queue    string
	Subject  string
	Stream   StreamConfig
	Consumer DurableConsumerConfig
}

// SetupWorker creates or updates the stream, then queue-subscribes with a bound durable.
//
// Durable name comes from setup.Consumer.Durable. The subscribe filter is setup.Subject
// when using a single filter, or setup.Consumer.FilterSubjects for multi-filter durables
// (Subject may be empty in that case). MaxAckPending / AckWait from setup.Consumer are
// applied as subscribe opts (falling back to client Backpressure.MaxAckPending /
// RuntimeConsumer.AckWait when unset). Additional durable fields (MaxDeliver, Metadata,
// RateLimit, …) are applied via UpdateConsumer after subscribe. Heartbeat/FlowControl
// are skipped for queue groups (nats.go limitation).
func (c *client) SetupWorker(ctx context.Context, setup WorkerSetup, handler MsgHandler) (Subscription, error) {
	if err := ValidateStreamName(setup.Stream.Name); err != nil {
		return nil, fmt.Errorf("setup worker stream=%q: %w", setup.Stream.Name, err)
	}

	if err := ValidateDurableName(setup.Consumer.Durable); err != nil {
		return nil, fmt.Errorf("setup worker durable=%q: %w", setup.Consumer.Durable, err)
	}

	if err := ValidateQueueName(setup.Queue); err != nil {
		return nil, fmt.Errorf("setup worker queue=%q: %w", setup.Queue, err)
	}

	multi := len(setup.Consumer.FilterSubjects) > 0
	if multi {
		if err := ValidateSubjects(setup.Consumer.FilterSubjects); err != nil {
			return nil, fmt.Errorf("setup worker filter subjects: %w", err)
		}
		if !bytesconv.IsEmpty(setup.Consumer.FilterSubject) {
			return nil, fmt.Errorf("setup worker: use FilterSubject or FilterSubjects, not both")
		}
		if !bytesconv.IsEmpty(setup.Subject) {
			if err := ValidateSubject(setup.Subject); err != nil {
				return nil, fmt.Errorf("setup worker subject=%q: %w", setup.Subject, err)
			}
		}
	} else {
		if err := ValidateSubject(setup.Subject); err != nil {
			return nil, fmt.Errorf("setup worker subject=%q: %w", setup.Subject, err)
		}
		if !bytesconv.IsEmpty(setup.Consumer.FilterSubject) && setup.Consumer.FilterSubject != setup.Subject {
			return nil, fmt.Errorf("setup worker: Consumer.FilterSubject %q must equal Subject %q",
				setup.Consumer.FilterSubject, setup.Subject)
		}
	}

	if _, err := c.streams.CreateOrUpdateStream(ctx, setup.Stream); err != nil {
		return nil, fmt.Errorf("setup worker create stream=%q: %w", setup.Stream.Name, err)
	}

	if multi {
		return c.setupWorkerMultiFilter(ctx, setup, handler)
	}

	opts := setupWorkerSubOpts(setup.Consumer, c.config.RuntimeConsumer, c.config.Backpressure)
	opts = append(opts, natspkg.BindStream(setup.Stream.Name), natspkg.Durable(setup.Consumer.Durable))

	sub, err := c.consumer.QueueSubscribe(ctx, setup.Queue, setup.Subject, handler, opts...)
	if err != nil {
		return nil, fmt.Errorf("setup worker subscribe stream=%q durable=%q queue=%q subject=%q: %w",
			setup.Stream.Name, setup.Consumer.Durable, setup.Queue, setup.Subject, err)
	}

	if err := c.applySetupDurableConfig(ctx, setup); err != nil {
		_ = sub.Unsubscribe()

		return nil, err
	}

	drainOnCancel(ctx, sub)

	return sub, nil
}

// setupWorkerMultiFilter creates the durable with FilterSubjects first, then binds
// a queue subscription to that consumer (nats.Bind) so all filters are honored.
func (c *client) setupWorkerMultiFilter(ctx context.Context, setup WorkerSetup, handler MsgHandler) (Subscription, error) {
	deliver := setup.Consumer.DeliverSubject
	if bytesconv.IsEmpty(deliver) {
		deliver = natspkg.NewInbox()
	}

	cfg := setup.Consumer
	cfg.DeliverSubject = deliver
	if bytesconv.IsEmpty(cfg.DeliverGroup) {
		cfg.DeliverGroup = setup.Queue
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = c.config.RuntimeConsumer.AckWait
	}
	if cfg.MaxAckPending <= 0 {
		cfg.MaxAckPending = c.config.Backpressure.MaxAckPending
	}

	if _, err := c.consumers.CreateOrUpdateConsumer(ctx, setup.Stream.Name, cfg); err != nil {
		return nil, fmt.Errorf("setup worker create multi-filter durable=%q: %w", setup.Consumer.Durable, err)
	}

	sub, err := c.consumer.QueueSubscribe(ctx, setup.Queue, deliver, handler,
		natspkg.Bind(setup.Stream.Name, setup.Consumer.Durable))
	if err != nil {
		return nil, fmt.Errorf("setup worker bind multi-filter stream=%q durable=%q queue=%q: %w",
			setup.Stream.Name, setup.Consumer.Durable, setup.Queue, err)
	}

	if err := c.applySetupDurableConfig(ctx, setup); err != nil {
		_ = sub.Unsubscribe()

		return nil, err
	}

	drainOnCancel(ctx, sub)

	return sub, nil
}

func setupWorkerSubOpts(durable DurableConsumerConfig, runtime RuntimeConsumerConfig, bp BackpressureConfig) []natspkg.SubOpt {
	opts := make([]natspkg.SubOpt, 0, 4)
	ackWait := durable.AckWait
	if ackWait <= 0 {
		ackWait = runtime.AckWait
	}
	if ackWait > 0 {
		opts = append(opts, natspkg.AckWait(ackWait))
	}

	maxAck := durable.MaxAckPending
	if maxAck <= 0 {
		maxAck = bp.MaxAckPending
	}
	if maxAck > 0 {
		opts = append(opts, natspkg.MaxAckPending(maxAck))
	}

	return opts
}

func (c *client) applySetupDurableConfig(ctx context.Context, setup WorkerSetup) error {
	info, err := c.consumers.ConsumerInfo(ctx, setup.Stream.Name, setup.Consumer.Durable)
	if err != nil {
		return fmt.Errorf("setup worker consumer info stream=%q durable=%q: %w",
			setup.Stream.Name, setup.Consumer.Durable, err)
	}

	cc := info.Config
	updated := false

	if setup.Consumer.MaxDeliver > 0 && cc.MaxDeliver != setup.Consumer.MaxDeliver {
		cc.MaxDeliver = setup.Consumer.MaxDeliver
		updated = true
	}
	if setup.Consumer.RateLimit > 0 && cc.RateLimit != setup.Consumer.RateLimit {
		cc.RateLimit = setup.Consumer.RateLimit
		updated = true
	}
	if setup.Consumer.InactiveThreshold > 0 && cc.InactiveThreshold != setup.Consumer.InactiveThreshold {
		cc.InactiveThreshold = setup.Consumer.InactiveThreshold
		updated = true
	}
	if setup.Consumer.Replicas > 0 && cc.Replicas != setup.Consumer.Replicas {
		cc.Replicas = setup.Consumer.Replicas
		updated = true
	}
	if setup.Consumer.MemStorage && !cc.MemoryStorage {
		cc.MemoryStorage = true
		updated = true
	}
	if len(setup.Consumer.Metadata) > 0 {
		if cc.Metadata == nil {
			cc.Metadata = make(map[string]string, len(setup.Consumer.Metadata))
		}
		for k, v := range setup.Consumer.Metadata {
			if cc.Metadata[k] != v {
				cc.Metadata[k] = v
				updated = true
			}
		}
	}
	if setup.Consumer.MaxAckPending > 0 && cc.MaxAckPending != setup.Consumer.MaxAckPending {
		cc.MaxAckPending = setup.Consumer.MaxAckPending
		updated = true
	}
	if setup.Consumer.AckWait > 0 && cc.AckWait != setup.Consumer.AckWait {
		cc.AckWait = setup.Consumer.AckWait
		updated = true
	}
	if setup.Consumer.Heartbeat > 0 && cc.Heartbeat != setup.Consumer.Heartbeat {
		cc.Heartbeat = setup.Consumer.Heartbeat
		updated = true
	}
	if setup.Consumer.MaxWaiting > 0 && cc.MaxWaiting != setup.Consumer.MaxWaiting {
		cc.MaxWaiting = setup.Consumer.MaxWaiting
		updated = true
	}
	if setup.Consumer.HasAckPolicy && cc.AckPolicy != setup.Consumer.AckPolicy {
		cc.AckPolicy = setup.Consumer.AckPolicy
		updated = true
	}
	if setup.Consumer.HasReplayPolicy && cc.ReplayPolicy != setup.Consumer.ReplayPolicy {
		cc.ReplayPolicy = setup.Consumer.ReplayPolicy
		updated = true
	}
	if setup.Consumer.HasDeliverPolicy && cc.DeliverPolicy != setup.Consumer.DeliverPolicy {
		// Deliver policy is immutable on most servers; surface recreate required.
		return fmt.Errorf("setup worker durable=%q: %w (deliver policy)",
			setup.Consumer.Durable, ErrConsumerRecreateRequired)
	}
	if len(setup.Consumer.FilterSubjects) > 0 {
		if !equalStringSlices(cc.FilterSubjects, setup.Consumer.FilterSubjects) {
			cc.FilterSubjects = append([]string(nil), setup.Consumer.FilterSubjects...)
			cc.FilterSubject = ""
			updated = true
		}
	} else if !bytesconv.IsEmpty(setup.Subject) && cc.FilterSubject != setup.Subject && len(cc.FilterSubjects) == 0 {
		cc.FilterSubject = setup.Subject
		updated = true
	}

	if !updated {
		return nil
	}

	if _, err := c.consumers.UpdateConsumer(ctx, setup.Stream.Name, &cc); err != nil {
		return fmt.Errorf("setup worker update consumer stream=%q durable=%q: %w",
			setup.Stream.Name, setup.Consumer.Durable, err)
	}

	return nil
}

func equalStringSlices(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}

	return true
}
