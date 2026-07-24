package nats

import (
	"context"
	"fmt"
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
// Only setup.Consumer.Durable is applied from DurableConsumerConfig: push QueueSubscribeBound
// creates the durable. Pre-creating via Consumers().CreateOrUpdateConsumer would make a pull
// consumer and break bound queue subscribe. Filter/subject come from setup.Subject (and the
// stream Subjects). For MaxAckPending / AckWait on an existing durable, update the consumer
// separately after subscribe, or use CreateOrUpdateConsumer + pull/push APIs that match the mode.
func (c *client) SetupWorker(ctx context.Context, setup WorkerSetup, handler MsgHandler) (Subscription, error) {
	if err := ValidateStreamName(setup.Stream.Name); err != nil {
		return nil, fmt.Errorf("setup worker stream=%q: %w", setup.Stream.Name, err)
	}

	if err := ValidateDurableName(setup.Consumer.Durable); err != nil {
		return nil, fmt.Errorf("setup worker durable=%q: %w", setup.Consumer.Durable, err)
	}

	if err := ValidateSubject(setup.Subject); err != nil {
		return nil, fmt.Errorf("setup worker subject=%q: %w", setup.Subject, err)
	}

	if err := ValidateQueueName(setup.Queue); err != nil {
		return nil, fmt.Errorf("setup worker queue=%q: %w", setup.Queue, err)
	}

	if _, err := c.streams.CreateOrUpdateStream(ctx, setup.Stream); err != nil {
		return nil, fmt.Errorf("setup worker create stream=%q: %w", setup.Stream.Name, err)
	}
	// Push subscribe creates the durable push consumer; pre-creating via the management
	// API would make a pull consumer and break bound queue subscribe.
	sub, err := c.consumer.QueueSubscribeBound(ctx, setup.Stream.Name, setup.Consumer.Durable, setup.Queue, setup.Subject, handler)
	if err != nil {
		return nil, fmt.Errorf("setup worker subscribe stream=%q durable=%q queue=%q subject=%q: %w",
			setup.Stream.Name, setup.Consumer.Durable, setup.Queue, setup.Subject, err)
	}

	return sub, nil
}
