package nats

import (
	"context"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIdleHeartbeatOnSubscribeBound(t *testing.T) {
	t.Parallel()
	client, ctx := testClientWithOptions(t, func(cfg *Config) {
		cfg.RuntimeConsumer.IdleHeartbeat = 500 * time.Millisecond
		cfg.RuntimeConsumer.FlowControl = true
	})
	stream := uniqueStream(t, "HB")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage,
	})
	require.NoError(t, err)

	durable := uniqueDurable(t, "hb")
	got := make(chan struct{}, 1)
	sub, err := client.Consumer().SubscribeBound(ctx, stream, durable, prefix+">",
		func(_ context.Context, msg *natspkg.Msg) error {
			select {
			case got <- struct{}{}:
			default:
			}
			return nil
		})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	info, err := sub.ConsumerInfo()
	require.NoError(t, err)
	assert.Equal(t, 500*time.Millisecond, info.Config.Heartbeat)
	assert.True(t, info.Config.FlowControl)

	require.NoError(t, client.Publisher().PublishJSON(ctx, prefix+"e", map[string]int{"n": 1}))
	select {
	case <-got:
	case <-time.After(testWaitShort):
		t.Fatal("timeout waiting for message")
	}
}

func TestQueueSubscribeBoundRejectsIdleHeartbeat(t *testing.T) {
	t.Parallel()
	// Queue path must strip IdleHeartbeat; subscribe must succeed.
	client, ctx := testClientWithOptions(t, func(cfg *Config) {
		cfg.RuntimeConsumer.IdleHeartbeat = time.Second
		cfg.RuntimeConsumer.FlowControl = true
	})
	stream := uniqueStream(t, "HBQ")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage,
	})
	require.NoError(t, err)

	durable := uniqueDurable(t, "hbq")
	queue := uniqueQueue(t, "hbq")
	sub, err := client.Consumer().QueueSubscribeBound(ctx, stream, durable, queue, prefix+">",
		func(_ context.Context, _ *natspkg.Msg) error { return nil })
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	info, err := sub.ConsumerInfo()
	require.NoError(t, err)
	assert.Zero(t, info.Config.Heartbeat)
	assert.False(t, info.Config.FlowControl)
}

func TestInactiveThresholdOnCreateConsumer(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "INACT")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage,
	})
	require.NoError(t, err)

	durable := uniqueDurable(t, "inact")
	_, err = client.Consumers().CreateOrUpdateConsumer(ctx, stream, DurableConsumerConfig{
		Durable:           durable,
		FilterSubject:     prefix + ">",
		InactiveThreshold: 45 * time.Second,
	})
	require.NoError(t, err)

	info, err := client.Consumers().ConsumerInfo(ctx, stream, durable)
	require.NoError(t, err)
	assert.Equal(t, 45*time.Second, info.Config.InactiveThreshold)
}

func TestOnErrorCountsIdleHeartbeatMiss(t *testing.T) {
	t.Parallel()
	metrics := newClientMetrics(context.Background(), MetricsConfig{
		AllowMetrics: true,
		Prefix:       "hbtest",
	})
	require.NotNil(t, metrics)
	require.NotNil(t, metrics.idleHeartbeatMisses)

	cl := &client{
		ctx:     context.Background(),
		metrics: metrics,
	}
	cl.onError(nil, nil, natspkg.ErrConsumerNotActive)
	// Counter increments; FastCounter may not expose value — smoke that no panic.
}

func TestWithFetchHeartbeatOpt(t *testing.T) {
	t.Parallel()
	cfg := fetchConfig{maxWait: time.Second}
	WithFetchHeartbeat(200 * time.Millisecond)(&cfg)
	assert.Equal(t, 200*time.Millisecond, cfg.heartbeat)

	pcfg := processConfig{}
	WithProcessHeartbeat(300 * time.Millisecond)(&pcfg)
	assert.Equal(t, 300*time.Millisecond, pcfg.heartbeat)
}
