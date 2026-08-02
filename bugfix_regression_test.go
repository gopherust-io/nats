package nats

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestEncodeRawCopiesBuffer(t *testing.T) {
	t.Parallel()

	src := []byte("hello")
	out, err := Encode(Message{Data: src, MessageType: Raw})
	require.NoError(t, err)
	src[0] = 'X'
	assert.Equal(t, []byte("hello"), out)
}

func TestDurableConfigPreservesDeliverSubject(t *testing.T) {
	t.Parallel()

	cfg := durableConfigFromNATS(natspkg.ConsumerConfig{
		Durable:        "worker",
		DeliverSubject: "_INBOX.push",
		DeliverGroup:   "q",
		AckPolicy:      AckExplicit,
	})
	assert.Equal(t, "_INBOX.push", cfg.DeliverSubject)
	assert.Equal(t, "q", cfg.DeliverGroup)

	cc := toNatsConsumerConfig(cfg)
	assert.Equal(t, "_INBOX.push", cc.DeliverSubject)
	assert.Equal(t, "q", cc.DeliverGroup)
}

func TestProcessMessageAlreadyAckedIsSuccess(t *testing.T) {
	t.Parallel()

	client, ctx := testClient(t)
	stream := uniqueName(t, "ackd")
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name:     stream,
		Subjects: []string{stream + ".>"},
	})
	require.NoError(t, err)

	done := make(chan struct{})
	_, err = client.Consumer().SubscribeBound(ctx, stream, "d", stream+".>", func(_ context.Context, msg *natspkg.Msg) error {
		require.NoError(t, msg.Ack())
		close(done)
		return nil // library must not fail on second Ack
	})
	require.NoError(t, err)

	require.NoError(t, client.Publisher().PublishBytes(ctx, stream+".x", []byte("1")))
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for handler")
	}
	time.Sleep(50 * time.Millisecond) // allow processMessage Ack path to finish without error logs/panic
}

func TestReplayBoundsUntilBeforeStart(t *testing.T) {
	t.Parallel()
	err := validateReplayBounds(ReplayConfig{
		optStartSeqSet: true,
		OptStartSeq:    10,
		untilSeqSet:    true,
		UntilSeq:       5,
	})
	require.ErrorIs(t, err, ErrInvalidReplayBound)
}

func TestShutdownDrainsConnection(t *testing.T) {
	t.Parallel()

	client, _ := testClient(t)
	require.NoError(t, client.Connector().Shutdown())
	require.False(t, client.Connector().IsConnected())
}

func TestNakRemainingNilSafe(t *testing.T) {
	t.Parallel()
	nakRemaining(nil)
	nakRemaining([]*natspkg.Msg{nil})
}

func TestPullProcessWithWorkerPool(t *testing.T) {
	t.Parallel()

	client, ctx := testClientWithOptions(t, func(cfg *Config) {
		cfg.RuntimeConsumer.WorkerPoolEnabled = true
		cfg.RuntimeConsumer.WorkerPoolSize = 2
		cfg.RuntimeConsumer.WorkerBufferSize = 8
	})
	stream := uniqueStream(t, "PULLPOOL")
	prefix := streamSubjectPrefix(stream)
	durable := uniqueDurable(t, "pullpool")

	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage, Replicas: 1,
	})
	require.NoError(t, err)
	_, err = client.Consumers().CreateOrUpdateConsumer(ctx, stream, DurableConsumerConfig{
		Durable: durable, FilterSubject: prefix + ">", MaxAckPending: 100,
	})
	require.NoError(t, err)

	for range 3 {
		require.NoError(t, client.Publisher().PublishBytes(ctx, prefix+"x", []byte("m")))
	}

	done := make(chan struct{})
	var n atomic.Int32
	go func() {
		pull, pullErr := client.Consumer().Pull(stream, durable)
		require.NoError(t, pullErr)
		runCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
		defer cancel()
		_ = pull.Process(runCtx, func(_ context.Context, _ *natspkg.Msg) error {
			if n.Add(1) == 3 {
				close(done)
			}
			return nil
		}, WithFetchBatch(3), WithProcessMaxWait(500*time.Millisecond))
	}()

	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("timeout waiting for pull pool process")
	}
}
