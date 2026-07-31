package nats

import (
	"context"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

func TestPublishExpectationLastSeq(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "EXPECT")
	prefix := streamSubjectPrefix(stream)
	subject := prefix + "events"
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage,
	})
	require.NoError(t, err)

	msg := Message{Data: map[string]string{"n": "1"}, MessageType: JSON}.
		WithExpectedStream(stream).
		WithExpectedLastSeq(0)
	require.NoError(t, client.Publisher().PublishMessage(ctx, subject, msg))

	bad := Message{Data: map[string]string{"n": "2"}, MessageType: JSON}.
		WithExpectedStream(stream).
		WithExpectedLastSeq(0)
	require.Error(t, client.Publisher().PublishMessage(ctx, subject, bad))
}

func TestAckHelpersInProgress(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "ACKHELP")
	prefix := streamSubjectPrefix(stream)
	subject := prefix + "job"
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage, Retention: WorkQueuePolicy,
	})
	require.NoError(t, err)

	durable := uniqueDurable(t, "ackhelp")
	got := make(chan struct{}, 1)
	sub, err := client.Consumer().QueueSubscribeBound(ctx, stream, durable, uniqueName(t, "q"), subject,
		func(_ context.Context, msg *natspkg.Msg) error {
			require.NoError(t, InProgress(msg))
			got <- struct{}{}

			return nil
		})
	require.NoError(t, err)
	t.Cleanup(func() { _ = sub.Unsubscribe() })

	require.NoError(t, client.Publisher().PublishBytes(ctx, subject, bytesconv.StringToBytes("x")))
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("timeout waiting for handler")
	}
}

func TestAckHelperNilMessage(t *testing.T) {
	t.Parallel()
	require.Error(t, InProgress(nil))
	require.Error(t, NakWithDelay(nil, time.Second))
	require.Error(t, TermWithReason(nil, "x"))
}

func TestPauseResumeConsumer(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "PAUSE")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage,
	})
	require.NoError(t, err)

	durable := uniqueDurable(t, "pause")
	_, err = client.Consumers().CreateOrUpdateConsumer(ctx, stream, DurableConsumerConfig{
		Durable: durable, FilterSubject: prefix + ">",
	})
	require.NoError(t, err)

	until := time.Now().Add(time.Hour)
	require.NoError(t, client.Consumers().PauseConsumer(ctx, stream, durable, until))
	require.NoError(t, client.Consumers().ResumeConsumer(ctx, stream, durable))
}

func TestCredentialsFileConflictsWithUserPass(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.Conn.CredentialsFile = "/tmp/demo.creds"
	cfg.Conn.User = "u"
	cfg.Conn.Password = "p"
	require.ErrorIs(t, validateAuthConfig(cfg.Conn), ErrConflictingAuth)
}
