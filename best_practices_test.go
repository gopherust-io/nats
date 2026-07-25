package nats

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestCreateOrUpdateConsumerRejectsDeliverPolicyChange(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "RECREATE")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage,
	})
	require.NoError(t, err)

	durable := uniqueDurable(t, "recreate")
	_, err = client.Consumers().CreateOrUpdateConsumer(ctx, stream, DurableConsumerConfig{
		Durable:          durable,
		FilterSubject:    prefix + ">",
		DeliverPolicy:    DeliverAll,
		HasDeliverPolicy: true,
		MaxAckPending:    42,
	})
	require.NoError(t, err)

	_, err = client.Consumers().CreateOrUpdateConsumer(ctx, stream, DurableConsumerConfig{
		Durable:          durable,
		FilterSubject:    prefix + ">",
		DeliverPolicy:    DeliverNew,
		HasDeliverPolicy: true,
		MaxAckPending:    42,
	})
	require.ErrorIs(t, err, ErrConsumerRecreateRequired)

	info, err := client.Consumers().ConsumerInfo(ctx, stream, durable)
	require.NoError(t, err)
	assert.Equal(t, DeliverAll, info.Config.DeliverPolicy)
	assert.Equal(t, 42, info.Config.MaxAckPending)
}

func TestCreateOrUpdateConsumerOmitsDeliverPolicyKeepsExisting(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "KEEPDEL")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage,
	})
	require.NoError(t, err)

	durable := uniqueDurable(t, "keepdel")
	_, err = client.Consumers().CreateOrUpdateConsumer(ctx, stream, DurableConsumerConfig{
		Durable:          durable,
		FilterSubject:    prefix + ">",
		DeliverPolicy:    DeliverNew,
		HasDeliverPolicy: true,
		MaxAckPending:    10,
	})
	require.NoError(t, err)

	_, err = client.Consumers().CreateOrUpdateConsumer(ctx, stream, DurableConsumerConfig{
		Durable:       durable,
		FilterSubject: prefix + ">",
		MaxAckPending: 99,
	})
	require.NoError(t, err)

	info, err := client.Consumers().ConsumerInfo(ctx, stream, durable)
	require.NoError(t, err)
	assert.Equal(t, DeliverNew, info.Config.DeliverPolicy)
	assert.Equal(t, 99, info.Config.MaxAckPending)
}

func TestKVCreateOrUpdateUpdatesTTL(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	bucket := uniqueName(t, "kvupd")

	_, err := client.KV().CreateOrUpdate(ctx, KeyValueConfig{
		Bucket:  bucket,
		TTL:     time.Hour,
		History: 1,
		Storage: MemoryStorage,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.KV().Delete(context.Background(), bucket) })

	kv, err := client.KV().CreateOrUpdate(ctx, KeyValueConfig{
		Bucket:  bucket,
		TTL:     2 * time.Hour,
		History: 1,
		Storage: MemoryStorage,
	})
	require.NoError(t, err)

	status, err := kv.Status()
	require.NoError(t, err)
	assert.Equal(t, 2*time.Hour, status.TTL())
}

func TestValidateAuthConfig(t *testing.T) {
	t.Parallel()
	require.NoError(t, validateAuthConfig(Connection{}))
	require.NoError(t, validateAuthConfig(Connection{Seed: "SXXXX"})) // seed format checked later
	require.NoError(t, validateAuthConfig(Connection{User: "u", Password: "p"}))
	require.NoError(t, validateAuthConfig(Connection{Secret: "tok"}))

	require.ErrorIs(t, validateAuthConfig(Connection{Seed: "s", Secret: "t"}), ErrConflictingAuth)
	require.ErrorIs(t, validateAuthConfig(Connection{Seed: "s", User: "u", Password: "p"}), ErrConflictingAuth)
	require.Error(t, validateAuthConfig(Connection{User: "u"}))
}

func TestRedactURLString(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "nats://127.0.0.1:4222", redactURLString("nats://user:pass@127.0.0.1:4222"))
	assert.Equal(t, "nats://127.0.0.1:4222,nats://127.0.0.1:4223",
		redactURLString("nats://a:b@127.0.0.1:4222, nats://c:d@127.0.0.1:4223"))
	assert.Equal(t, "nats://127.0.0.1:4222", redactURLString("nats://127.0.0.1:4222"))
}

func TestNewClientRejectsConflictingAuth(t *testing.T) {
	t.Parallel()
	cfg := DefaultConfig()
	cfg.Conn.Address = "nats://127.0.0.1:4222"
	cfg.Conn.Seed = "seed"
	cfg.Conn.Secret = "token"
	cfg.Conn.InitialRetryAttempts = 0
	disableTelemetry(&cfg)

	_, err := NewClient(context.Background(), &cfg)
	require.ErrorIs(t, err, ErrConflictingAuth)
}
