package nats

import (
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

func TestToNatsStreamConfigDefaults(t *testing.T) {
	sc := toNatsStreamConfig(StreamConfig{
		Name:     "ORDERS",
		Subjects: []string{"orders.>"},
	})

	assert.Equal(t, "ORDERS", sc.Name)
	assert.Equal(t, []string{"orders.>"}, sc.Subjects)
	assert.Equal(t, 1, sc.Replicas)
	assert.Equal(t, natspkg.LimitsPolicy, sc.Retention)
}

func TestToNatsStreamConfigFull(t *testing.T) {
	age := time.Hour
	sc := toNatsStreamConfig(StreamConfig{
		Name:            "EVENTS",
		Description:     "events stream",
		Subjects:        []string{"events.>"},
		Replicas:        3,
		Retention:       InterestPolicy,
		MaxMsgs:         1000,
		MaxBytes:        1_000_000,
		MaxAge:          age,
		MaxMsgSize:      512,
		Storage:         MemoryStorage,
		Discard:         DiscardNew,
		DuplicateWindow: 2 * time.Minute,
		NoAck:           true,
		MaxConsumers:    10,
		Mirror:          &StreamSource{Name: "EVENTS_SRC"},
		Sources:         []*StreamSource{{Name: "EVENTS_A"}},
	})

	assert.Equal(t, "events stream", sc.Description)
	assert.Equal(t, 3, sc.Replicas)
	assert.Equal(t, InterestPolicy, sc.Retention)
	assert.Equal(t, int64(1000), sc.MaxMsgs)
	assert.Equal(t, int64(1_000_000), sc.MaxBytes)
	assert.Equal(t, age, sc.MaxAge)
	assert.Equal(t, int32(512), sc.MaxMsgSize)
	assert.Equal(t, MemoryStorage, sc.Storage)
	assert.Equal(t, DiscardNew, sc.Discard)
	assert.Equal(t, 2*time.Minute, sc.Duplicates)
	assert.True(t, sc.NoAck)
	assert.Equal(t, 10, sc.MaxConsumers)
	require.NotNil(t, sc.Mirror)
	assert.Equal(t, "EVENTS_SRC", sc.Mirror.Name)
	require.Len(t, sc.Sources, 1)
	assert.Equal(t, "EVENTS_A", sc.Sources[0].Name)
}

func TestPurgeSubjectOpt(t *testing.T) {
	req := &natspkg.StreamPurgeRequest{}
	PurgeSubject("orders.created")(req)
	assert.Equal(t, "orders.created", req.Subject)
}

func TestPurgeStreamIntegration(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "PURGE")
	prefix := streamSubjectPrefix(stream)
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1,
	})
	require.NoError(t, err)
	require.NoError(t, client.Publisher().PublishBytes(ctx, prefix+"a", bytesconv.StringToBytes("1")))
	require.NoError(t, client.Streams().PurgeStream(ctx, stream))
	require.NoError(t, client.Streams().PurgeStream(ctx, stream, PurgeSubject(prefix+"a")))
	require.Error(t, client.Streams().PurgeStream(ctx, "bad name"))
}
