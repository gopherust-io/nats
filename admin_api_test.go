package nats

import (
	"context"
	"testing"

	"github.com/gopherust-io/nats/internal/bytesconv"
	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPageSlice(t *testing.T) {
	t.Parallel()

	items := []string{"a", "b", "c", "d"}
	page, total := pageSlice(items, 1, 2)
	assert.Equal(t, 4, total)
	assert.Equal(t, []string{"b", "c"}, page)

	page, total = pageSlice(items, 10, 2)
	assert.Equal(t, 4, total)
	assert.Empty(t, page)
}

func TestStreamManagerAdminAPIs(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	name := uniqueName(t, "admin")

	info, err := client.Streams().AddStream(ctx, &natspkg.StreamConfig{
		Name:     name,
		Subjects: []string{name + ".>"},
		Storage:  natspkg.MemoryStorage,
	})
	require.NoError(t, err)
	require.Equal(t, name, info.Config.Name)
	t.Cleanup(func() { _ = client.Streams().DeleteStream(context.Background(), name) })

	names, err := StreamNames(ctx, client.Streams())
	require.NoError(t, err)
	assert.Contains(t, names, name)

	page, total, err := client.Streams().ListStreamsPage(ctx, 0, 100)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, total, 1)
	found := false
	for _, s := range page {
		if s.Config.Name == name {
			found = true
			break
		}
	}
	assert.True(t, found)

	_, err = client.Streams().UpdateStream(ctx, &natspkg.StreamConfig{
		Name:        name,
		Subjects:    []string{name + ".>"},
		Storage:     natspkg.MemoryStorage,
		Description: "updated",
	})
	require.NoError(t, err)

	got, err := client.Connector().AccountInfo(ctx)
	require.NoError(t, err)
	require.NotNil(t, got)
	require.NotNil(t, client.Connector().Conn())
	require.NotNil(t, client.Connector().JetStream())
}

func TestConsumerManagerAdminAPIs(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueName(t, "cons")
	durable := uniqueName(t, "dur")

	_, err := client.Streams().AddStream(ctx, &natspkg.StreamConfig{
		Name:     stream,
		Subjects: []string{stream + ".>"},
		Storage:  natspkg.MemoryStorage,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Streams().DeleteStream(context.Background(), stream) })

	_, err = client.Consumers().AddConsumer(ctx, stream, &natspkg.ConsumerConfig{
		Durable:       durable,
		AckPolicy:     natspkg.AckExplicitPolicy,
		FilterSubject: stream + ".>",
	})
	require.NoError(t, err)

	names, err := client.Consumers().ConsumerNames(ctx, stream)
	require.NoError(t, err)
	assert.Contains(t, names, durable)

	page, total, err := client.Consumers().ListConsumersPage(ctx, stream, 0, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	require.Len(t, page, 1)
	assert.Equal(t, durable, page[0].Name)
}

func TestKeyValueManagerAdminAPIs(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	bucket := uniqueName(t, "kvadm")

	status, err := client.KV().CreateRaw(ctx, &natspkg.KeyValueConfig{
		Bucket:  bucket,
		History: 5,
		Storage: natspkg.MemoryStorage,
	})
	require.NoError(t, err)
	assert.Equal(t, bucket, status.Bucket)
	t.Cleanup(func() { _ = client.KV().Delete(context.Background(), bucket) })

	entry, err := client.KVKeys().Put(ctx, bucket, "k1", bytesconv.StringToBytes("v1"))
	require.NoError(t, err)
	assert.Equal(t, "k1", entry.Key)

	got, err := client.KVKeys().Get(ctx, bucket, "k1")
	require.NoError(t, err)
	assert.Equal(t, bytesconv.StringToBytes("v1"), got.Value)

	keys, total, err := client.KVKeys().ListKeys(ctx, bucket, 0, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, []string{"k1"}, keys)

	hist, err := client.KVKeys().History(ctx, bucket, "k1")
	require.NoError(t, err)
	assert.NotEmpty(t, hist)

	buckets, err := client.KV().ListBuckets(ctx)
	require.NoError(t, err)
	found := false
	for _, b := range buckets {
		if b.Bucket == bucket {
			found = true
			break
		}
	}
	assert.True(t, found)

	require.NoError(t, client.KVKeys().DeleteKey(ctx, bucket, "k1"))
}

func TestObjectStoreManagerCRUD(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	bucket := uniqueName(t, "obj")

	status, err := client.Objects().Create(ctx, ObjectStoreConfig{
		Bucket:  bucket,
		Storage: MemoryStorage,
	})
	require.NoError(t, err)
	assert.Equal(t, bucket, status.Bucket)
	t.Cleanup(func() { _ = client.Objects().Delete(context.Background(), bucket) })

	entry, err := client.Objects().Put(ctx, bucket, "file.txt", bytesconv.StringToBytes("hello"))
	require.NoError(t, err)
	assert.Equal(t, "file.txt", entry.Name)
	assert.Equal(t, uint64(5), entry.Size)

	got, err := client.Objects().Get(ctx, bucket, "file.txt")
	require.NoError(t, err)
	assert.Equal(t, bytesconv.StringToBytes("hello"), got.Data)

	names, total, err := client.Objects().ListObjects(ctx, bucket, 0, 10)
	require.NoError(t, err)
	assert.Equal(t, 1, total)
	assert.Equal(t, []string{"file.txt"}, names)

	info, err := client.Objects().BucketInfo(ctx, bucket)
	require.NoError(t, err)
	assert.Equal(t, bucket, info.Bucket)

	require.NoError(t, client.Objects().DeleteObject(ctx, bucket, "file.txt"))
}

func TestListObjectsEmptyBucket(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	bucket := uniqueName(t, "objempty")

	_, err := client.Objects().Create(ctx, ObjectStoreConfig{
		Bucket:  bucket,
		Storage: MemoryStorage,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Objects().Delete(context.Background(), bucket) })

	names, total, err := client.Objects().ListObjects(ctx, bucket, 0, -1)
	require.NoError(t, err)
	assert.Equal(t, 0, total)
	assert.Empty(t, names)
}

func TestPublishRaw(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueName(t, "raw")
	subject := stream + ".evt"

	_, err := client.Streams().AddStream(ctx, &natspkg.StreamConfig{
		Name:     stream,
		Subjects: []string{stream + ".>"},
		Storage:  natspkg.MemoryStorage,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Streams().DeleteStream(context.Background(), stream) })

	ack, err := client.PublishRaw(ctx, subject, bytesconv.StringToBytes("payload"), map[string]string{"X-Test": "1"})
	require.NoError(t, err)
	require.NotNil(t, ack)
	assert.Equal(t, stream, ack.Stream)
	assert.Greater(t, ack.Sequence, uint64(0))
}

func TestMonitoringEmptyBaseURL(t *testing.T) {
	t.Parallel()
	m := newMonitoring(0)
	_, err := m.Fetch(context.Background(), "", "/jsz")
	require.Error(t, err)
}
