package nats

import (
	"context"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestKeyValueManagerCRUD(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	bucket := uniqueName(t, "kvcrud")

	kv, err := client.KV().CreateOrUpdate(ctx, KeyValueConfig{
		Bucket:  bucket,
		TTL:     time.Hour,
		History: 1,
		Storage: MemoryStorage,
	})
	require.NoError(t, err)
	require.NotNil(t, kv)
	assert.Equal(t, bucket, kv.Bucket())

	opened, err := client.KV().Open(ctx, bucket)
	require.NoError(t, err)
	assert.Equal(t, bucket, opened.Bucket())

	again, err := client.KV().CreateOrUpdate(ctx, KeyValueConfig{
		Bucket:  bucket,
		TTL:     time.Hour,
		Storage: MemoryStorage,
	})
	require.NoError(t, err)
	assert.Equal(t, bucket, again.Bucket())

	require.NoError(t, client.KV().Delete(ctx, bucket))
}

func TestKeyValueManagerRejectsInvalidBucket(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)

	_, err := client.KV().CreateOrUpdate(ctx, KeyValueConfig{Bucket: "bad.bucket"})
	require.ErrorIs(t, err, ErrInvalidBucketName)

	_, err = client.KV().Open(ctx, "bad.bucket")
	require.ErrorIs(t, err, ErrInvalidBucketName)

	err = client.KV().Delete(ctx, "bad.bucket")
	require.ErrorIs(t, err, ErrInvalidBucketName)
}

func TestKeyValueTTLExpires(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	bucket := uniqueName(t, "kvttl")

	kv, err := client.KV().CreateOrUpdate(ctx, KeyValueConfig{
		Bucket:  bucket,
		TTL:     500 * time.Millisecond,
		History: 1,
		Storage: MemoryStorage,
	})
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.KV().Delete(context.Background(), bucket) })

	_, err = kv.Put("id-1", []byte{1})
	require.NoError(t, err)

	_, err = kv.Get("id-1")
	require.NoError(t, err)

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		_, err = kv.Get("id-1")
		if err != nil {
			require.ErrorIs(t, err, natspkg.ErrKeyNotFound)
			return
		}
		time.Sleep(testPollFast)
	}
	t.Fatal("expected KV key to expire via TTL")
}
