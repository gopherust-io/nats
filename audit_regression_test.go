package nats

import (
	"bytes"
	"context"
	"net"
	"sync/atomic"
	"testing"
	"time"

	"github.com/klauspost/compress/gzip"
	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/require"

	"github.com/gopherust-io/nats/workerpool"
)

func TestShutdownWithHealthCheckNoPanic(t *testing.T) {
	t.Parallel()

	client, _ := testClientWithOptions(t, func(cfg *Config) {
		cfg.Conn.HealthCheckInterval = 5 * time.Millisecond
	})

	time.Sleep(20 * time.Millisecond)
	require.NoError(t, client.Connector().Shutdown())
	require.NoError(t, client.Connector().Shutdown())
}

func TestSharedWorkerPoolUsesPerSubscriptionHandler(t *testing.T) {
	t.Parallel()

	client, ctx := testClientWithOptions(t, func(cfg *Config) {
		cfg.RuntimeConsumer.WorkerPoolEnabled = true
		cfg.RuntimeConsumer.WorkerPoolSize = 2
		cfg.RuntimeConsumer.WorkerBufferSize = 8
	})

	stream := uniqueName(t, "audit-pool")
	_, err := client.Streams().AddStream(ctx, &natspkg.StreamConfig{
		Name:     stream,
		Subjects: []string{stream + ".>"},
	})
	require.NoError(t, err)

	var aCount, bCount atomic.Int64
	_, err = client.Consumer().SubscribeBound(ctx, stream, "a", stream+".a", func(_ context.Context, _ *natspkg.Msg) error {
		aCount.Add(1)
		return nil
	})
	require.NoError(t, err)
	_, err = client.Consumer().SubscribeBound(ctx, stream, "b", stream+".b", func(_ context.Context, _ *natspkg.Msg) error {
		bCount.Add(1)
		return nil
	})
	require.NoError(t, err)

	require.NoError(t, client.Publisher().PublishBytes(ctx, stream+".a", []byte("1")))
	require.NoError(t, client.Publisher().PublishBytes(ctx, stream+".b", []byte("2")))

	require.Eventually(t, func() bool {
		return aCount.Load() >= 1 && bCount.Load() >= 1
	}, 5*time.Second, 20*time.Millisecond)
}

func TestDecompressRejectsOversizePayload(t *testing.T) {
	t.Parallel()

	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	chunk := bytes.Repeat([]byte("z"), 1<<20)
	written := 0
	for written <= MaxPayloadDecompressBytes {
		n, err := w.Write(chunk)
		require.NoError(t, err)
		written += n
	}
	require.NoError(t, w.Close())

	_, err := DecompressPayload(buf.Bytes(), EncodingGzip)
	require.ErrorIs(t, err, ErrPayloadTooLarge)
}

func TestWorkerPoolRespectsCallerCancel(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	block := make(chan struct{})
	pool := workerpool.New(ctx, 1, 0, nil)
	pool.Consume()
	t.Cleanup(pool.GracefulStop)

	require.NoError(t, pool.TryPublish(ctx, &natspkg.Msg{Subject: "hold"}, true, func(context.Context, *natspkg.Msg) error {
		<-block
		return nil
	}))

	pubCtx, cancel := context.WithCancel(context.Background())
	cancel()
	err := pool.TryPublish(pubCtx, &natspkg.Msg{Subject: "blocked"}, true, func(context.Context, *natspkg.Msg) error {
		return nil
	})
	require.ErrorIs(t, err, context.Canceled)
	close(block)
}

func TestMonitoringBlocksCloudMetadataIPs(t *testing.T) {
	t.Parallel()

	require.True(t, isBlockedMonitoringIP(net.ParseIP("169.254.169.254")))
	require.True(t, isBlockedMonitoringIP(net.ParseIP("100.100.100.200")))
	require.True(t, isBlockedMonitoringIP(net.ParseIP("fd00:ec2::254")))
	require.False(t, isBlockedMonitoringIP(net.ParseIP("10.0.0.5")))
	require.Error(t, validateMonitoringFetchURL(t.Context(), mustURL(t, "http://100.100.100.200/")))
}
