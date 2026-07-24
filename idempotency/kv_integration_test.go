package idempotency_test

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	libnats "github.com/gopherust-io/nats"
	"github.com/gopherust-io/nats/idempotency"
)

func startKVTestServer(t *testing.T) string {
	t.Helper()
	dir, err := os.MkdirTemp("", "nats-kv-dedup-*")
	require.NoError(t, err)
	t.Cleanup(func() { _ = os.RemoveAll(dir) })

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  dir,
		NoLog:     true,
		NoSigs:    true,
	}
	s, err := server.NewServer(opts)
	require.NoError(t, err)
	go s.Start()
	require.True(t, s.ReadyForConnections(2*time.Second))
	t.Cleanup(s.Shutdown)

	return s.ClientURL()
}

func TestKVStoreIntegrationWithHandler(t *testing.T) {
	url := startKVTestServer(t)
	cfg := libnats.DefaultConfig()
	cfg.Conn.Address = url
	cfg.Conn.InitialRetryAttempts = 1
	cfg.Metrics.AllowMetrics = false
	cfg.Metrics.AllowTracing = false
	cfg.Conn.AllowMetrics = false
	cfg.RuntimeConsumer.AllowMetrics = false
	cfg.RuntimeConsumer.AllowTracing = false
	cfg.PublisherConfig.AllowMetrics = false
	cfg.PublisherConfig.AllowTracing = false

	client, err := libnats.NewClient(context.Background(), &cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Connector().Shutdown() })

	ctx := context.Background()
	kv, err := client.KV().CreateOrUpdate(ctx, libnats.KeyValueConfig{
		Bucket:  "DEDUP_HANDLER",
		TTL:     time.Hour,
		History: 1,
		Storage: libnats.MemoryStorage,
	})
	require.NoError(t, err)

	store := idempotency.NewKVStore(kv)
	calls := 0
	handler := idempotency.WithHandler(store, idempotency.MsgIDFromHeader,
		func(_ context.Context, _ *natspkg.Msg) error {
			calls++
			return nil
		})

	msg := &natspkg.Msg{
		Header: natspkg.Header{libnats.HeaderMsgID: []string{"order-99"}},
	}
	require.NoError(t, handler(ctx, msg))
	require.NoError(t, handler(ctx, msg))
	assert.Equal(t, 1, calls)

	seen, err := store.Seen(ctx, "order-99")
	require.NoError(t, err)
	assert.True(t, seen)
}
