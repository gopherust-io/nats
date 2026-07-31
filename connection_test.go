package nats

import (
	"context"
	"net"
	"testing"
	"time"

	"github.com/gopherust-io/nats/internal/bytesconv"
	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestConfigureOptionsNoReconnect(t *testing.T) {
	t.Parallel()

	c, _ := testClientWithOptions(t, func(cfg *Config) {
		cfg.Conn.AllowReconnect = false
	})

	cl := c.(*client)
	require.NotNil(t, cl.conn)
	assert.False(t, cl.conn.Opts.AllowReconnect)
}

func TestConfigureOptionsUnlimitedReconnect(t *testing.T) {
	t.Parallel()

	c, _ := testClientWithOptions(t, func(cfg *Config) {
		cfg.Conn.AllowReconnect = true
		cfg.Conn.MaxReconnect = -1
		cfg.Conn.ReconnectBufSize = defaultReconnectBufSize
		cfg.Conn.ReconnectWait = time.Second
	})

	cl := c.(*client)
	require.NotNil(t, cl.conn)
	assert.True(t, cl.conn.Opts.AllowReconnect)
	assert.Equal(t, -1, cl.conn.Opts.MaxReconnect)
	assert.Equal(t, defaultReconnectBufSize, cl.conn.Opts.ReconnectBufSize)
	assert.NotNil(t, cl.conn.Opts.CustomReconnectDelayCB)
}

func TestConfigureOptionsDisableReconnectBuf(t *testing.T) {
	t.Parallel()

	c, _ := testClientWithOptions(t, func(cfg *Config) {
		cfg.Conn.AllowReconnect = true
		cfg.Conn.ReconnectBufSize = -1
	})

	cl := c.(*client)
	assert.Equal(t, -1, cl.conn.Opts.ReconnectBufSize)
}

func TestConfigureTLSInvalidCA(t *testing.T) {
	t.Parallel()

	cl := &client{config: &Config{Conn: Connection{
		Address: "nats://127.0.0.1:4222",
		TLS:     ConnectionTLS{CA: bytesconv.StringToBytes("not-a-pem")},
	}}}
	_, err := cl.configureOptions()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "CA cert")
}

func TestConfigureTLSIncompleteClientCert(t *testing.T) {
	t.Parallel()

	cl := &client{config: &Config{Conn: Connection{
		Address: "nats://127.0.0.1:4222",
		TLS:     ConnectionTLS{Cert: bytesconv.StringToBytes("cert-only")},
	}}}
	_, err := cl.configureOptions()
	require.Error(t, err)
	assert.Contains(t, err.Error(), "both cert and key")
}

func TestCappedExponentialReconnectDelay(t *testing.T) {
	t.Parallel()

	fn := cappedExponentialReconnectDelay(time.Second)
	assert.Equal(t, time.Second, fn(1))
	assert.Equal(t, 2*time.Second, fn(2))
	assert.Equal(t, 4*time.Second, fn(3))
	assert.Equal(t, defaultMaxReconnectDelay, fn(100))
}

func TestShutdownIdempotent(t *testing.T) {
	t.Parallel()

	c, _ := testClient(t)
	require.NoError(t, c.Connector().Shutdown())
	require.NoError(t, c.Connector().Shutdown())
	assert.False(t, c.Connector().IsConnected())
}

func TestWaitConnectedAlreadyConnected(t *testing.T) {
	t.Parallel()

	c, ctx := testClient(t)
	require.NoError(t, c.Connector().WaitConnected(ctx))
}

func TestWaitConnectedCanceled(t *testing.T) {
	t.Parallel()

	c, _ := testClientWithOptions(t, func(cfg *Config) {
		cfg.Conn.AllowReconnect = true
	})
	cl := c.(*client)
	require.NotNil(t, cl.conn)

	// Force a disconnected wait by closing without Shutdown (avoids Drain path).
	cl.conn.Close()
	require.Eventually(t, func() bool { return !c.Connector().IsConnected() }, testWaitShort, testPollFast)

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	err := c.Connector().WaitConnected(ctx)
	require.Error(t, err)
}

func TestReconnectIncrementsCountWithoutMetrics(t *testing.T) {
	t.Parallel()

	s, url, port := startPrivateNATSServer(t)
	defer s.Shutdown()

	ctx := context.Background()
	cfg := DefaultConfig()
	cfg.Conn.Address = url
	cfg.Conn.AllowReconnect = true
	cfg.Conn.MaxReconnect = -1
	cfg.Conn.ReconnectWait = 50 * time.Millisecond
	cfg.Conn.ReconnectJitter = time.Millisecond
	cfg.Conn.ReconnectJitterTLS = time.Millisecond
	cfg.Conn.CustomReconnectDelay = func(int) time.Duration { return 50 * time.Millisecond }
	cfg.Conn.InitialRetryAttempts = 1
	disableTelemetry(&cfg)

	c, err := NewClient(ctx, &cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Connector().Shutdown() })

	require.True(t, c.Connector().IsConnected())
	before := c.Connector().ConnectionStatus().ReconnectCount

	s.Shutdown()
	require.Eventually(t, func() bool { return !c.Connector().IsConnected() }, testWaitShort, testPollFast)

	restartPrivateNATSServer(t, port)
	require.Eventually(t, func() bool { return c.Connector().IsConnected() }, 5*time.Second, 20*time.Millisecond)

	status := c.Connector().ConnectionStatus()
	assert.Greater(t, status.ReconnectCount, before)
	assert.False(t, status.LastDisconnect.IsZero())
}

func TestPublishGuardWhenReconnectBufDisabled(t *testing.T) {
	t.Parallel()

	s, url, _ := startPrivateNATSServer(t)
	defer s.Shutdown()

	ctx := context.Background()
	cfg := DefaultConfig()
	cfg.Conn.Address = url
	cfg.Conn.AllowReconnect = true
	cfg.Conn.MaxReconnect = -1
	cfg.Conn.ReconnectBufSize = -1
	cfg.Conn.ReconnectWait = 50 * time.Millisecond
	cfg.Conn.InitialRetryAttempts = 1
	disableTelemetry(&cfg)

	c, err := NewClient(ctx, &cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Connector().Shutdown() })

	stream := uniqueStream(t, "PUBGUARD")
	prefix := streamSubjectPrefix(stream)
	_, err = c.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1, Storage: MemoryStorage,
	})
	require.NoError(t, err)

	s.Shutdown()
	require.Eventually(t, func() bool { return !c.Connector().IsConnected() }, testWaitShort, testPollFast)

	err = c.Publisher().PublishJSON(ctx, prefix+"evt", map[string]string{"k": "v"})
	require.ErrorIs(t, err, ErrNatsConnectionNotEstablished)
}

func TestConnectionStatusIncludesLameDuckFlag(t *testing.T) {
	t.Parallel()

	c, _ := testClient(t)
	status := c.Connector().ConnectionStatus()
	assert.False(t, status.InLameDuck)
	assert.True(t, status.Connected)
}

func startPrivateNATSServer(t *testing.T) (*server.Server, string, int) {
	t.Helper()

	dir := t.TempDir()
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

	addr, ok := s.Addr().(*net.TCPAddr)
	require.True(t, ok)

	url := s.ClientURL()

	return s, url, addr.Port
}

func restartPrivateNATSServer(t *testing.T, port int) *server.Server {
	t.Helper()

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      port,
		JetStream: true,
		StoreDir:  t.TempDir(),
		NoLog:     true,
		NoSigs:    true,
	}

	s, err := server.NewServer(opts)
	require.NoError(t, err)
	go s.Start()
	require.True(t, s.ReadyForConnections(2*time.Second))
	t.Cleanup(func() { s.Shutdown() })

	return s
}

func TestClientReconnectAndLameDuckHandlers(t *testing.T) {
	t.Parallel()
	c := &client{ctx: context.Background()}

	c.onReconnectErr(nil, nil)
	assert.Nil(t, c.ConnectionStatus().LastError)

	c.onReconnectErr(nil, assert.AnError)
	assert.ErrorIs(t, c.ConnectionStatus().LastError, assert.AnError)

	assert.False(t, c.ConnectionStatus().InLameDuck)
	c.onLameDuck(nil)
	assert.True(t, c.ConnectionStatus().InLameDuck)
}
