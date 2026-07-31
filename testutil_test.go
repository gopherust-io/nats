package nats

import (
	"context"
	"os"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/nats-io/nats-server/v2/server"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/propagation"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"

	"github.com/gopherust-io/nats/internal/bytesconv"
	"github.com/gopherust-io/tel"
)

const (
	testWaitShort = 2 * time.Second
	testPollFast  = 5 * time.Millisecond
)

var (
	testNameSanitizer = regexp.MustCompile(`[^a-zA-Z0-9_]+`)
	sharedNATSURL     string
	sharedNATS        *server.Server
)

func TestMain(m *testing.M) {
	dir, err := os.MkdirTemp("", "nats-lib-test-*")
	if err != nil {
		panic(err)
	}

	opts := &server.Options{
		Host:      "127.0.0.1",
		Port:      -1,
		JetStream: true,
		StoreDir:  dir,
		NoLog:     true,
		NoSigs:    true,
	}

	s, err := server.NewServer(opts)
	if err != nil {
		_ = os.RemoveAll(dir)
		panic(err)
	}

	go s.Start()
	if !s.ReadyForConnections(2 * time.Second) {
		s.Shutdown()
		_ = os.RemoveAll(dir)
		panic("shared nats server not ready")
	}

	sharedNATS = s
	sharedNATSURL = s.ClientURL()

	code := m.Run()

	s.Shutdown()
	_ = os.RemoveAll(dir)
	os.Exit(code)
}

func uniqueName(tb testing.TB, prefix string) string {
	tb.Helper()
	name := prefix + "_" + testNameSanitizer.ReplaceAllString(tb.Name(), "_")
	if len(name) > 64 {
		name = name[:64]
	}
	return name
}

func uniqueStream(tb testing.TB, prefix string) string {
	tb.Helper()
	return strings.ToUpper(uniqueName(tb, prefix))
}

func uniqueDurable(tb testing.TB, prefix string) string {
	tb.Helper()
	return strings.ToLower(uniqueName(tb, prefix))
}

func uniqueQueue(tb testing.TB, prefix string) string {
	tb.Helper()
	return strings.ToLower(uniqueName(tb, prefix)) + "_q"
}

func streamSubjectPrefix(stream string) string {
	return strings.ToLower(stream) + "."
}

// startTestNATSServer returns the package-shared JetStream server URL.
// Benchmarks and tests share one server; use uniqueStream for isolation.
func startTestNATSServer(tb testing.TB) string {
	tb.Helper()
	if bytesconv.IsEmpty(sharedNATSURL) {
		tb.Fatal("shared nats server not initialized")
	}
	return sharedNATSURL
}

func disableTelemetry(cfg *Config) {
	cfg.Metrics.AllowMetrics = false
	cfg.Metrics.AllowTracing = false
	cfg.Conn.AllowMetrics = false
	cfg.Conn.DrainTimeout = 250 * time.Millisecond
	cfg.Conn.ConnectTimeout = time.Second
	cfg.Conn.InitialRetryAttempts = 1
	cfg.RuntimeConsumer.AllowMetrics = false
	cfg.RuntimeConsumer.AllowTracing = false
	cfg.PublisherConfig.AllowMetrics = false
	cfg.PublisherConfig.AllowTracing = false
	cfg.RequesterConfig.AllowMetrics = false
	cfg.RequesterConfig.AllowTracing = false
	cfg.ResponderConfig.AllowMetrics = false
	cfg.ResponderConfig.AllowTracing = false
	cfg.Backpressure.PendingMsgLimit = 0
	cfg.Backpressure.PendingMsgBuffer = 0
	cfg.Backpressure.MaxAckPending = 0
	// Avoid idle-heartbeat async errors during short-lived test subscriptions.
	cfg.RuntimeConsumer.IdleHeartbeat = 0
	cfg.RuntimeConsumer.FlowControl = false
}

func testClientWithOptions(t *testing.T, fn func(*Config)) (Client, context.Context) {
	t.Helper()
	ctx := context.Background()
	cfg := DefaultConfig()
	cfg.Conn.Address = startTestNATSServer(t)
	cfg.Conn.AllowReconnect = false
	disableTelemetry(&cfg)
	if fn != nil {
		fn(&cfg)
	}
	serverURL := cfg.Conn.Address
	client, err := NewClient(ctx, &cfg)
	require.NoError(t, err)
	require.Equal(t, serverURL, client.Connector().ConnectionStatus().ServerURL)
	t.Cleanup(func() { _ = client.Connector().Shutdown() })
	return client, ctx
}

func testClient(t *testing.T) (Client, context.Context) {
	t.Helper()
	return testClientWithOptions(t, nil)
}

func testTelemetry(t *testing.T) (context.Context, *tel.Telemetry, *tracetest.SpanRecorder) {
	t.Helper()
	sr := tracetest.NewSpanRecorder()
	provider := sdktrace.NewTracerProvider(
		sdktrace.WithSpanProcessor(sr),
		sdktrace.WithSampler(sdktrace.AlwaysSample()),
	)
	otel.SetTracerProvider(provider)
	otel.SetTextMapPropagator(propagation.NewCompositeTextMapPropagator(propagation.TraceContext{}))
	t.Cleanup(func() { _ = provider.Shutdown(context.Background()) })

	cfg := tel.DefaultDebugConfig()
	telem := tel.NewWithTracerProvider(cfg, provider)
	ctx := tel.WrapContext(context.Background(), telem)
	return ctx, telem, sr
}

func testClientWithTracing(t *testing.T) (Client, context.Context, *tracetest.SpanRecorder) {
	t.Helper()
	ctx, telem, sr := testTelemetry(t)
	cfg := DefaultConfig()
	cfg.Conn.Address = startTestNATSServer(t)
	cfg.Conn.AllowReconnect = false
	disableTelemetry(&cfg)
	cfg.Metrics.AllowTracing = true
	cfg.PublisherConfig.AllowTracing = true
	cfg.RequesterConfig.AllowTracing = true
	cfg.ResponderConfig.AllowTracing = true
	cfg.RuntimeConsumer.AllowTracing = true
	client, err := NewClient(ctx, &cfg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Connector().Shutdown() })
	ctx = tel.WrapContext(ctx, telem)
	return client, ctx, sr
}
