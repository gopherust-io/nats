package nats

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"sync/atomic"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/nats-io/nkeys"
)

type Connector interface {
	IsConnected() bool
	Shutdown() error
	HealthCheck(ctx context.Context) error
	ConnectionStatus() ConnectionStatus
	WaitConnected(ctx context.Context) error
}

type ConnectionStatus struct {
	LastDisconnect time.Time
	LastError      error
	ServerURL      string
	ReconnectCount int64
	Connected      bool
	InLameDuck     bool
}

func (c *client) connectWithRetry(ctx context.Context) (*natspkg.Conn, error) {
	waitTime := c.config.Conn.InitialRetryWait
	if waitTime == 0 {
		waitTime = defaultInitialRetryWait
	}

	backoff := c.config.Conn.InitialRetryBackoff
	if backoff == 0 {
		backoff = defaultInitialRetryBackoff
	}

	attempts := c.config.Conn.InitialRetryAttempts
	if attempts <= 0 {
		attempts = defaultInitialRetryAttempts
	}

	var lastErr error

	for i := range attempts {
		opts, optsErr := c.configureOptions()
		if optsErr != nil {
			return nil, optsErr
		}

		nc, err := natspkg.Connect(c.config.Conn.Address, opts...)
		if err == nil {
			return nc, nil
		}

		lastErr = err
		slog.WarnContext(ctx, "failed to connect to NATS",
			slog.String("address", redactURLString(c.config.Conn.Address)),
			slog.Int("attempt", i+1),
			slog.String("err", err.Error()))

		if i < attempts-1 {
			select {
			case <-ctx.Done():
				return nil, fmt.Errorf("context cancelled: %w", ctx.Err())
			case <-time.After(waitTime):
			}

			waitTime = time.Duration(float64(waitTime) * backoff)
		}
	}

	return nil, fmt.Errorf("connect after %d attempts: %w", attempts, lastErr)
}

func (c *client) configureOptions() ([]natspkg.Option, error) {
	opts := make([]natspkg.Option, 0, defaultNATSOptionsCapacity)
	conn := c.config.Conn

	opts = appendBaseConnOptions(opts, conn)

	authOpts, err := appendAuthOptions(opts, conn)
	if err != nil {
		return nil, err
	}

	opts = authOpts

	opts, err = appendTLSOptions(opts, conn.TLS)
	if err != nil {
		return nil, err
	}

	opts = appendReconnectOptions(opts, conn)
	opts = append(opts,
		natspkg.ConnectHandler(c.onConnect),
		natspkg.DisconnectErrHandler(c.onDisconnect),
		natspkg.ReconnectHandler(c.onReconnect),
		natspkg.ReconnectErrHandler(c.onReconnectErr),
		natspkg.ClosedHandler(c.onClosed),
		natspkg.ErrorHandler(c.onError),
		natspkg.LameDuckModeHandler(c.onLameDuck),
	)

	return opts, nil
}

func appendBaseConnOptions(opts []natspkg.Option, conn Connection) []natspkg.Option {
	if conn.ConnectTimeout > 0 {
		opts = append(opts, natspkg.Timeout(conn.ConnectTimeout))
	}

	if conn.DrainTimeout > 0 {
		opts = append(opts, natspkg.DrainTimeout(conn.DrainTimeout))
	}

	if conn.ClientName != empty {
		opts = append(opts, natspkg.Name(conn.ClientName))
	}

	if conn.PingInterval > 0 {
		opts = append(opts, natspkg.PingInterval(conn.PingInterval))
	}

	if conn.MaxPingsOut > 0 {
		opts = append(opts, natspkg.MaxPingsOutstanding(conn.MaxPingsOut))
	}

	if conn.FlusherTimeout > 0 {
		opts = append(opts, natspkg.FlusherTimeout(conn.FlusherTimeout))
	}

	if conn.DontRandomize {
		opts = append(opts, natspkg.DontRandomize())
	}

	return opts
}

func appendAuthOptions(opts []natspkg.Option, conn Connection) ([]natspkg.Option, error) {
	if conn.CredentialsFile != empty {
		opts = append(opts, natspkg.UserCredentials(conn.CredentialsFile))
	}

	if conn.Seed != empty {
		kp, err := nkeys.FromSeed([]byte(conn.Seed))
		if err != nil {
			return nil, fmt.Errorf("%w: %w", ErrInvalidNKeySeed, err)
		}

		pubKey, err := kp.PublicKey()
		if err != nil {
			return nil, fmt.Errorf("%w: public key: %w", ErrInvalidNKeySeed, err)
		}

		opts = append(opts, natspkg.Nkey(pubKey, func(nonce []byte) ([]byte, error) {
			return kp.Sign(nonce)
		}))
	}

	if conn.User != empty && conn.Password != empty {
		opts = append(opts, natspkg.UserInfo(conn.User, conn.Password))
	}

	if conn.Secret != empty {
		opts = append(opts, natspkg.Token(conn.Secret))
	}

	return opts, nil
}

func appendReconnectOptions(opts []natspkg.Option, conn Connection) []natspkg.Option {
	if !conn.AllowReconnect {
		return append(opts, natspkg.NoReconnect())
	}

	opts = append(opts, natspkg.MaxReconnects(conn.MaxReconnect))

	reconnectWait := conn.ReconnectWait
	if reconnectWait <= 0 {
		reconnectWait = defaultReconnectWait
	}

	opts = append(opts, natspkg.ReconnectWait(reconnectWait))

	jitter := conn.ReconnectJitter
	if jitter <= 0 {
		jitter = defaultReconnectJitter
	}

	jitterTLS := conn.ReconnectJitterTLS
	if jitterTLS <= 0 {
		jitterTLS = defaultReconnectJitterTLS
	}

	opts = append(opts, natspkg.ReconnectJitter(jitter, jitterTLS))

	delayFn := conn.CustomReconnectDelay
	if delayFn == nil {
		delayFn = cappedExponentialReconnectDelay(reconnectWait)
	}

	opts = append(opts, natspkg.CustomReconnectDelay(delayFn))

	if conn.ReconnectBufSize != 0 {
		opts = append(opts, natspkg.ReconnectBufSize(conn.ReconnectBufSize))
	}

	if conn.RetryOnFailedConnect {
		opts = append(opts, natspkg.RetryOnFailedConnect(true))
	}

	return opts
}

func appendTLSOptions(opts []natspkg.Option, cfg ConnectionTLS) ([]natspkg.Option, error) {
	tlsCfg, err := buildTLSConfig(cfg)
	if err != nil {
		return nil, err
	}

	if tlsCfg == nil {
		return opts, nil
	}

	return append(opts, natspkg.Secure(tlsCfg)), nil
}

func buildTLSConfig(cfg ConnectionTLS) (*tls.Config, error) {
	if cfg.Config != nil {
		return cfg.Config, nil
	}

	hasMaterial := len(cfg.CA) > 0 || len(cfg.Cert) > 0 || len(cfg.Key) > 0 ||
		cfg.InsecureSkipVerify || cfg.ServerName != empty
	if !hasMaterial {
		return nil, nil //nolint:nilnil // no TLS configured is a valid outcome
	}

	tlsCfg := &tls.Config{
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: cfg.InsecureSkipVerify, //nolint:gosec // intentional for ConnectionTLS.InsecureSkipVerify
		ServerName:         cfg.ServerName,
	}

	if len(cfg.CA) > 0 {
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(cfg.CA) {
			return nil, fmt.Errorf("tls: append CA cert from PEM")
		}

		tlsCfg.RootCAs = pool
	}

	if len(cfg.Cert) > 0 || len(cfg.Key) > 0 {
		if len(cfg.Cert) == 0 || len(cfg.Key) == 0 {
			return nil, fmt.Errorf("tls: both cert and key PEM are required")
		}

		cert, err := tls.X509KeyPair(cfg.Cert, cfg.Key)
		if err != nil {
			return nil, fmt.Errorf("tls: load client cert: %w", err)
		}

		tlsCfg.Certificates = []tls.Certificate{cert}
	}

	return tlsCfg, nil
}

func cappedExponentialReconnectDelay(base time.Duration) func(attempts int) time.Duration {
	if base <= 0 {
		base = defaultReconnectWait
	}

	return func(attempts int) time.Duration {
		if attempts < 1 {
			attempts = 1
		}

		delay := base
		for i := 1; i < attempts; i++ {
			if delay >= defaultMaxReconnectDelay/reconnectDelayMultiplier {
				return defaultMaxReconnectDelay
			}

			delay *= reconnectDelayMultiplier
		}

		if delay > defaultMaxReconnectDelay {
			return defaultMaxReconnectDelay
		}

		return delay
	}
}

func (c *client) onConnect(_ *natspkg.Conn) {
	c.mu.Lock()
	c.inLameDuck = false
	c.mu.Unlock()

	slog.InfoContext(c.ctx, "NATS connected")

	if c.metrics != nil && c.metrics.connectionState != nil {
		c.metrics.connectionState.Record(c.ctx, 1)
	}
}

func (c *client) onDisconnect(_ *natspkg.Conn, err error) {
	c.mu.Lock()
	c.lastError = err
	c.lastDisconnect = time.Now()
	c.mu.Unlock()

	if err != nil {
		slog.ErrorContext(c.ctx, "NATS disconnected", slog.String("err", err.Error()))
	} else {
		slog.WarnContext(c.ctx, "NATS disconnected")
	}

	if c.metrics != nil && c.metrics.connectionState != nil {
		c.metrics.connectionState.Record(c.ctx, 0)
	}
}

func (c *client) onReconnect(_ *natspkg.Conn) {
	atomic.AddInt64(&c.reconnectCount, 1)

	c.mu.Lock()
	c.inLameDuck = false
	c.mu.Unlock()

	slog.InfoContext(c.ctx, "NATS reconnected")

	if c.metrics != nil && c.metrics.connectionState != nil {
		c.metrics.connectionState.Record(c.ctx, 1)
	}

	if c.metrics != nil && c.metrics.reconnectCount != nil {
		c.metrics.reconnectCount.Add(c.ctx, 1)
	}
}

func (c *client) onReconnectErr(_ *natspkg.Conn, err error) {
	if err != nil {
		c.mu.Lock()
		c.lastError = err
		c.mu.Unlock()

		slog.ErrorContext(c.ctx, "NATS reconnect error", slog.String("err", err.Error()))
	}
}

func (c *client) onClosed(nc *natspkg.Conn) {
	c.mu.Lock()

	if nc != nil {
		if err := nc.LastError(); err != nil {
			c.lastError = err
		}
	}

	c.inLameDuck = false
	c.mu.Unlock()

	slog.InfoContext(c.ctx, "NATS connection closed")

	if c.metrics != nil && c.metrics.connectionState != nil {
		c.metrics.connectionState.Record(c.ctx, 0)
	}
}

func (c *client) onError(_ *natspkg.Conn, _ *natspkg.Subscription, err error) {
	if err == nil {
		return
	}

	c.mu.Lock()
	c.lastError = err
	c.mu.Unlock()

	slog.ErrorContext(c.ctx, "NATS async error", slog.String("err", err.Error()))

	if c.metrics != nil && c.metrics.connectionErrors != nil {
		c.metrics.connectionErrors.Add(c.ctx, 1)
	}

	if errors.Is(err, natspkg.ErrConsumerNotActive) &&
		c.metrics != nil && c.metrics.idleHeartbeatMisses != nil {
		c.metrics.idleHeartbeatMisses.Add(c.ctx, 1)
	}
}

func (c *client) onLameDuck(_ *natspkg.Conn) {
	c.mu.Lock()
	c.inLameDuck = true
	c.mu.Unlock()

	slog.WarnContext(c.ctx, "NATS server entered lame duck mode; drain or fail over before hard close")

	if c.metrics != nil && c.metrics.lameDuckEvents != nil {
		c.metrics.lameDuckEvents.Add(c.ctx, 1)
	}
}

func (c *client) IsConnected() bool {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return c.conn != nil && c.conn.IsConnected()
}

func (c *client) WaitConnected(ctx context.Context) error {
	c.mu.RLock()
	nc := c.conn
	c.mu.RUnlock()

	if nc == nil {
		return ErrNatsConnectionNotEstablished
	}

	if nc.IsConnected() {
		return nil
	}

	ch := nc.StatusChanged(natspkg.CONNECTED, natspkg.CLOSED)
	defer nc.RemoveStatusListener(ch)

	if nc.IsConnected() {
		return nil
	}

	for {
		select {
		case <-ctx.Done():
			return fmt.Errorf("wait connected: %w", ctx.Err())
		case status, ok := <-ch:
			if !ok {
				return ErrNatsConnectionNotEstablished
			}

			switch status {
			case natspkg.CONNECTED:
				return nil
			case natspkg.CLOSED:
				return ErrNatsConnectionNotEstablished
			case natspkg.DISCONNECTED, natspkg.RECONNECTING, natspkg.CONNECTING,
				natspkg.DRAINING_SUBS, natspkg.DRAINING_PUBS:
				// Keep waiting for CONNECTED or CLOSED.
			}
		}
	}
}

func (c *client) HealthCheck(_ context.Context) error {
	if !c.IsConnected() {
		return ErrNatsConnectionNotEstablished
	}

	if c.js != nil {
		if _, err := c.js.AccountInfo(); err != nil {
			return fmt.Errorf("health check: %w", err)
		}
	}

	return nil
}

func (c *client) ConnectionStatus() ConnectionStatus {
	c.mu.RLock()
	defer c.mu.RUnlock()

	status := ConnectionStatus{
		Connected:      c.conn != nil && c.conn.IsConnected(),
		ReconnectCount: atomic.LoadInt64(&c.reconnectCount),
		LastError:      c.lastError,
		LastDisconnect: c.lastDisconnect,
		InLameDuck:     c.inLameDuck,
	}
	if status.Connected && c.conn != nil {
		status.ServerURL = redactURLString(c.conn.ConnectedUrl())
	}

	return status
}

func (c *client) startHealthCheck() {
	if c.config.Conn.HealthCheckInterval <= 0 {
		return
	}

	c.healthCheck = time.NewTicker(c.config.Conn.HealthCheckInterval)

	go func() {
		defer c.healthCheck.Stop()

		for {
			select {
			case <-c.ctx.Done():
				return
			case <-c.healthCheck.C:
				err := c.HealthCheck(c.ctx)
				if err != nil {
					slog.WarnContext(c.ctx, "health check failed", slog.String("err", err.Error()))
				}
			}
		}
	}()
}

func (c *client) Shutdown() error {
	var err error

	c.shutdownOnce.Do(func() {
		err = c.shutdownGraceful()
	})

	return err
}

// shutdownGraceful stops background work, drains in-flight consumers while the
// connection is still up (so acks can complete), then drains/flushes the NATS
// connection. The client context is cancelled last so handlers are not aborted
// mid-flight by a parent cancel.
func (c *client) shutdownGraceful() error {
	var errs []error

	if c.healthCheck != nil {
		c.healthCheck.Stop()
		c.healthCheck = nil
	}

	if c.collector != nil {
		c.collector.stop()
		c.collector = nil
	}

	// Finish in-flight message handlers while the connection can still ack/nak.
	if c.consumer != nil {
		c.consumer.stop()
	}

	if c.publisher != nil {
		c.publisher.stop()
	}

	if err := c.drainAndClose(); err != nil {
		errs = append(errs, err)
	}

	if c.cancel != nil {
		c.cancel()
		c.cancel = nil
	}

	return errors.Join(errs...)
}

func (c *client) drainAndClose() error {
	c.mu.Lock()
	nc := c.conn
	c.conn = nil
	c.mu.Unlock()

	if nc == nil {
		return nil
	}

	timeout := c.config.Conn.DrainTimeout
	if timeout <= 0 {
		timeout = defaultDrainTimeout
	}

	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan error, 1)

	go func() { done <- nc.Drain() }()

	select {
	case err := <-done:
		if err != nil {
			nc.Close()

			return fmt.Errorf("drain: %w", err)
		}
		// Drain already closed the connection on success.
		return nil
	case <-ctx.Done():
		slog.WarnContext(c.ctx, "drain timeout, forcing close",
			slog.Duration("timeout", timeout))
		nc.Close()

		return fmt.Errorf("%w after %s: %w", ErrDrainTimeout, timeout, ctx.Err())
	}
}

var _ Connector = (*client)(nil)

type client struct {
	lastDisconnect time.Time
	streams        StreamManager
	consumers      ConsumerManager
	kv             KeyValueManager
	replay         Replay
	js             natspkg.JetStreamContext

	ctx       context.Context
	lastError error
	config    *Config
	publisher *publisher
	consumer  *consumer
	conn      *natspkg.Conn
	metrics   *clientMetrics
	collector *metricsCollector

	cancel context.CancelFunc

	healthCheck    *time.Ticker
	reconnectCount int64

	mu           sync.RWMutex
	inLameDuck   bool
	shutdownOnce sync.Once
}
