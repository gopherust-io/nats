package nats

import (
	"context"
	"fmt"

	"github.com/gopherust-io/nats/internal/bytesconv"
	natspkg "github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
)

type Client interface {
	Consumer() Consumer
	Publisher() Publisher
	Requester() Requester
	Responder() Responder
	Connector() Connector
	Streams() StreamManager
	Consumers() ConsumerManager
	KV() KeyValueManager
	KVKeys() KeyValueKeys
	Objects() ObjectStoreManager
	Monitoring() Monitoring
	Replay() Replay
	// PublishRaw publishes bytes with optional headers and returns the JetStream ack.
	PublishRaw(ctx context.Context, subject string, data []byte, headers map[string]string) (*PubAck, error)
	SetupWorker(ctx context.Context, setup WorkerSetup, handler MsgHandler) (Subscription, error)
	SuperviseQueueSubscribeBound(ctx context.Context, stream, durable, queue, subject string, handler MsgHandler, cfg SupervisorConfig) (SupervisedSubscription, error)
	SuperviseSubscribeBound(ctx context.Context, stream, durable, subject string, handler MsgHandler, cfg SupervisorConfig) (SupervisedSubscription, error)
	SupervisePullProcess(ctx context.Context, stream, durable string, handler MsgHandler, cfg SupervisorConfig, opts ...ProcessOpt) error
	WatchSoftLiveness(ctx context.Context, sub Subscription, cfg SoftLivenessConfig) (*SoftLiveness, error)
	WatchBehaviorFingerprint(ctx context.Context, sub Subscription, cfg BehaviorFingerprintConfig) (*BehaviorFingerprint, error)
	// WithShadow wraps handlers and records shadow_* metrics when metrics are enabled.
	WithShadow(cfg ShadowConfig, primary, shadowHandler MsgHandler) MsgHandler
}

func NewClient(ctx context.Context, cfg *Config) (Client, error) {
	if cfg == nil {
		return nil, fmt.Errorf("new client: %w", ErrEmptyConfigNotAllowed)
	}

	if bytesconv.IsEmpty(cfg.Conn.Address) {
		return nil, fmt.Errorf("new client: %w", ErrEmptyAddressNotAllowed)
	}

	if err := validateAuthConfig(cfg.Conn); err != nil {
		return nil, fmt.Errorf("new client: %w", err)
	}

	if cfg.Conn.TLS.InsecureSkipVerify {
		zerolog.Ctx(ctx).Warn().Msg("NATS TLS InsecureSkipVerify enabled; server certificate will not be verified")
	}

	cl := &client{config: cfg}
	cl.ctx, cl.cancel = context.WithCancel(ctx)

	metricsCfg := cfg.Metrics
	if !metricsCfg.AllowMetrics && cfg.Conn.AllowMetrics {
		metricsCfg.AllowMetrics = true
	}

	if bytesconv.IsEmpty(metricsCfg.Prefix) && !bytesconv.IsEmpty(cfg.Conn.MetricPrefix) {
		metricsCfg.Prefix = cfg.Conn.MetricPrefix
	}

	cl.metrics = newClientMetrics(ctx, metricsCfg)

	var (
		nc  *natspkg.Conn
		err error
	)

	if cfg.Conn.InitialRetryAttempts > 0 {
		nc, err = cl.connectWithRetry(ctx)
	} else {
		opts, optsErr := cl.configureOptions()
		if optsErr != nil {
			return nil, fmt.Errorf("connect: %w", optsErr)
		}

		nc, err = natspkg.Connect(cfg.Conn.Address, opts...)
	}

	if err != nil {
		return nil, fmt.Errorf("connect: %w", err)
	}

	js, err := nc.JetStream()
	if err != nil {
		nc.Close()

		return nil, fmt.Errorf("jetstream: %w", err)
	}

	cl.conn = nc
	cl.js = js

	cl.collector = newMetricsCollector(ctx, nc, js, metricsCfg, cl.metrics)

	cl.streams = newStreamManager(js)
	cl.consumers = newConsumerManager(nc, js)
	cl.kv = newKeyValueManager(nc, js)
	cl.objects = newObjectStoreManager(js)
	cl.monitoring = newMonitoring(cfg.Conn.ConnectTimeout)
	cl.replay = newReplay(cl.streams, cl.consumers)
	cl.publisher = newPublisher(ctx, cfg.PublisherConfig, js, nc, cfg.Conn.ReconnectBufSize, cl.metrics, metricsCfg.AllowTracing)
	cl.consumer = newConsumer(ctx, cfg.RuntimeConsumer, cfg.Backpressure, js, cl.metrics, metricsCfg.AllowTracing)
	cl.requester = newRequester(cfg.RequesterConfig, nc, cl.metrics, metricsCfg.AllowTracing)
	cl.responder = newResponder(cfg.ResponderConfig, nc, cl.metrics, metricsCfg.AllowTracing)

	if cfg.Conn.HealthCheckInterval > 0 {
		cl.startHealthCheck()
	}

	if cl.metrics != nil && cl.metrics.connectionState != nil {
		if nc.IsConnected() {
			cl.metrics.connectionState.Record(ctx, 1)
		} else {
			cl.metrics.connectionState.Record(ctx, 0)
		}
	}

	return cl, nil
}

func (c *client) Consumer() Consumer          { return c.consumer }
func (c *client) Publisher() Publisher        { return c.publisher }
func (c *client) Requester() Requester        { return c.requester }
func (c *client) Responder() Responder        { return c.responder }
func (c *client) Connector() Connector        { return c }
func (c *client) Streams() StreamManager      { return c.streams }
func (c *client) Consumers() ConsumerManager  { return c.consumers }
func (c *client) KV() KeyValueManager         { return c.kv }
func (c *client) KVKeys() KeyValueKeys        { return c.kv }
func (c *client) Objects() ObjectStoreManager { return c.objects }
func (c *client) Monitoring() Monitoring      { return c.monitoring }
func (c *client) Replay() Replay              { return c.replay }

func (c *client) PublishRaw(ctx context.Context, subject string, data []byte, headers map[string]string) (*PubAck, error) {
	return c.publisher.PublishRaw(ctx, subject, data, headers)
}
