package nats

import (
	"crypto/tls"
	"time"

	natspkg "github.com/nats-io/nats.go"
)

const (
	defaultNATSOptionsCapacity = 32
	defaultNATSAddress         = "nats://127.0.0.1:4222"
	defaultMetricPrefix        = "nats"

	defaultDrainTimeout             = 30 * time.Second
	defaultAckWait                  = 30 * time.Second
	defaultIdleHeartbeat            = 5 * time.Second
	defaultFetchMaxWait             = 5 * time.Second
	defaultConnectTimeout           = 5 * time.Second
	defaultPingInterval             = 20 * time.Second
	defaultMaxPingsOut              = 3
	defaultMaxReconnect             = -1 // unlimited
	defaultReconnectWait            = time.Second
	defaultReconnectBufSize         = 16 << 20 // 16 MiB
	defaultMaxReconnectDelay        = 30 * time.Second
	defaultReconnectJitter          = 100 * time.Millisecond
	defaultReconnectJitterTLS       = time.Second
	defaultInitialRetryAttempts     = 5
	defaultInitialRetryWait         = time.Second
	defaultInitialRetryBackoff      = 2.0
	reconnectDelayMultiplier        = 2
	defaultFetchBatch               = 10
	defaultMaxAsyncPending          = 1024
	defaultStreamReplicas           = 1
	defaultWorkerPoolSize           = 4
	defaultWorkerBufferSize         = 64
	defaultMaxAckPending            = 1000
	defaultQueueDepthSampleInterval = 5 * time.Second
	defaultMetricsCollectInterval   = 15 * time.Second
	defaultRequestTimeout           = 2 * time.Second

	devInitialRetryAttempts = 1
	devWorkerPoolSize       = 2
	devWorkerBufferSize     = 32

	prodWorkerPoolSize      = 8
	prodWorkerBufferSize    = 256
	prodWorkerAckWait       = 45 * time.Second
	prodPendingMsgLimit     = 1000
	prodPendingMsgBuffer    = 10 << 20 // 10 MiB
	prodWorkerMaxAckPending = 1000
	prodFanOutMaxAckPending = 500
)

type Config struct {
	PublisherConfig PublisherConfig
	RequesterConfig RequesterConfig
	ResponderConfig ResponderConfig
	Metrics         MetricsConfig
	RuntimeConsumer RuntimeConsumerConfig
	// Stream is an optional topology template for callers; NewClient does not
	// create or update it. Provision streams via nats CLI / platform ops, or
	// explicitly with Streams().CreateOrUpdateStream / SetupWorker.
	Stream       StreamConfig
	Conn         Connection
	Backpressure BackpressureConfig
}

// ConsumerConfig is an alias kept for backward compatibility.
type ConsumerConfig = RuntimeConsumerConfig

// ConnectionTLS holds optional TLS material for NATS connections.
// Prefer PEM fields for config-file / env wiring; set Config for full control.
type ConnectionTLS struct {
	// Config is used as-is when non-nil (takes precedence over PEM fields).
	Config *tls.Config
	// ServerName overrides the hostname used for certificate verification.
	ServerName string
	// CA is a PEM-encoded CA certificate pool.
	CA []byte
	// Cert and Key are a PEM-encoded client certificate pair (mTLS).
	Cert []byte
	Key  []byte
	// InsecureSkipVerify disables server certificate verification (dev only).
	InsecureSkipVerify bool
}

type Connection struct {
	// CustomReconnectDelay overrides the built-in capped exponential delay.
	CustomReconnectDelay func(attempts int) time.Duration

	Address  string
	User     string
	Password string
	Seed     string
	Secret   string
	// CredentialsFile is a NATS .creds / chained credentials file (operator JWT model).
	// Mutually exclusive with Seed, User/Password, and Secret.
	CredentialsFile string
	MetricPrefix    string

	ClientName string
	// TLS configures secure connections (mTLS / CA verification).
	TLS ConnectionTLS

	ReconnectWait      time.Duration
	MaxReconnect       int
	ConnectTimeout     time.Duration
	DrainTimeout       time.Duration
	PingInterval       time.Duration
	MaxPingsOut        int
	FlusherTimeout     time.Duration
	ReconnectJitter    time.Duration
	ReconnectJitterTLS time.Duration

	// ReconnectBufSize is the outbound buffer while reconnecting.
	// 0 leaves the nats.go library default (8 MiB); -1 disables buffering.
	ReconnectBufSize int

	HealthCheckInterval  time.Duration
	InitialRetryAttempts int
	InitialRetryWait     time.Duration
	InitialRetryBackoff  float64
	RetryOnFailedConnect bool
	AllowReconnect       bool
	AllowMetrics         bool
	// DontRandomize keeps server order when Address is a comma-separated list.
	DontRandomize bool
}

type RuntimeConsumerConfig struct {
	MetricPrefix     string
	PendingMsgLimit  int
	PendingMsgBuffer int
	WorkerPoolSize   int
	WorkerBufferSize int
	AckWait          time.Duration
	// IdleHeartbeat enables JetStream idle heartbeats on non-queue push
	// subscriptions (and defaults pull Process heartbeats). 0 disables.
	// Queue groups do not support idle heartbeat (nats.go limitation).
	IdleHeartbeat     time.Duration
	WorkerPoolEnabled bool
	// FlowControl enables JetStream flow control on non-queue push when
	// IdleHeartbeat > 0. Pair with IdleHeartbeat (recommended by nats.go).
	FlowControl  bool
	AllowMetrics bool
	AllowTracing bool
}

type PublisherConfig struct {
	MetricPrefix string
	AllowMetrics bool
	AllowTracing bool
	// SkipSubjectValidation skips per-publish subject validation for trusted static subjects.
	SkipSubjectValidation bool
	// MaxAsyncPending caps in-flight PublishAsync requests (0 = defaultMaxAsyncPending, -1 = unlimited).
	MaxAsyncPending int
}

// RequesterConfig configures core NATS request/reply client calls.
type RequesterConfig struct {
	MetricPrefix string
	// Timeout is used when ctx has no deadline (0 = defaultRequestTimeout).
	Timeout      time.Duration
	AllowMetrics bool
	AllowTracing bool
	// SkipSubjectValidation skips per-request subject validation for trusted static subjects.
	SkipSubjectValidation bool
}

// ResponderConfig configures core NATS reply subscribers.
type ResponderConfig struct {
	MetricPrefix string
	AllowMetrics bool
	AllowTracing bool
}

type StreamConfig struct {
	// Mirror configures this stream as a mirror of another stream (geo / DR).
	// Prefer nats CLI or platform ops for complex cross-domain setups; see devops.md.
	Mirror      *StreamSource
	Name        string
	Description string
	Subjects    []string
	// Sources aggregates messages from other streams into this stream.
	Sources         []*StreamSource
	Storage         StorageType
	MaxBytes        int64
	MaxAge          time.Duration
	MaxMsgs         int64
	Discard         DiscardPolicy
	DuplicateWindow time.Duration
	MaxConsumers    int
	Retention       RetentionPolicy
	Replicas        int
	MaxMsgSize      int32
	NoAck           bool
}

// StreamSource is a JetStream stream source or mirror origin.
type StreamSource = natspkg.StreamSource

// KeyValueConfig configures a JetStream Key-Value bucket.
type KeyValueConfig struct {
	Bucket      string
	Description string
	TTL         time.Duration
	MaxBytes    int64
	Storage     StorageType
	Replicas    int
	History     uint8 // defaults to 1 when zero
	Compression bool
}

type DurableConsumerConfig struct {
	OptStartTime      *time.Time
	Durable           string
	FilterSubject     string
	FilterSubjects    []string
	DeliverPolicy     DeliverPolicy
	ReplayPolicy      ReplayPolicy
	AckPolicy         AckPolicy
	MaxDeliver        int
	AckWait           time.Duration
	MaxAckPending     int
	RateLimit         uint64
	Heartbeat         time.Duration
	InactiveThreshold time.Duration
	Replicas          int
	MaxWaiting        int
	OptStartSeq       uint64
	FlowControl       bool
	MemStorage        bool
}

type BackpressureConfig struct {
	Mode                     BackpressureMode
	MaxAckPending            int
	PendingMsgLimit          int
	PendingMsgBuffer         int
	QueueDepthSampleInterval time.Duration
}

// ReplayConfig holds replay seek settings. Prefer ReplayOpt helpers
// (FromSeq, FromTime, WithReplayPolicy, …) at call sites.
type ReplayConfig struct {
	OptStartTime   *time.Time
	Durable        string // target durable for CreateReplayConsumer; ignored by ResetConsumer
	FilterSubject  string
	FilterSubjects []string
	DeliverPolicy  DeliverPolicy
	ReplayPolicy   ReplayPolicy
	OptStartSeq    uint64
}

type MetricsConfig struct {
	Prefix           string
	TrackedStreams   []string
	TrackedConsumers []TrackedConsumer
	CollectInterval  time.Duration
	AllowMetrics     bool
	AllowTracing     bool
	// Lite registers only publish/consume/ack counters (skips JetStream gauges and RTT).
	Lite bool
	// FixedCardinality records hot-path metrics without per-subject attributes.
	FixedCardinality bool
}

type TrackedConsumer struct {
	Stream  string
	Durable string
}

func DefaultConfig() Config {
	return Config{
		Conn: Connection{
			Address:              defaultNATSAddress,
			AllowReconnect:       true,
			MaxReconnect:         defaultMaxReconnect,
			ReconnectWait:        defaultReconnectWait,
			ReconnectJitter:      defaultReconnectJitter,
			ReconnectJitterTLS:   defaultReconnectJitterTLS,
			ReconnectBufSize:     defaultReconnectBufSize,
			PingInterval:         defaultPingInterval,
			MaxPingsOut:          defaultMaxPingsOut,
			RetryOnFailedConnect: true,
			ConnectTimeout:       defaultConnectTimeout,
			DrainTimeout:         defaultDrainTimeout,
			InitialRetryAttempts: defaultInitialRetryAttempts,
			InitialRetryWait:     defaultInitialRetryWait,
			InitialRetryBackoff:  defaultInitialRetryBackoff,
			AllowMetrics:         true,
			MetricPrefix:         defaultMetricPrefix,
		},
		PublisherConfig: PublisherConfig{
			AllowMetrics: true,
			AllowTracing: true,
			MetricPrefix: defaultMetricPrefix,
		},
		RequesterConfig: RequesterConfig{
			Timeout:      defaultRequestTimeout,
			AllowMetrics: true,
			AllowTracing: true,
			MetricPrefix: defaultMetricPrefix,
		},
		ResponderConfig: ResponderConfig{
			AllowMetrics: true,
			AllowTracing: true,
			MetricPrefix: defaultMetricPrefix,
		},
		RuntimeConsumer: RuntimeConsumerConfig{
			AckWait:          defaultAckWait,
			IdleHeartbeat:    defaultIdleHeartbeat,
			FlowControl:      true,
			WorkerPoolSize:   defaultWorkerPoolSize,
			WorkerBufferSize: defaultWorkerBufferSize,
			AllowMetrics:     true,
			AllowTracing:     true,
			MetricPrefix:     defaultMetricPrefix,
		},
		Backpressure: BackpressureConfig{
			Mode:                     BackpressureBlock,
			MaxAckPending:            defaultMaxAckPending,
			QueueDepthSampleInterval: defaultQueueDepthSampleInterval,
		},
		Metrics: MetricsConfig{
			AllowMetrics:    true,
			AllowTracing:    true,
			Prefix:          defaultMetricPrefix,
			CollectInterval: defaultMetricsCollectInterval,
		},
	}
}

// DevConfig returns a minimal local-development configuration.
func DevConfig() Config {
	cfg := DefaultConfig()
	cfg.Conn.AllowReconnect = false
	cfg.Conn.RetryOnFailedConnect = false
	cfg.Conn.InitialRetryAttempts = devInitialRetryAttempts
	cfg.Metrics.AllowMetrics = false
	cfg.Metrics.AllowTracing = false
	cfg.Conn.AllowMetrics = false
	cfg.PublisherConfig.AllowMetrics = false
	cfg.PublisherConfig.AllowTracing = false
	cfg.RequesterConfig.AllowMetrics = false
	cfg.RequesterConfig.AllowTracing = false
	cfg.ResponderConfig.AllowMetrics = false
	cfg.ResponderConfig.AllowTracing = false
	cfg.RuntimeConsumer.AllowMetrics = false
	cfg.RuntimeConsumer.AllowTracing = false
	cfg.RuntimeConsumer.WorkerPoolEnabled = true
	cfg.RuntimeConsumer.WorkerPoolSize = devWorkerPoolSize
	cfg.RuntimeConsumer.WorkerBufferSize = devWorkerBufferSize

	return cfg
}

// ProdWorkerConfig returns a production job-queue worker configuration.
func ProdWorkerConfig() Config {
	cfg := DefaultConfig()
	cfg.RuntimeConsumer.WorkerPoolEnabled = true
	cfg.RuntimeConsumer.WorkerPoolSize = prodWorkerPoolSize
	cfg.RuntimeConsumer.WorkerBufferSize = prodWorkerBufferSize
	cfg.RuntimeConsumer.AckWait = prodWorkerAckWait
	cfg.RuntimeConsumer.PendingMsgLimit = prodPendingMsgLimit
	cfg.RuntimeConsumer.PendingMsgBuffer = prodPendingMsgBuffer
	cfg.Backpressure.Mode = BackpressureNak
	cfg.Backpressure.MaxAckPending = prodWorkerMaxAckPending

	return cfg
}

// ProdFanOutConfig returns a production event-bus configuration.
func ProdFanOutConfig() Config {
	cfg := DefaultConfig()
	cfg.Backpressure.Mode = BackpressureBlock
	cfg.Backpressure.MaxAckPending = prodFanOutMaxAckPending

	return cfg
}

const throughputCollectInterval = 60 * time.Second

// ThroughputConfig returns a job-queue preset tuned for max publish/consume throughput.
// Observability is minimized; prefer Proto/PublishBytes on the application hot path.
// ReconnectBufSize stays at the resilient default (16 MiB); set Conn.ReconnectBufSize to -1
// for fail-fast publishers that must not buffer while disconnected.
func ThroughputConfig() Config {
	cfg := ProdWorkerConfig()
	cfg.PublisherConfig.AllowMetrics = false
	cfg.PublisherConfig.AllowTracing = false
	cfg.PublisherConfig.SkipSubjectValidation = true
	cfg.RequesterConfig.AllowMetrics = false
	cfg.RequesterConfig.AllowTracing = false
	cfg.RequesterConfig.SkipSubjectValidation = true
	cfg.ResponderConfig.AllowMetrics = false
	cfg.ResponderConfig.AllowTracing = false
	cfg.RuntimeConsumer.AllowMetrics = false
	cfg.RuntimeConsumer.AllowTracing = false
	cfg.Conn.AllowMetrics = false
	cfg.Metrics.AllowMetrics = false
	cfg.Metrics.AllowTracing = false
	cfg.Metrics.CollectInterval = throughputCollectInterval
	cfg.Metrics.Lite = true
	cfg.Metrics.FixedCardinality = true
	cfg.Metrics.TrackedStreams = nil
	cfg.Metrics.TrackedConsumers = nil

	return cfg
}
