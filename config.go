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
)

type Config struct {
	PublisherConfig  PublisherConfig
	RequesterConfig  RequesterConfig
	ResponderConfig  ResponderConfig
	Metrics          MetricsConfig
	RuntimeConsumer  RuntimeConsumerConfig
	Stream           StreamConfig
	Conn             Connection
	Backpressure     BackpressureConfig
	AdaptivePressure AdaptivePressureConfig
}

// ConsumerConfig is an alias kept for backward compatibility.
type ConsumerConfig = RuntimeConsumerConfig

// ConnectionTLS holds optional TLS material for NATS connections.
// Prefer PEM fields for config-file / env wiring; set Config for full control.
//
// goalign:ignore
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

// Connection holds dial, auth, TLS, and reconnect settings.
//
// goalign:ignore
type Connection struct {
	// CustomReconnectDelay overrides the built-in capped exponential delay.
	CustomReconnectDelay func(attempts int) time.Duration

	// Optional hooks run after the library's internal connection handlers.
	OnDisconnect func(*natspkg.Conn, error)
	OnReconnect  func(*natspkg.Conn)
	OnClosed     func(*natspkg.Conn)

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

// RuntimeConsumerConfig holds push/pull worker and heartbeat settings.
//
// goalign:ignore
type RuntimeConsumerConfig struct {
	MetricPrefix      string
	PendingMsgLimit   int
	PendingMsgBuffer  int
	WorkerPoolSize    int
	WorkerBufferSize  int
	AckWait           time.Duration
	IdleHeartbeat     time.Duration // 0 disables; unsupported on queue groups
	WorkerPoolEnabled bool
	FlowControl       bool
	AllowMetrics      bool
	AllowTracing      bool
	// PayloadDecompression expands Content-Encoding (br/gzip) before the handler
	// sees msg.Data. DefaultConfig enables this so compressed publishers interoperate.
	PayloadDecompression bool
}

// PublisherConfig holds publish metrics and async pending limits.
//
// goalign:ignore
type PublisherConfig struct {
	MetricPrefix string
	AllowMetrics bool
	AllowTracing bool
	// SkipSubjectValidation skips per-publish subject validation for trusted static subjects.
	SkipSubjectValidation bool
	// MaxAsyncPending caps in-flight PublishAsync requests (0 = defaultMaxAsyncPending, -1 = unlimited).
	MaxAsyncPending int
	// PayloadCompression opt-in auto compression (br→gzip) for payloads >32 KiB.
	PayloadCompression PayloadCompressionMode
}

// RequesterConfig configures core NATS request/reply client calls.
//
// goalign:ignore
type RequesterConfig struct {
	MetricPrefix string
	// Timeout is used when ctx has no deadline (0 = defaultRequestTimeout).
	Timeout      time.Duration
	AllowMetrics bool
	AllowTracing bool
	// SkipSubjectValidation skips per-request subject validation for trusted static subjects.
	SkipSubjectValidation bool
	// PayloadCompression opt-in compression for outbound requests (>32 KiB, shrink-only).
	PayloadCompression PayloadCompressionMode
	// PayloadDecompression expands Content-Encoding on replies before return / Into (default on).
	PayloadDecompression bool
}

// ResponderConfig configures core NATS reply subscribers.
//
// goalign:ignore
type ResponderConfig struct {
	MetricPrefix string
	AllowMetrics bool
	AllowTracing bool
	// PayloadCompression opt-in compression for Responder.Respond* replies (>32 KiB, shrink-only).
	PayloadCompression PayloadCompressionMode
	// PayloadDecompression expands Content-Encoding on inbound requests before the handler (default on).
	PayloadDecompression bool
}

// StreamConfig holds JetStream stream topology fields for explicit provisioning.
//
// goalign:ignore
type StreamConfig struct {
	Mirror          *StreamSource
	Name            string
	Description     string
	Subjects        []string
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
//
// goalign:ignore
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

// DurableConsumerConfig holds durable consumer create/update options.
//
// goalign:ignore
type DurableConsumerConfig struct {
	OptStartTime   *time.Time
	Metadata       map[string]string
	Durable        string
	FilterSubject  string
	FilterSubjects []string
	// DeliverSubject / DeliverGroup preserve push consumers across ResetConsumer.
	// Empty DeliverSubject means pull.
	DeliverSubject    string
	DeliverGroup      string
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
	// HasAckPolicy marks AckPolicy as intentionally set so AckNone (zero) is preserved.
	HasAckPolicy bool
	// HasDeliverPolicy marks DeliverPolicy as intentionally set (including DeliverAll).
	HasDeliverPolicy bool
	// HasReplayPolicy marks ReplayPolicy as intentionally set (including ReplayInstant).
	HasReplayPolicy bool
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
//
// Zero-value JetStream enums (DeliverAll, ReplayInstant, AckNone) are valid;
// set flags track whether an option explicitly chose a policy or start position.
//
// goalign:ignore
type ReplayConfig struct {
	OptStartTime    *time.Time
	UntilTime       *time.Time
	Durable         string // target durable for CreateReplayConsumer; ignored by ResetConsumer
	FilterSubject   string
	FilterSubjects  []string
	DeliverPolicy   DeliverPolicy
	ReplayPolicy    ReplayPolicy
	OptStartSeq     uint64
	UntilSeq        uint64
	Limit           int
	deliverSet      bool
	replaySet       bool
	optStartSeqSet  bool
	optStartTimeSet bool
	untilSeqSet     bool
	untilTimeSet    bool
	limitSet        bool
}

// MetricsConfig holds JetStream metrics collection options.
//
// goalign:ignore
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
			// PayloadCompression stays Off; enable PayloadCompressionAuto explicitly.
		},
		RequesterConfig: RequesterConfig{
			Timeout:              defaultRequestTimeout,
			AllowMetrics:         true,
			AllowTracing:         true,
			MetricPrefix:         defaultMetricPrefix,
			PayloadDecompression: true,
			// PayloadCompression stays Off; enable Auto/Gzip/Brotli explicitly.
		},
		ResponderConfig: ResponderConfig{
			AllowMetrics:         true,
			AllowTracing:         true,
			MetricPrefix:         defaultMetricPrefix,
			PayloadDecompression: true,
			// PayloadCompression stays Off; enable Auto/Gzip/Brotli explicitly.
		},
		RuntimeConsumer: RuntimeConsumerConfig{
			AckWait:              defaultAckWait,
			IdleHeartbeat:        defaultIdleHeartbeat,
			FlowControl:          true,
			WorkerPoolSize:       defaultWorkerPoolSize,
			WorkerBufferSize:     defaultWorkerBufferSize,
			AllowMetrics:         true,
			AllowTracing:         true,
			MetricPrefix:         defaultMetricPrefix,
			PayloadDecompression: true,
		},
		Backpressure: BackpressureConfig{
			Mode:                     BackpressureNak,
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
