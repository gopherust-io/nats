package nats

import (
	"context"
	"sync"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/rs/zerolog"

	"github.com/gopherust-io/tel"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

// goalign:ignore
type clientMetrics struct {
	registry *tel.Registry

	connectionState            *tel.FastGauge
	reconnectCount             *tel.FastCounter
	connectionInBytes          *tel.FastCounter
	connectionOutBytes         *tel.FastCounter
	connectionErrors           *tel.FastCounter
	lameDuckEvents             *tel.FastCounter
	idleHeartbeatMisses        *tel.FastCounter
	resubscribeTotal           *tel.FastCounter
	supervisorGiveUp           *tel.FastCounter
	consumerStall              *tel.FastCounter
	slowConsumerDetected       *tel.FastCounter
	behaviorFingerprintAnomaly *tel.FastCounter
	connectionRTT              *tel.FastHistogram
	jsMemoryBytes              *tel.FastGauge
	jsStorageBytes             *tel.FastGauge

	publishTotal   *tel.FastCounter
	publishErrors  *tel.FastCounter
	publishLatency *tel.FastHistogram
	publishBytes   *tel.FastHistogram

	requestTotal   *tel.FastCounter
	requestErrors  *tel.FastCounter
	requestLatency *tel.FastHistogram
	requestBytes   *tel.FastHistogram

	replyHandled *tel.FastCounter
	replyErrors  *tel.FastCounter

	messagesReceived     *tel.FastCounter
	messagesErrors       *tel.FastCounter
	handlingTime         *tel.FastHistogram
	messageBytes         *tel.FastHistogram
	redeliveryTotal      *tel.FastCounter
	ackTotal             *tel.FastCounter
	nakTotal             *tel.FastCounter
	termTotal            *tel.FastCounter
	shadowErrorTotal     *tel.FastCounter
	shadowMismatchTotal  *tel.FastCounter
	workerQueueDepth     *tel.FastGauge
	slowConsumerEvents   *tel.FastCounter
	fetchBatchSize       *tel.FastHistogram
	fetchWaitTime        *tel.FastHistogram
	pullBatchProcessTime *tel.FastHistogram
	pullBatchInflight    *tel.FastGauge

	streamMessages      *tel.FastGauge
	streamBytes         *tel.FastGauge
	streamFirstSeq      *tel.FastGauge
	streamLastSeq       *tel.FastGauge
	streamConsumerCount *tel.FastGauge
	streamReplicaCount  *tel.FastGauge

	consumerNumPending     *tel.FastGauge
	consumerNumAckPending  *tel.FastGauge
	consumerNumRedelivered *tel.FastGauge
	consumerAckFloor       *tel.FastGauge
	consumerLag            *tel.FastGauge

	fixedCardinality bool
	lite             bool
}

func newClientMetrics(ctx context.Context, cfg MetricsConfig) *clientMetrics {
	if !cfg.AllowMetrics {
		return nil
	}

	prefix := cfg.Prefix
	if bytesconv.IsEmpty(prefix) {
		prefix = defaultMetricPrefix
	}

	telem := tel.FromCtx(ctx)
	registry := telem.Registry()
	cm := &clientMetrics{
		registry:         registry,
		fixedCardinality: cfg.FixedCardinality,
		lite:             cfg.Lite,
	}

	must := func(name string, fn func() error) {
		err := fn()
		if err != nil {
			zerolog.Ctx(ctx).Warn().Str("name", name).Err(err).Msg("failed to create metric")
		}
	}

	must(prefix+"/connection_state", func() error {
		var err error

		cm.connectionState, err = registry.Gauge(prefix + "/connection_state")

		return err
	})
	must(prefix+"/reconnect_count", func() error {
		var err error

		cm.reconnectCount, err = registry.Counter(prefix + "/reconnect_count")

		return err
	})
	must(prefix+"/connection_in_bytes", func() error {
		var err error

		cm.connectionInBytes, err = registry.Counter(prefix + "/connection_in_bytes")

		return err
	})
	must(prefix+"/connection_out_bytes", func() error {
		var err error

		cm.connectionOutBytes, err = registry.Counter(prefix + "/connection_out_bytes")

		return err
	})
	must(prefix+"/connection_errors", func() error {
		var err error

		cm.connectionErrors, err = registry.Counter(prefix + "/connection_errors")

		return err
	})
	must(prefix+"/lame_duck_events", func() error {
		var err error

		cm.lameDuckEvents, err = registry.Counter(prefix + "/lame_duck_events")

		return err
	})
	must(prefix+"/idle_heartbeat_misses", func() error {
		var err error

		cm.idleHeartbeatMisses, err = registry.Counter(prefix + "/idle_heartbeat_misses")

		return err
	})
	must(prefix+"/resubscribe_total", func() error {
		var err error

		cm.resubscribeTotal, err = registry.Counter(prefix + "/resubscribe_total")

		return err
	})
	must(prefix+"/supervisor_give_up", func() error {
		var err error

		cm.supervisorGiveUp, err = registry.Counter(prefix + "/supervisor_give_up")

		return err
	})
	must(prefix+"/consumer_stall", func() error {
		var err error

		cm.consumerStall, err = registry.Counter(prefix + "/consumer_stall")

		return err
	})
	must(prefix+"/slow_consumer_detected", func() error {
		var err error

		cm.slowConsumerDetected, err = registry.Counter(prefix + "/slow_consumer_detected")

		return err
	})
	must(prefix+"/behavior_fingerprint_anomaly", func() error {
		var err error

		cm.behaviorFingerprintAnomaly, err = registry.Counter(prefix + "/behavior_fingerprint_anomaly")

		return err
	})
	if !cfg.Lite {
		must(prefix+"/connection_rtt_seconds", func() error {
			var err error

			cm.connectionRTT, err = registry.Histogram(prefix + "/connection_rtt_seconds")

			return err
		})
		must(prefix+"/jetstream_memory_bytes", func() error {
			var err error

			cm.jsMemoryBytes, err = registry.Gauge(prefix + "/jetstream_memory_bytes")

			return err
		})
		must(prefix+"/jetstream_storage_bytes", func() error {
			var err error

			cm.jsStorageBytes, err = registry.Gauge(prefix + "/jetstream_storage_bytes")

			return err
		})
	}
	must(prefix+"/publish_total", func() error {
		var err error

		cm.publishTotal, err = registry.Counter(prefix + "/publish_total")

		return err
	})
	must(prefix+"/publish_errors", func() error {
		var err error

		cm.publishErrors, err = registry.Counter(prefix + "/publish_errors")

		return err
	})
	must(prefix+"/publish_latency_seconds", func() error {
		var err error

		cm.publishLatency, err = registry.Histogram(prefix + "/publish_latency_seconds")

		return err
	})
	must(prefix+"/publish_bytes", func() error {
		var err error

		cm.publishBytes, err = registry.Histogram(prefix + "/publish_bytes")

		return err
	})
	must(prefix+"/request_total", func() error {
		var err error

		cm.requestTotal, err = registry.Counter(prefix + "/request_total")

		return err
	})
	must(prefix+"/request_errors", func() error {
		var err error

		cm.requestErrors, err = registry.Counter(prefix + "/request_errors")

		return err
	})
	must(prefix+"/request_latency_seconds", func() error {
		var err error

		cm.requestLatency, err = registry.Histogram(prefix + "/request_latency_seconds")

		return err
	})
	must(prefix+"/request_bytes", func() error {
		var err error

		cm.requestBytes, err = registry.Histogram(prefix + "/request_bytes")

		return err
	})
	must(prefix+"/reply_handled", func() error {
		var err error

		cm.replyHandled, err = registry.Counter(prefix + "/reply_handled")

		return err
	})
	must(prefix+"/reply_errors", func() error {
		var err error

		cm.replyErrors, err = registry.Counter(prefix + "/reply_errors")

		return err
	})
	must(prefix+"/messages_received", func() error {
		var err error

		cm.messagesReceived, err = registry.Counter(prefix + "/messages_received")

		return err
	})
	must(prefix+"/messages_errors", func() error {
		var err error

		cm.messagesErrors, err = registry.Counter(prefix + "/messages_errors")

		return err
	})
	must(prefix+"/message_handling_seconds", func() error {
		var err error

		cm.handlingTime, err = registry.Histogram(prefix + "/message_handling_seconds")

		return err
	})
	must(prefix+"/message_bytes", func() error {
		var err error

		cm.messageBytes, err = registry.Histogram(prefix + "/message_bytes")

		return err
	})
	must(prefix+"/redelivery_total", func() error {
		var err error

		cm.redeliveryTotal, err = registry.Counter(prefix + "/redelivery_total")

		return err
	})
	must(prefix+"/ack_total", func() error {
		var err error

		cm.ackTotal, err = registry.Counter(prefix + "/ack_total")

		return err
	})
	must(prefix+"/nak_total", func() error {
		var err error

		cm.nakTotal, err = registry.Counter(prefix + "/nak_total")

		return err
	})
	must(prefix+"/term_total", func() error {
		var err error

		cm.termTotal, err = registry.Counter(prefix + "/term_total")

		return err
	})
	must(prefix+"/shadow_error_total", func() error {
		var err error

		cm.shadowErrorTotal, err = registry.Counter(prefix + "/shadow_error_total")

		return err
	})
	must(prefix+"/shadow_mismatch_total", func() error {
		var err error

		cm.shadowMismatchTotal, err = registry.Counter(prefix + "/shadow_mismatch_total")

		return err
	})
	must(prefix+"/worker_queue_depth", func() error {
		var err error

		cm.workerQueueDepth, err = registry.Gauge(prefix + "/worker_queue_depth")

		return err
	})
	must(prefix+"/slow_consumer_events", func() error {
		var err error

		cm.slowConsumerEvents, err = registry.Counter(prefix + "/slow_consumer_events")

		return err
	})
	must(prefix+"/fetch_batch_size", func() error {
		var err error

		cm.fetchBatchSize, err = registry.Histogram(prefix + "/fetch_batch_size")

		return err
	})
	must(prefix+"/fetch_wait_seconds", func() error {
		var err error

		cm.fetchWaitTime, err = registry.Histogram(prefix + "/fetch_wait_seconds")

		return err
	})
	must(prefix+"/pull_batch_process_seconds", func() error {
		var err error

		cm.pullBatchProcessTime, err = registry.Histogram(prefix + "/pull_batch_process_seconds")

		return err
	})
	must(prefix+"/pull_batch_inflight", func() error {
		var err error

		cm.pullBatchInflight, err = registry.Gauge(prefix + "/pull_batch_inflight")

		return err
	})
	if !cfg.Lite {
		must(prefix+"/stream_messages", func() error {
			var err error

			cm.streamMessages, err = registry.Gauge(prefix + "/stream_messages")

			return err
		})
		must(prefix+"/stream_bytes", func() error {
			var err error

			cm.streamBytes, err = registry.Gauge(prefix + "/stream_bytes")

			return err
		})
		must(prefix+"/stream_first_seq", func() error {
			var err error

			cm.streamFirstSeq, err = registry.Gauge(prefix + "/stream_first_seq")

			return err
		})
		must(prefix+"/stream_last_seq", func() error {
			var err error

			cm.streamLastSeq, err = registry.Gauge(prefix + "/stream_last_seq")

			return err
		})
		must(prefix+"/stream_consumer_count", func() error {
			var err error

			cm.streamConsumerCount, err = registry.Gauge(prefix + "/stream_consumer_count")

			return err
		})
		must(prefix+"/stream_replica_count", func() error {
			var err error

			cm.streamReplicaCount, err = registry.Gauge(prefix + "/stream_replica_count")

			return err
		})
		must(prefix+"/consumer_num_pending", func() error {
			var err error

			cm.consumerNumPending, err = registry.Gauge(prefix + "/consumer_num_pending")

			return err
		})
		must(prefix+"/consumer_num_ack_pending", func() error {
			var err error

			cm.consumerNumAckPending, err = registry.Gauge(prefix + "/consumer_num_ack_pending")

			return err
		})
		must(prefix+"/consumer_num_redelivered", func() error {
			var err error

			cm.consumerNumRedelivered, err = registry.Gauge(prefix + "/consumer_num_redelivered")

			return err
		})
		must(prefix+"/consumer_ack_floor", func() error {
			var err error

			cm.consumerAckFloor, err = registry.Gauge(prefix + "/consumer_ack_floor")

			return err
		})
		must(prefix+"/consumer_lag_messages", func() error {
			var err error

			cm.consumerLag, err = registry.Gauge(prefix + "/consumer_lag_messages")

			return err
		})
	}

	return cm
}

type metricsCollector struct {
	js      natspkg.JetStreamContext
	ctx     context.Context
	conn    *natspkg.Conn
	metrics *clientMetrics
	cancel  context.CancelFunc
	cfg     MetricsConfig
	wg      sync.WaitGroup

	lastInBytes  uint64
	lastOutBytes uint64
}

func newMetricsCollector(
	parent context.Context,
	conn *natspkg.Conn,
	js natspkg.JetStreamContext,
	cfg MetricsConfig,
	metrics *clientMetrics,
) *metricsCollector {
	if metrics == nil || !cfg.AllowMetrics {
		return nil
	}

	ctx, cancel := context.WithCancel(parent)

	interval := cfg.CollectInterval
	if interval <= 0 {
		interval = defaultMetricsCollectInterval
	}

	c := &metricsCollector{
		js:      js,
		conn:    conn,
		metrics: metrics,
		cfg:     cfg,
		ctx:     ctx,
		cancel:  cancel,
	}
	c.wg.Add(1)

	go c.run(interval)

	return c
}

func (c *metricsCollector) run(interval time.Duration) {
	defer c.wg.Done()

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			c.collect()
		}
	}
}

func (c *metricsCollector) collect() {
	ctx := c.ctx
	if c.conn != nil {
		c.collectConnectionStats(ctx)
	}

	if c.js != nil && (c.metrics == nil || !c.metrics.lite) {
		c.collectJetStream(ctx)
	}
}

func (c *metricsCollector) collectConnectionStats(ctx context.Context) {
	stats := c.conn.Stats()
	c.recordByteDelta(ctx, c.metrics.connectionInBytes, stats.InBytes, &c.lastInBytes)
	c.recordByteDelta(ctx, c.metrics.connectionOutBytes, stats.OutBytes, &c.lastOutBytes)

	if !c.conn.IsConnected() || c.metrics.connectionRTT == nil {
		return
	}

	rtt, err := c.conn.RTT()
	if err != nil {
		return
	}

	c.metrics.connectionRTT.Record(ctx, rtt.Seconds())
}

func (c *metricsCollector) recordByteDelta(
	ctx context.Context,
	counter *tel.FastCounter,
	current uint64,
	last *uint64,
) {
	if counter == nil || current < *last {
		*last = current

		return
	}

	delta := int64(current - *last)
	*last = current

	if delta > 0 {
		counter.Add(ctx, delta)
	}
}

func (c *metricsCollector) collectJetStream(ctx context.Context) {
	streamCache := make(map[string]*natspkg.StreamInfo)

	info, err := c.js.AccountInfo()
	if err == nil {
		if c.metrics.jsMemoryBytes != nil {
			c.metrics.jsMemoryBytes.Record(ctx, int64(info.Memory))
		}

		if c.metrics.jsStorageBytes != nil {
			c.metrics.jsStorageBytes.Record(ctx, int64(info.Store))
		}
	}

	for _, stream := range c.cfg.TrackedStreams {
		si := c.collectStream(ctx, stream)
		if si != nil {
			streamCache[stream] = si
		}
	}

	for _, tc := range c.cfg.TrackedConsumers {
		c.collectConsumer(ctx, tc.Stream, tc.Durable, streamCache)
	}
}

func (c *metricsCollector) collectStream(ctx context.Context, name string) *natspkg.StreamInfo {
	info, err := c.js.StreamInfo(name)
	if err != nil {
		return nil
	}

	if c.metrics.streamMessages != nil {
		c.metrics.streamMessages.RecordWith(ctx, int64(info.State.Msgs), name)
	}

	if c.metrics.streamBytes != nil {
		c.metrics.streamBytes.RecordWith(ctx, int64(info.State.Bytes), name)
	}

	if c.metrics.streamFirstSeq != nil {
		c.metrics.streamFirstSeq.RecordWith(ctx, int64(info.State.FirstSeq), name)
	}

	if c.metrics.streamLastSeq != nil {
		c.metrics.streamLastSeq.RecordWith(ctx, int64(info.State.LastSeq), name)
	}

	if c.metrics.streamConsumerCount != nil {
		c.metrics.streamConsumerCount.RecordWith(ctx, int64(info.State.Consumers), name)
	}

	if c.metrics.streamReplicaCount != nil && info.Cluster != nil {
		c.metrics.streamReplicaCount.RecordWith(ctx, int64(len(info.Cluster.Replicas)+1), name)
	}

	return info
}

func (c *metricsCollector) collectConsumer(ctx context.Context, stream, durable string, streamCache map[string]*natspkg.StreamInfo) {
	info, err := c.js.ConsumerInfo(stream, durable)
	if err != nil {
		return
	}

	label := stream + "." + durable

	if c.metrics.consumerNumPending != nil {
		c.metrics.consumerNumPending.RecordWith(ctx, int64(info.NumPending), label)
	}

	if c.metrics.consumerNumAckPending != nil {
		c.metrics.consumerNumAckPending.RecordWith(ctx, int64(info.NumAckPending), label)
	}

	if c.metrics.consumerNumRedelivered != nil {
		c.metrics.consumerNumRedelivered.RecordWith(ctx, int64(info.NumRedelivered), label)
	}

	if c.metrics.consumerAckFloor != nil {
		c.metrics.consumerAckFloor.RecordWith(ctx, int64(info.AckFloor.Stream), label)
	}

	if c.metrics.consumerLag != nil {
		streamInfo := streamCache[stream]
		if streamInfo == nil {
			var err error
			streamInfo, err = c.js.StreamInfo(stream)
			if err != nil {
				return
			}
		}
		lag := max(int64(streamInfo.State.LastSeq)-int64(info.Delivered.Stream), 0)
		c.metrics.consumerLag.RecordWith(ctx, lag, label)
	}
}

func (c *metricsCollector) stop() {
	if c == nil {
		return
	}

	c.cancel()
	c.wg.Wait()
}

func (c *clientMetrics) TrackStream(name string) {
	// tracked via MetricsConfig.TrackedStreams at init; runtime add omitted for simplicity
}
