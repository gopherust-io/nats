package main

import (
	"os"
	"time"

	libnats "github.com/gopherust-io/nats"
	"github.com/gopherust-io/nats/internal/bytesconv"
)

const (
	handlerP99     = 15 * time.Second
	pullFetchBatch = 100
)

func ackWaitForHandler(p99 time.Duration) time.Duration {
	return p99 * 3
}

func buildWorkerConfig() libnats.Config {
	cfg := libnats.DefaultConfig()

	cfg.Conn.Address = envOr("NATS_URL", "nats://127.0.0.1:4222")
	cfg.Conn.ClientName = clientName("worker")

	cfg.RuntimeConsumer.WorkerPoolEnabled = true
	cfg.RuntimeConsumer.WorkerPoolSize = 8
	cfg.RuntimeConsumer.WorkerBufferSize = 256
	cfg.RuntimeConsumer.PendingMsgLimit = 1000
	cfg.RuntimeConsumer.PendingMsgBuffer = 10 << 20
	ackWait := ackWaitForHandler(handlerP99)
	cfg.RuntimeConsumer.AckWait = ackWait
	cfg.RuntimeConsumer.IdleHeartbeat = 0
	cfg.RuntimeConsumer.FlowControl = false

	cfg.Backpressure.Mode = libnats.BackpressureNak
	cfg.Backpressure.MaxAckPending = prodWorkerMaxAckPending()

	cfg.Metrics.TrackedStreams = []string{streamName, dlqStream}
	cfg.Metrics.TrackedConsumers = []libnats.TrackedConsumer{
		{Stream: streamName, Durable: durableName},
	}

	return cfg
}

func buildPullConfig() libnats.Config {
	cfg := libnats.DefaultConfig()

	cfg.Conn.Address = envOr("NATS_URL", "nats://127.0.0.1:4222")
	cfg.Conn.ClientName = clientName("puller")

	cfg.RuntimeConsumer.WorkerPoolEnabled = false
	batchP99 := handlerP99 * 2
	cfg.RuntimeConsumer.AckWait = ackWaitForHandler(batchP99)

	cfg.Metrics.TrackedStreams = []string{streamName}
	cfg.Metrics.TrackedConsumers = []libnats.TrackedConsumer{
		{Stream: streamName, Durable: pullDurableName},
	}

	return cfg
}

func prodWorkerMaxAckPending() int {
	return 1000
}

func clientName(role string) string {
	host, _ := os.Hostname()
	if bytesconv.IsEmpty(host) {
		host = "local"
	}

	return "nats-orders-" + role + "-" + host
}
