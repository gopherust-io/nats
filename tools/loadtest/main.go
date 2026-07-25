// Command loadtest publishes and consumes JetStream messages for CPU/memory profiling.
//
// Example:
//
//	docker compose -f docker/nats/single/docker-compose.yml up -d
//	go run ./tools/loadtest -nats nats://127.0.0.1:4222 -duration 30s -codec bytes -workers 8
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	libnats "github.com/gopherust-io/nats"
	"github.com/gopherust-io/tel"
	natspkg "github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
)

func main() {
	var (
		natsURL  = flag.String("nats", "nats://127.0.0.1:4222", "NATS URL")
		duration = flag.Duration("duration", 15*time.Second, "run duration")
		codec    = flag.String("codec", "json", "json|bytes")
		workers  = flag.Int("workers", 4, "worker pool size")
		batch    = flag.Int("batch", 50, "pull fetch batch (pull mode)")
		mode     = flag.String("mode", "push", "push|pull")
		metrics  = flag.Bool("metrics", false, "enable NATS metrics")
		rate     = flag.Int("rate", 500, "publish target msgs/sec")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runCtx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	telem := tel.NewWithConfig(tel.DefaultDebugConfig())
	_ = telem.Start(runCtx)
	defer func() { _ = telem.Shutdown(context.Background()) }()
	// Prefer stderr for the loadtest CLI after tel seeds the process logger.
	tel.SetLogger(zerolog.New(os.Stderr).With().Timestamp().Logger())
	runCtx = tel.WrapContext(runCtx, telem)
	log := zerolog.Ctx(runCtx)

	var cfg libnats.Config
	if *metrics {
		cfg = libnats.ProdWorkerConfig()
	} else {
		cfg = libnats.ThroughputConfig()
	}
	cfg.Conn.Address = *natsURL
	cfg.Stream = libnats.StreamConfig{
		Name: "LOAD", Subjects: []string{"load.>"}, Replicas: 1,
		Storage: libnats.MemoryStorage, Retention: libnats.WorkQueuePolicy,
	}
	cfg.RuntimeConsumer.WorkerPoolSize = *workers
	cfg.RuntimeConsumer.WorkerBufferSize = (*workers) * 8
	cfg.RuntimeConsumer.WorkerPoolEnabled = *mode == "push"

	client, err := libnats.NewClient(runCtx, &cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("nats client")
	}
	defer func() { _ = client.Connector().Shutdown() }()

	if _, err := client.Streams().CreateOrUpdateStream(runCtx, cfg.Stream); err != nil {
		log.Fatal().Err(err).Msg("create stream")
	}

	var consumed atomic.Int64
	var published atomic.Int64
	handler := func(_ context.Context, msg *natspkg.Msg) error {
		consumed.Add(1)
		_ = msg

		return nil
	}

	switch *mode {
	case "pull":
		if _, err := client.Consumers().CreateOrUpdateConsumer(runCtx, "LOAD", libnats.DurableConsumerConfig{
			Durable: "load-pull", FilterSubject: "load.>", MaxAckPending: 2000, MaxWaiting: 512,
		}); err != nil {
			log.Fatal().Err(err).Msg("create pull consumer")
		}
		go func() {
			pull, err := client.Consumer().Pull("LOAD", "load-pull")
			if err != nil {
				zerolog.Ctx(runCtx).Error().Err(err).Msg("pull consumer")
				return
			}
			_ = pull.Process(runCtx, handler,
				libnats.WithFetchBatch(*batch),
				libnats.WithProcessConcurrency(*workers),
				libnats.WithProcessMaxWait(500*time.Millisecond),
			)
		}()
	default:
		if _, err := client.Consumer().QueueSubscribeBound(runCtx, "LOAD", "load-proc", "load-q", "load.>", handler); err != nil {
			log.Fatal().Err(err).Msg("queue subscribe")
		}
	}

	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)
	gorStart := runtime.NumGoroutine()
	start := time.Now()

	ticker := time.NewTicker(time.Second / time.Duration(max(*rate, 1)))
	defer ticker.Stop()

	payload := map[string]any{"id": 1, "ts": time.Now().UTC().Format(time.RFC3339Nano)}
	raw, _ := json.Marshal(payload)

	go func() {
		i := 0
		for {
			select {
			case <-runCtx.Done():
				return
			case <-ticker.C:
				i++
				subj := "load.evt"
				var err error
				switch *codec {
				case "bytes", "raw":
					err = client.Publisher().PublishBytes(runCtx, subj, raw)
				default:
					err = client.Publisher().PublishJSON(runCtx, subj, payload)
				}
				if err == nil {
					published.Add(1)
				}
			}
		}
	}()

	<-runCtx.Done()
	elapsed := time.Since(start).Seconds()
	runtime.GC()
	var memEnd runtime.MemStats
	runtime.ReadMemStats(&memEnd)

	pub := published.Load()
	con := consumed.Load()
	fmt.Printf("mode=%s codec=%s metrics=%v duration=%.1fs\n", *mode, *codec, *metrics, elapsed)
	fmt.Printf("published=%d (%.0f/s) consumed=%d (%.0f/s)\n", pub, float64(pub)/elapsed, con, float64(con)/elapsed)
	fmt.Printf("alloc_delta_MB=%.2f heap_inuse_MB=%.2f goroutines_start=%d end=%d\n",
		float64(memEnd.TotalAlloc-memStart.TotalAlloc)/(1<<20),
		float64(memEnd.HeapInuse)/(1<<20),
		gorStart, runtime.NumGoroutine())
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
