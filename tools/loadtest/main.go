// Command loadtest publishes and consumes JetStream messages for CPU/memory profiling.
//
// Example:
//
//	# nats-console: make nats-up  (or nats-server -js)
//	go run ./tools/loadtest -nats nats://127.0.0.1:4222 -duration 30s -codec bytes -workers 8
//	go run ./tools/loadtest -impl natsgo -codec bytes -mode push -duration 30s
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

	"github.com/gopherust-io/tel"
	natspkg "github.com/nats-io/nats.go"
	"github.com/rs/zerolog"

	libnats "github.com/gopherust-io/nats"
)

func main() {
	var (
		natsURL  = flag.String("nats", "nats://127.0.0.1:4222", "NATS URL")
		duration = flag.Duration("duration", 15*time.Second, "run duration")
		codec    = flag.String("codec", "json", "json|bytes")
		workers  = flag.Int("workers", 4, "worker pool size")
		batch    = flag.Int("batch", 50, "pull fetch batch (pull mode)")
		mode     = flag.String("mode", "push", "push|pull")
		metrics  = flag.Bool("metrics", false, "enable NATS metrics (gopherust only)")
		rate     = flag.Int("rate", 500, "publish target msgs/sec")
		impl     = flag.String("impl", "gopherust", "gopherust|natsgo")
	)
	flag.Parse()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	runCtx, cancel := context.WithTimeout(ctx, *duration)
	defer cancel()

	telem := tel.NewWithConfig(tel.DefaultDebugConfig())
	_ = telem.Start(runCtx)
	defer func() { _ = telem.Shutdown(context.Background()) }()
	tel.SetLogger(zerolog.New(os.Stderr).With().Timestamp().Logger())
	runCtx = tel.WrapContext(runCtx, telem)
	log := zerolog.Ctx(runCtx)

	var consumed atomic.Int64
	var published atomic.Int64
	payload := map[string]any{"id": 1, "ts": time.Now().UTC().Format(time.RFC3339Nano)}
	raw, _ := json.Marshal(payload)

	var memStart runtime.MemStats
	runtime.ReadMemStats(&memStart)
	gorStart := runtime.NumGoroutine()
	start := time.Now()

	switch *impl {
	case "natsgo":
		runNatsGo(runCtx, log, *natsURL, *mode, *codec, *batch, *workers, *rate, raw, payload, &published, &consumed)
	case "gopherust":
		runGopherust(runCtx, log, *natsURL, *mode, *codec, *batch, *workers, *metrics, *rate, raw, payload, &published, &consumed)
	default:
		log.Fatal().Str("impl", *impl).Msg("unknown -impl (want gopherust|natsgo)")
	}

	elapsed := time.Since(start).Seconds()
	if elapsed <= 0 {
		elapsed = 0.001
	}
	runtime.GC()
	var memEnd runtime.MemStats
	runtime.ReadMemStats(&memEnd)

	pub := published.Load()
	con := consumed.Load()
	fmt.Printf("impl=%s mode=%s codec=%s metrics=%v duration=%.1fs\n", *impl, *mode, *codec, *metrics, elapsed)
	fmt.Printf("published=%d (%.0f/s) consumed=%d (%.0f/s)\n", pub, float64(pub)/elapsed, con, float64(con)/elapsed)
	fmt.Printf("alloc_delta_MB=%.2f heap_inuse_MB=%.2f goroutines_start=%d end=%d\n",
		float64(memEnd.TotalAlloc-memStart.TotalAlloc)/(1<<20),
		float64(memEnd.HeapInuse)/(1<<20),
		gorStart, runtime.NumGoroutine())
}

func runGopherust(
	runCtx context.Context,
	log *zerolog.Logger,
	natsURL, mode, codec string,
	batch, workers int,
	metrics bool,
	rate int,
	raw []byte,
	payload map[string]any,
	published, consumed *atomic.Int64,
) {
	var cfg libnats.Config
	if metrics {
		cfg = libnats.ProdWorkerConfig()
	} else {
		cfg = libnats.ThroughputConfig()
	}
	cfg.Conn.Address = natsURL
	cfg.Stream = libnats.StreamConfig{
		Name: "LOAD", Subjects: []string{"load.>"}, Replicas: 1,
		Storage: libnats.MemoryStorage, Retention: libnats.WorkQueuePolicy,
	}
	cfg.RuntimeConsumer.WorkerPoolSize = workers
	cfg.RuntimeConsumer.WorkerBufferSize = workers * 8
	cfg.RuntimeConsumer.WorkerPoolEnabled = mode == "push"

	client, err := libnats.NewClient(runCtx, &cfg)
	if err != nil {
		log.Fatal().Err(err).Msg("nats client")
	}
	defer func() { _ = client.Connector().Shutdown() }()

	if _, err := client.Streams().CreateOrUpdateStream(runCtx, cfg.Stream); err != nil {
		log.Fatal().Err(err).Msg("create stream")
	}

	handler := func(_ context.Context, msg *natspkg.Msg) error {
		consumed.Add(1)
		_ = msg
		return nil
	}

	switch mode {
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
				libnats.WithFetchBatch(batch),
				libnats.WithProcessConcurrency(workers),
				libnats.WithProcessMaxWait(500*time.Millisecond),
			)
		}()
	default:
		if _, err := client.Consumer().QueueSubscribeBound(runCtx, "LOAD", "load-proc", "load-q", "load.>", handler); err != nil {
			log.Fatal().Err(err).Msg("queue subscribe")
		}
	}

	ticker := time.NewTicker(time.Second / time.Duration(max(rate, 1)))
	defer ticker.Stop()
	for {
		select {
		case <-runCtx.Done():
			return
		case <-ticker.C:
			subj := "load.evt"
			var err error
			switch codec {
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
}

func runNatsGo(
	runCtx context.Context,
	log *zerolog.Logger,
	natsURL, mode, codec string,
	batch, workers, rate int,
	raw []byte,
	payload map[string]any,
	published, consumed *atomic.Int64,
) {
	nc, err := natspkg.Connect(natsURL)
	if err != nil {
		log.Fatal().Err(err).Msg("nats.go connect")
	}
	defer nc.Close()
	js, err := nc.JetStream()
	if err != nil {
		log.Fatal().Err(err).Msg("jetstream")
	}
	_, err = js.AddStream(&natspkg.StreamConfig{
		Name: "LOAD", Subjects: []string{"load.>"}, Storage: natspkg.MemoryStorage,
		Retention: natspkg.WorkQueuePolicy, Replicas: 1,
	})
	if err != nil {
		// Stream may already exist from a prior gopherust run.
		if _, err2 := js.StreamInfo("LOAD"); err2 != nil {
			log.Fatal().Err(err).Msg("add stream")
		}
	}

	switch mode {
	case "pull":
		sub, err := js.PullSubscribe("load.>", "load-pull-ng", natspkg.BindStream("LOAD"))
		if err != nil {
			log.Fatal().Err(err).Msg("pull subscribe")
		}
		go func() {
			for {
				select {
				case <-runCtx.Done():
					return
				default:
				}
				msgs, err := sub.Fetch(batch, natspkg.MaxWait(500*time.Millisecond))
				if err != nil {
					continue
				}
				for _, msg := range msgs {
					_ = msg.Ack()
					consumed.Add(1)
				}
			}
		}()
	default:
		_, err := js.QueueSubscribe("load.>", "load-q", func(msg *natspkg.Msg) {
			_ = msg.Ack()
			consumed.Add(1)
		}, natspkg.Durable("load-proc-ng"), natspkg.BindStream("LOAD"), natspkg.ManualAck())
		if err != nil {
			log.Fatal().Err(err).Msg("queue subscribe")
		}
		_ = workers // nats.go path has no worker-pool knob
	}

	ticker := time.NewTicker(time.Second / time.Duration(max(rate, 1)))
	defer ticker.Stop()
	for {
		select {
		case <-runCtx.Done():
			return
		case <-ticker.C:
			subj := "load.evt"
			var err error
			switch codec {
			case "bytes", "raw":
				_, err = js.Publish(subj, raw)
			default:
				_, err = js.Publish(subj, mustJSON(payload))
			}
			if err == nil {
				published.Add(1)
			}
		}
	}
}

func mustJSON(v any) []byte {
	b, _ := json.Marshal(v)
	return b
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}
