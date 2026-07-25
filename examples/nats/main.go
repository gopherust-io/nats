package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	libnats "github.com/gopherust-io/nats"
	"github.com/gopherust-io/tel"
	"github.com/rs/zerolog"
)

const (
	streamName      = "ORDERS"
	dlqStream       = "ORDERS_DLQ"
	durableName     = "orders-processor"
	pullDurableName = "orders-puller"
	queueName       = "orders-workers"
	subjectFilter   = "orders.>"
	publishSubject  = "orders.created"
	dlqSubject      = "orders.dlq.poison"
)

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	role := strings.ToLower(envOr("ROLE", "all"))
	log := zerolog.Ctx(ctx)

	telem := mustTelemetry(ctx)
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := telem.Shutdown(shutdownCtx); err != nil {
			zerolog.Ctx(shutdownCtx).Error().Err(err).Msg("telemetry shutdown")
		}
	}()
	ctx = tel.WrapContext(ctx, telem)
	log = zerolog.Ctx(ctx)

	if cache := telem.Registry().AttrCache(); cache != nil {
		for _, s := range []string{publishSubject, subjectFilter, dlqSubject} {
			_ = cache.SubjectOpts(s)
		}
	}

	cfg := buildConfigForRole(role)
	client, err := libnats.NewClient(ctx, &cfg)
	if err != nil {
		log.Error().Err(err).Msg("nats client")
		os.Exit(1)
	}
	defer func() {
		if err := client.Connector().Shutdown(); err != nil {
			log.Error().Err(err).Msg("nats shutdown")
		}
	}()

	if err := ensureTopology(ctx, client); err != nil {
		log.Error().Err(err).Msg("ensure topology")
		os.Exit(1)
	}

	log.Info().
		Str("role", role).
		Str("nats_url", cfg.Conn.Address).
		Str("service", telem.Config().Service).
		Msg("nats orders example started")

	switch role {
	case "publisher":
		runPublisher(ctx, client)
	case "worker":
		if err := runWorker(ctx, client, telem); err != nil {
			log.Error().Err(err).Msg("worker")
			os.Exit(1)
		}
		<-ctx.Done()
	case "puller":
		if err := runPuller(ctx, client, telem); err != nil && ctx.Err() == nil {
			log.Error().Err(err).Msg("puller")
			os.Exit(1)
		}
	case "all":
		go runPublisher(ctx, client)
		if err := runWorker(ctx, client, telem); err != nil {
			log.Error().Err(err).Msg("worker")
			os.Exit(1)
		}
		<-ctx.Done()
	default:
		log.Error().
			Str("role", role).
			Str("want", "all|publisher|worker|puller").
			Msg("unknown ROLE")
		os.Exit(1)
	}

	log.Info().Msg("shutting down")
}

func buildConfigForRole(role string) libnats.Config {
	switch role {
	case "puller":
		return buildPullConfig()
	default:
		return buildWorkerConfig()
	}
}

func ensureTopology(ctx context.Context, client libnats.Client) error {
	if _, err := client.Streams().CreateOrUpdateStream(ctx, libnats.StreamConfig{
		Name:            streamName,
		Subjects:        []string{subjectFilter},
		Replicas:        1,
		Storage:         libnats.MemoryStorage,
		Retention:       libnats.WorkQueuePolicy,
		MaxAge:          24 * time.Hour,
		Discard:         libnats.DiscardOld,
		DuplicateWindow: 2 * time.Minute,
	}); err != nil {
		return fmt.Errorf("stream %s: %w", streamName, err)
	}

	if _, err := client.Streams().CreateOrUpdateStream(ctx, libnats.StreamConfig{
		Name:      dlqStream,
		Subjects:  []string{"orders.dlq.>"},
		Replicas:  1,
		Storage:   libnats.MemoryStorage,
		Retention: libnats.LimitsPolicy,
		MaxAge:    7 * 24 * time.Hour,
		Discard:   libnats.DiscardOld,
	}); err != nil {
		return fmt.Errorf("stream %s: %w", dlqStream, err)
	}

	if err := ensurePullConsumer(ctx, client); err != nil {
		return err
	}

	return nil
}

func ensurePullConsumer(ctx context.Context, client libnats.Client) error {
	ackWait := ackWaitForHandler(handlerP99 * 2)

	if _, err := client.Consumers().CreateOrUpdateConsumer(ctx, streamName, libnats.DurableConsumerConfig{
		Durable:       pullDurableName,
		FilterSubject: subjectFilter,
		AckPolicy:     libnats.AckExplicit,
		MaxAckPending: prodWorkerMaxAckPending(),
		AckWait:       ackWait,
		MaxDeliver:    5,
		MaxWaiting:    512,
		DeliverPolicy: libnats.DeliverNew,
	}); err != nil {
		return fmt.Errorf("pull durable %s: %w", pullDurableName, err)
	}

	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}

	return fallback
}
