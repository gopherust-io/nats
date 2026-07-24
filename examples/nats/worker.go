package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"time"

	natspkg "github.com/nats-io/nats.go"

	libnats "github.com/gopherust-io/nats"
	"github.com/gopherust-io/tel"
)

type orderEvent struct {
	TS string `json:"ts"`
	ID int    `json:"id"`
}

func runWorker(ctx context.Context, client libnats.Client, telem *tel.Telemetry) error {
	processed, err := telem.Registry().Counter("orders.processed")
	if err != nil {
		return fmt.Errorf("metric orders.processed: %w", err)
	}
	failed, err := telem.Registry().Counter("orders.failed")
	if err != nil {
		return fmt.Errorf("metric orders.failed: %w", err)
	}

	rec := libnats.NewFlightRecorder(128)

	primary := func(msgCtx context.Context, msg *natspkg.Msg) error {
		var ev orderEvent
		if err := json.Unmarshal(msg.Data, &ev); err != nil {
			failed.Add(msgCtx, 1)
			slog.Error("decode order", "err", err)

			return fmt.Errorf("decode: %w", libnats.ErrSendToDLQ)
		}

		if ev.ID > 0 && ev.ID%17 == 0 {
			failed.Add(msgCtx, 1)
			slog.Warn("poison order routed to dlq", "order_id", ev.ID, "subject", msg.Subject)

			return fmt.Errorf("simulated poison order_id=%d: %w", ev.ID, libnats.ErrSendToDLQ)
		}

		slog.Info("processed order",
			"order_id", ev.ID,
			"subject", msg.Subject,
			"msg_id", msg.Header.Get(libnats.HeaderMsgID))
		processed.Add(msgCtx, 1)

		return nil
	}

	shadow := func(msgCtx context.Context, msg *natspkg.Msg) error {
		var ev orderEvent
		if err := json.Unmarshal(msg.Data, &ev); err != nil {
			return err
		}
		if ev.ID > 0 && ev.ID%17 == 0 {
			return fmt.Errorf("shadow also rejects poison")
		}

		return nil
	}

	handler := libnats.WithDLQ(libnats.DLQConfig{
		Publisher:  client.Publisher(),
		Subject:    dlqSubject,
		MaxDeliver: 5,
		Recorder:   rec,
		Autopsy:    libnats.AutopsyConfig{Enabled: true},
	}, client.WithShadow(libnats.ShadowConfig{
		SampleRate: 0.25,
		Recorder:   rec,
	}, primary, shadow))

	supCfg := libnats.SupervisorConfig{
		MaxRetries:     10,
		InitialBackoff: time.Second,
		CheckInterval:  time.Second,
	}
	rec.AttachSupervisor(&supCfg)

	sub, err := client.SuperviseQueueSubscribeBound(ctx,
		streamName, durableName, queueName, subjectFilter,
		handler, supCfg)
	if err != nil {
		return fmt.Errorf("supervise queue subscribe: %w", err)
	}

	liveCfg := libnats.SoftLivenessConfig{
		PollInterval:  2 * time.Second,
		StallAfter:    15 * time.Second,
		RisingWindows: 3,
	}
	rec.AttachSoftLiveness(&liveCfg)

	live, err := client.WatchSoftLiveness(ctx, sub, liveCfg)
	if err != nil {
		_ = sub.Stop()

		return fmt.Errorf("soft liveness: %w", err)
	}

	go func() {
		<-ctx.Done()
		live.Stop()
		_ = sub.Stop()
		dumpFlightRecorder(rec)
	}()

	slog.Info("worker started",
		"stream", streamName,
		"durable", durableName,
		"queue", queueName)

	return nil
}

func dumpFlightRecorder(rec *libnats.FlightRecorder) {
	if rec == nil || len(rec.Snapshot()) == 0 {
		return
	}
	slog.Warn("flight recorder dump on shutdown", "incidents", len(rec.Snapshot()))
	_ = rec.WriteJSON(os.Stderr)
}
