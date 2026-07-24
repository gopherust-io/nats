package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	natspkg "github.com/nats-io/nats.go"

	libnats "github.com/gopherust-io/nats"
	"github.com/gopherust-io/tel"
)

func runPuller(ctx context.Context, client libnats.Client, telem *tel.Telemetry) error {
	processed, err := telem.Registry().Counter("orders.puller.processed")
	if err != nil {
		return fmt.Errorf("metric orders.puller.processed: %w", err)
	}

	handler := func(msgCtx context.Context, msg *natspkg.Msg) error {
		var ev orderEvent
		if err := json.Unmarshal(msg.Data, &ev); err != nil {
			return fmt.Errorf("decode: %w", err)
		}

		slog.Info("puller processed order",
			"order_id", ev.ID,
			"subject", msg.Subject,
			"msg_id", msg.Header.Get(libnats.HeaderMsgID))
		processed.Add(msgCtx, 1)

		return nil
	}

	pull, err := client.Consumer().Pull(streamName, pullDurableName)
	if err != nil {
		return fmt.Errorf("pull consumer: %w", err)
	}

	slog.Info("puller started",
		"stream", streamName,
		"durable", pullDurableName,
		"fetch_batch", pullFetchBatch)

	return pull.Process(ctx, handler,
		libnats.WithFetchBatch(pullFetchBatch),
		libnats.WithProcessMaxWait(3*time.Second),
		libnats.WithProcessHeartbeat(500*time.Millisecond),
	)
}
