package main

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/rs/zerolog"

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

		zerolog.Ctx(msgCtx).Info().
			Int("order_id", ev.ID).
			Str("subject", msg.Subject).
			Str("msg_id", msg.Header.Get(libnats.HeaderMsgID)).
			Msg("puller processed order")
		processed.Add(msgCtx, 1)

		return nil
	}

	pull, err := client.Consumer().Pull(streamName, pullDurableName)
	if err != nil {
		return fmt.Errorf("pull consumer: %w", err)
	}

	zerolog.Ctx(ctx).Info().
		Str("stream", streamName).
		Str("durable", pullDurableName).
		Int("fetch_batch", pullFetchBatch).
		Msg("puller started")

	return pull.Process(ctx, handler,
		libnats.WithFetchBatch(pullFetchBatch),
		libnats.WithProcessMaxWait(3*time.Second),
		libnats.WithProcessHeartbeat(500*time.Millisecond),
	)
}
