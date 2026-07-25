package main

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/rs/zerolog"

	libnats "github.com/gopherust-io/nats"
)

func runPublisher(ctx context.Context, client libnats.Client) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	codec := strings.ToLower(envOr("CODEC", "json"))
	i := 0
	zerolog.Ctx(ctx).Info().
		Str("subject", publishSubject).
		Str("codec", codec).
		Msg("publisher started")

	for {
		select {
		case <-ctx.Done():
			zerolog.Ctx(ctx).Info().Msg("publisher stopped")
			return
		case <-ticker.C:
			i++
			msgID := fmt.Sprintf("order-%d", i)
			payload := map[string]any{
				"id": i,
				"ts": time.Now().UTC().Format(time.RFC3339Nano),
			}

			var err error
			switch codec {
			case "raw", "bytes":
				raw, mErr := json.Marshal(payload)
				if mErr != nil {
					zerolog.Ctx(ctx).Error().Err(mErr).Msg("marshal")
					continue
				}
				err = client.Publisher().PublishBytesWithMsgID(ctx, publishSubject, msgID, raw)
			default:
				err = client.Publisher().PublishWithMsgID(ctx, publishSubject, msgID, libnats.Message{
					Data:        payload,
					MessageType: libnats.JSON,
				})
			}
			if err != nil {
				zerolog.Ctx(ctx).Error().
					Str("msg_id", msgID).
					Err(err).
					Msg("publish failed")
				continue
			}
			zerolog.Ctx(ctx).Info().
				Str("subject", publishSubject).
				Str("msg_id", msgID).
				Int("order_id", i).
				Str("codec", codec).
				Msg("published")
		}
	}
}
