package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
	"time"

	libnats "github.com/gopherust-io/nats"
)

func runPublisher(ctx context.Context, client libnats.Client) {
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	codec := strings.ToLower(envOr("CODEC", "json"))
	i := 0
	slog.Info("publisher started", "subject", publishSubject, "codec", codec)

	for {
		select {
		case <-ctx.Done():
			slog.Info("publisher stopped")
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
					slog.Error("marshal", "err", mErr)
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
				slog.Error("publish failed", "msg_id", msgID, "err", err)
				continue
			}
			slog.Info("published", "subject", publishSubject, "msg_id", msgID, "order_id", i, "codec", codec)
		}
	}
}
