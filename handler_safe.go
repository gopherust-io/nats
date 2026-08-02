package nats

import (
	"context"
	"fmt"

	natspkg "github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
)

func invokeMsgHandler(ctx context.Context, msg *natspkg.Msg, handler MsgHandler) (err error) {
	defer func() {
		if rec := recover(); rec != nil {
			if e, ok := rec.(error); ok {
				err = fmt.Errorf("handler panic: %w", e)
			} else {
				err = fmt.Errorf("handler panic: %v", rec)
			}
			zerolog.Ctx(ctx).Error().Any("panic", rec).Msg("message handler panic")
		}
	}()

	return handler(ctx, msg)
}
