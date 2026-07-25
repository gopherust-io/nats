package main

import (
	"context"
	"os"

	"github.com/gopherust-io/tel"
	"github.com/rs/zerolog"
)

func mustTelemetry(ctx context.Context) *tel.Telemetry {
	cfg := tel.DefaultConfig()
	cfg.Service = "nats-orders-example"
	cfg.Version = envOr("SERVICE_VERSION", "1.0.0")
	cfg.Environment = envOr("ENVIRONMENT", "dev")

	switch os.Getenv("TEL_ENABLE") {
	case "false":
		cfg.TelConfig.Enable = false
	case "true":
		cfg.TelConfig.Enable = true
	}

	cfg.MonitorConfig.Enable = false
	cfg.LogEncode = "console"
	cfg.LogLevel = "info"

	tel.ConfigureLogger(cfg)
	telem := tel.NewWithConfig(cfg)
	tel.SetGlobal(telem)

	if err := telem.Start(ctx); err != nil {
		zerolog.Ctx(ctx).Error().Err(err).Msg("telemetry start")
		os.Exit(1)
	}

	zerolog.Ctx(ctx).Info().
		Str("service", cfg.Service).
		Bool("otlp_enabled", cfg.TelConfig.Enable).
		Str("otlp_addr", cfg.TelConfig.Address).
		Msg("telemetry ready")

	return telem
}
