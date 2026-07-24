package main

import (
	"context"
	"log/slog"
	"os"

	"github.com/gopherust-io/tel"
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

	telem := tel.NewWithConfig(cfg)
	tel.SetGlobal(telem)

	if err := telem.Start(ctx); err != nil {
		slog.Error("telemetry start", "err", err)
		os.Exit(1)
	}

	slog.Info("telemetry ready",
		"service", cfg.Service,
		"otlp_enabled", cfg.TelConfig.Enable,
		"otlp_addr", cfg.TelConfig.Address)

	return telem
}
