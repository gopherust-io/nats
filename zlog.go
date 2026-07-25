package nats

import (
	"os"

	"github.com/rs/zerolog"
)

func ensureDefaultLogger() {
	if zerolog.DefaultContextLogger != nil {
		return
	}
	l := zerolog.New(os.Stdout).With().Timestamp().Logger()
	zerolog.DefaultContextLogger = &l
}

// Called from package vars that need a seeded default logger.
var _ = func() int {
	ensureDefaultLogger()

	return 0
}()
