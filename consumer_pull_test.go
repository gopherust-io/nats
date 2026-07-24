package nats

import (
	"context"
	"testing"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

func TestProcessDefaultMaxWait(t *testing.T) {
	cfg := &processConfig{}
	if cfg.maxWait <= 0 {
		cfg.maxWait = defaultFetchMaxWait
	}
	assert.Equal(t, defaultFetchMaxWait, cfg.maxWait)
}

func TestProcessBatchEmpty(t *testing.T) {
	p := &pullConsumer{}
	err := p.processBatch(context.Background(), nil, func(context.Context, *natspkg.Msg) error {
		t.Fatal("handler should not run")
		return nil
	}, &processConfig{})
	assert.NoError(t, err)
}
