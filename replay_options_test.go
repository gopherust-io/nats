package nats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestReplayOptions(t *testing.T) {
	t.Parallel()
	start := time.Unix(1_700_000_000, 0).UTC()

	cfg := applyReplayOpts([]ReplayOpt{
		WithReplayDurable("replay-worker"),
		WithFilterSubject("orders.>"),
		WithDeliverPolicy(DeliverAll),
		WithReplayPolicy(ReplayInstant),
		WithStartSeq(9),
		WithStartTime(start),
		nil,
	})
	assert.Equal(t, "replay-worker", cfg.Durable)
	assert.Equal(t, "orders.>", cfg.FilterSubject)
	assert.Nil(t, cfg.FilterSubjects)
	assert.Equal(t, DeliverAll, cfg.DeliverPolicy)
	assert.Equal(t, ReplayInstant, cfg.ReplayPolicy)
	assert.Equal(t, uint64(9), cfg.OptStartSeq)
	requireTime(t, start, cfg.OptStartTime)

	cfg = applyReplayOpts([]ReplayOpt{WithFilterSubjects("a.>", "b.>")})
	assert.Empty(t, cfg.FilterSubject)
	assert.Equal(t, []string{"a.>", "b.>"}, cfg.FilterSubjects)

	cfg = applyReplayOpts([]ReplayOpt{FromSeq(42)})
	assert.Equal(t, DeliverByStartSequence, cfg.DeliverPolicy)
	assert.Equal(t, uint64(42), cfg.OptStartSeq)
	assert.Nil(t, cfg.OptStartTime)

	cfg = applyReplayOpts([]ReplayOpt{FromTime(start)})
	assert.Equal(t, DeliverByStartTime, cfg.DeliverPolicy)
	requireTime(t, start, cfg.OptStartTime)
	assert.Equal(t, uint64(0), cfg.OptStartSeq)

	cfg = applyReplayOpts([]ReplayOpt{FromBeginning()})
	assert.Equal(t, DeliverAll, cfg.DeliverPolicy)

	cfg = applyReplayOpts([]ReplayOpt{FromNew()})
	assert.Equal(t, DeliverNew, cfg.DeliverPolicy)
}

func requireTime(t *testing.T, want time.Time, got *time.Time) {
	t.Helper()
	if assert.NotNil(t, got) {
		assert.True(t, want.Equal(*got))
	}
}
