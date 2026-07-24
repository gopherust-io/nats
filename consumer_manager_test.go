package nats

import (
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestToNatsConsumerConfigDefaults(t *testing.T) {
	cc := toNatsConsumerConfig(DurableConsumerConfig{
		Durable:       "worker",
		FilterSubject: "jobs.>",
	})

	assert.Equal(t, "worker", cc.Durable)
	assert.Equal(t, "jobs.>", cc.FilterSubject)
	assert.Equal(t, AckExplicit, cc.AckPolicy)
	assert.Equal(t, ReplayInstant, cc.ReplayPolicy)
}

func TestToNatsConsumerConfigCustom(t *testing.T) {
	start := time.Now().UTC()
	cc := toNatsConsumerConfig(DurableConsumerConfig{
		Durable:           "puller",
		FilterSubjects:    []string{"a.>", "b.>"},
		DeliverPolicy:     DeliverNew,
		ReplayPolicy:      ReplayOriginal,
		AckPolicy:         AckAll,
		MaxDeliver:        5,
		AckWait:           15 * time.Second,
		MaxAckPending:     200,
		RateLimit:         1000,
		Heartbeat:         time.Second,
		InactiveThreshold: 30 * time.Second,
		FlowControl:       true,
		Replicas:          2,
		MemStorage:        true,
		MaxWaiting:        50,
		OptStartSeq:       42,
		OptStartTime:      &start,
	})

	assert.Equal(t, []string{"a.>", "b.>"}, cc.FilterSubjects)
	assert.Equal(t, DeliverNew, cc.DeliverPolicy)
	assert.Equal(t, ReplayOriginal, cc.ReplayPolicy)
	assert.Equal(t, AckAll, cc.AckPolicy)
	assert.Equal(t, 5, cc.MaxDeliver)
	assert.Equal(t, 15*time.Second, cc.AckWait)
	assert.Equal(t, 200, cc.MaxAckPending)
	assert.Equal(t, uint64(1000), cc.RateLimit)
	assert.Equal(t, time.Second, cc.Heartbeat)
	assert.Equal(t, 30*time.Second, cc.InactiveThreshold)
	assert.True(t, cc.FlowControl)
	assert.Equal(t, 2, cc.Replicas)
	assert.True(t, cc.MemoryStorage)
	assert.Equal(t, 50, cc.MaxWaiting)
	assert.Equal(t, uint64(42), cc.OptStartSeq)
	assert.Equal(t, &start, cc.OptStartTime)
}

func TestToNatsConsumerConfigFilterSubjectsOnly(t *testing.T) {
	cc := toNatsConsumerConfig(DurableConsumerConfig{
		Durable:        "worker",
		FilterSubjects: []string{"orders.>", "orders.events"},
	})
	assert.Empty(t, cc.FilterSubject)
	assert.Equal(t, []string{"orders.>", "orders.events"}, cc.FilterSubjects)
}

func TestToNatsConsumerConfigFilterSubjectPrecedence(t *testing.T) {
	cc := toNatsConsumerConfig(DurableConsumerConfig{
		Durable:        "worker",
		FilterSubject:  "orders.>",
		FilterSubjects: []string{"orders.events"},
	})
	assert.Equal(t, "orders.>", cc.FilterSubject)
	assert.Equal(t, []string{"orders.events"}, cc.FilterSubjects)
}
