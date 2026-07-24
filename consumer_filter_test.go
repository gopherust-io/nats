package nats

import (
	"testing"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
)

func TestConsumerFilterSubjectSingle(t *testing.T) {
	cfg := natspkg.ConsumerConfig{FilterSubject: "orders.>"}
	assert.Equal(t, "orders.>", consumerFilterSubject(cfg))
}

func TestConsumerFilterSubjectsOne(t *testing.T) {
	cfg := natspkg.ConsumerConfig{FilterSubjects: []string{"orders.created"}}
	assert.Equal(t, "orders.created", consumerFilterSubject(cfg))
}

func TestConsumerFilterSubjectsWildcard(t *testing.T) {
	cfg := natspkg.ConsumerConfig{
		FilterSubjects: []string{"orders.created", "orders.updated", "orders.cancelled"},
	}
	assert.Equal(t, "orders.>", consumerFilterSubject(cfg))
}

func TestConsumerFilterSubjectsNoCommon(t *testing.T) {
	cfg := natspkg.ConsumerConfig{
		FilterSubjects: []string{"orders.created", "payments.settled"},
	}
	assert.Equal(t, "orders.created", consumerFilterSubject(cfg))
}

func TestCommonWildcardSubject(t *testing.T) {
	assert.Equal(t, "orders.>", commonWildcardSubject([]string{"orders.a", "orders.b"}))
	assert.Empty(t, commonWildcardSubject([]string{"orders", "payments"}))
	assert.Empty(t, commonWildcardSubject(nil))
	assert.Equal(t, ">", consumerFilterSubjects(nil))
	assert.Equal(t, "shared.>", commonWildcardSubject([]string{"shared", "sharedx"}))
}
