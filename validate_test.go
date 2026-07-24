package nats

import (
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestValidateStreamName(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateStreamName("ORDERS"))
	require.NoError(t, ValidateStreamName("orders_v1"))
	require.NoError(t, ValidateStreamName("A"))

	require.ErrorIs(t, ValidateStreamName(""), ErrInvalidStreamName)
	require.ErrorIs(t, ValidateStreamName("orders.events"), ErrInvalidStreamName)
	require.ErrorIs(t, ValidateStreamName("orders*"), ErrInvalidStreamName)
	require.ErrorIs(t, ValidateStreamName("orders>"), ErrInvalidStreamName)
	require.ErrorIs(t, ValidateStreamName("orders/events"), ErrInvalidStreamName)
	require.ErrorIs(t, ValidateStreamName(`orders\events`), ErrInvalidStreamName)
	require.ErrorIs(t, ValidateStreamName("orders events"), ErrInvalidStreamName)
	require.ErrorIs(t, ValidateStreamName("orders\t"), ErrInvalidStreamName)
	require.ErrorIs(t, ValidateStreamName(strings.Repeat("a", maxNameLen+1)), ErrInvalidStreamName)
}

func TestValidateDurableName(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateDurableName("orders-processor"))
	require.ErrorIs(t, ValidateDurableName(""), ErrInvalidDurableName)
	require.ErrorIs(t, ValidateDurableName("orders.processor"), ErrInvalidDurableName)
	require.ErrorIs(t, ValidateDurableName("proc*"), ErrInvalidDurableName)
}

func TestValidateQueueName(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateQueueName("orders-workers"))
	require.ErrorIs(t, ValidateQueueName(""), ErrInvalidQueueName)
	require.ErrorIs(t, ValidateQueueName("workers.group"), ErrInvalidQueueName)
}

func TestValidateBucketName(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateBucketName("ORDERS_DEDUP"))
	require.NoError(t, ValidateBucketName("orders-dedup"))
	require.ErrorIs(t, ValidateBucketName(""), ErrInvalidBucketName)
	require.ErrorIs(t, ValidateBucketName("bad.bucket"), ErrInvalidBucketName)
	require.ErrorIs(t, ValidateBucketName("bad/bucket"), ErrInvalidBucketName)
	require.ErrorIs(t, ValidateBucketName(strings.Repeat("a", maxNameLen+1)), ErrInvalidBucketName)
}

func TestValidateKVKey(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateKVKey("pay-42"))
	require.NoError(t, ValidateKVKey("a/b=c_d.e"))
	require.ErrorIs(t, ValidateKVKey(""), ErrInvalidKVKey)
	require.ErrorIs(t, ValidateKVKey("bad key"), ErrInvalidKVKey)
	require.ErrorIs(t, ValidateKVKey("bad*key"), ErrInvalidKVKey)
	require.ErrorIs(t, ValidateKVKey(strings.Repeat("a", maxNameLen+1)), ErrInvalidKVKey)
}

func TestValidateSubject(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateSubject("orders"))
	require.NoError(t, ValidateSubject("orders.created"))
	require.NoError(t, ValidateSubject("orders.*"))
	require.NoError(t, ValidateSubject("orders.>"))
	require.NoError(t, ValidateSubject("a.*.b.>"))
	require.NoError(t, ValidateSubject("*"))
	require.NoError(t, ValidateSubject(">"))

	require.ErrorIs(t, ValidateSubject(""), ErrInvalidSubject)
	require.ErrorIs(t, ValidateSubject(".orders"), ErrInvalidSubject)
	require.ErrorIs(t, ValidateSubject("orders."), ErrInvalidSubject)
	require.ErrorIs(t, ValidateSubject("orders..created"), ErrInvalidSubject)
	require.ErrorIs(t, ValidateSubject("orders.>.created"), ErrInvalidSubject)
	require.ErrorIs(t, ValidateSubject("ord*ers"), ErrInvalidSubject)
	require.ErrorIs(t, ValidateSubject("orders created"), ErrInvalidSubject)
}

func TestValidatePublishSubject(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidatePublishSubject("orders.created"))
	require.ErrorIs(t, ValidatePublishSubject(""), ErrInvalidSubject)
	require.ErrorIs(t, ValidatePublishSubject("orders.*"), ErrInvalidSubject)
	require.ErrorIs(t, ValidatePublishSubject("orders.>"), ErrInvalidSubject)
	require.ErrorIs(t, ValidatePublishSubject("*"), ErrInvalidSubject)
}

func TestValidatePublishSubjectNoAlloc(t *testing.T) {
	subject := "orders.created.region.us"
	allocs := testing.AllocsPerRun(1000, func() {
		_ = ValidatePublishSubject(subject)
	})
	assert.Zero(t, allocs)
}

func TestValidateSubjects(t *testing.T) {
	t.Parallel()

	require.NoError(t, ValidateSubjects(nil))
	require.NoError(t, ValidateSubjects([]string{"orders.>", "payments.>"}))
	require.ErrorIs(t, ValidateSubjects([]string{"orders.>", "bad subject"}), ErrInvalidSubject)
}

func TestPublisherRejectsWildcardSubject(t *testing.T) {
	t.Parallel()

	p := &publisher{}
	err := p.publish(t.Context(), "orders.>", Message{MessageType: JSON, Data: map[string]string{"id": "1"}})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidSubject)
}
