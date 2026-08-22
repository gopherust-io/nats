package dlq_test

import (
	"context"
	"testing"

	"github.com/gopherust-io/nats/dlq"
	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type memSource struct {
	msgs []*natspkg.RawStreamMsg
}

func (m *memSource) GetMsg(_ context.Context, _ string, seq uint64) (*natspkg.RawStreamMsg, error) {
	for _, msg := range m.msgs {
		if msg.Sequence == seq {
			return msg, nil
		}
	}

	return nil, natspkg.ErrMsgNotFound
}

func (m *memSource) GetNextMsgAfter(_ context.Context, _ string, seq uint64) (*natspkg.RawStreamMsg, error) {
	for _, msg := range m.msgs {
		if msg.Sequence > seq {
			return msg, nil
		}
	}

	return nil, natspkg.ErrMsgNotFound
}

type memPub struct {
	subjects []string
	count    int
}

func (p *memPub) PublishRaw(_ context.Context, subject string, _ dlq.RawPublish) error {
	p.subjects = append(p.subjects, subject)
	p.count++

	return nil
}

func TestReplayToStreamDryRunAndFilter(t *testing.T) {
	t.Parallel()
	src := &memSource{msgs: []*natspkg.RawStreamMsg{
		{
			Sequence: 1,
			Subject:  "dlq.orders",
			Data:     []byte(`{"a":1}`),
			Header: natspkg.Header{
				dlq.HeaderReason:          []string{"max_deliver"},
				dlq.HeaderOriginalSubject: []string{"orders.created"},
			},
		},
		{
			Sequence: 2,
			Subject:  "dlq.orders",
			Data:     []byte(`{"a":2}`),
			Header: natspkg.Header{
				dlq.HeaderReason:          []string{"handler"},
				dlq.HeaderOriginalSubject: []string{"orders.updated"},
			},
		},
	}}
	pub := &memPub{}
	res, err := dlq.ReplayToStream(context.Background(), src, pub, dlq.ReplayConfig{
		SourceStream: "DLQ",
		DestSubject:  "orders.redrive",
		ReasonFilter: "handler",
		Limit:        10,
		DryRun:       true,
	})
	require.NoError(t, err)
	assert.Equal(t, 2, res.Considered)
	assert.Equal(t, 1, res.Matched)
	assert.Equal(t, 0, res.Published)
	assert.Equal(t, 0, pub.count)

	res, err = dlq.ReplayToStream(context.Background(), src, pub, dlq.ReplayConfig{
		SourceStream: "DLQ",
		DestSubject:  "orders.redrive",
		ReasonFilter: "handler",
		Limit:        10,
	})
	require.NoError(t, err)
	assert.Equal(t, 1, res.Published)
	assert.Equal(t, []string{"orders.redrive"}, pub.subjects)
}
