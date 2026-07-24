package dlq

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"sync"
	"testing"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testPublisher struct {
	subjects []string
	msgs     []RawPublish
	mu       sync.Mutex
}

func (p *testPublisher) PublishRaw(_ context.Context, subject string, msg RawPublish) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.subjects = append(p.subjects, subject)
	p.msgs = append(p.msgs, msg)

	return nil
}

func TestWithPassthroughWhenDisabled(t *testing.T) {
	t.Parallel()
	called := false
	h := With(Config{}, func(_ context.Context, _ *natspkg.Msg) error {
		called = true

		return nil
	})
	require.NoError(t, h(context.Background(), &natspkg.Msg{Subject: "a", Data: []byte("x")}))
	assert.True(t, called)
}

func TestWithHandlerRequestCopiesHeaders(t *testing.T) {
	t.Parallel()
	pub := &testPublisher{}
	h := With(Config{
		Publisher: pub,
		Subject:   "orders.dlq",
		Autopsy:   AutopsyConfig{Enabled: true},
	}, func(_ context.Context, _ *natspkg.Msg) error {
		return ErrSendToDLQ
	})

	err := h(context.Background(), &natspkg.Msg{
		Subject: "orders.created",
		Data:    []byte(`{"id":1}`),
		Header: natspkg.Header{
			headerMsgID:       []string{"m1"},
			headerTraceID:     []string{"trace-1"},
			headerContentType: []string{"application/json"},
		},
	})
	require.Error(t, err)

	pub.mu.Lock()
	defer pub.mu.Unlock()
	require.Len(t, pub.msgs, 1)
	assert.Equal(t, "trace-1", pub.msgs[0].Header[headerTraceID][0])
	assert.Equal(t, "application/json", pub.msgs[0].Header[headerContentType][0])
	assert.NotEmpty(t, pub.msgs[0].Header[HeaderAutopsyHash])
}

func TestWithNonDLQErrorPassthrough(t *testing.T) {
	t.Parallel()
	pub := &testPublisher{}
	want := errors.New("boom")
	h := With(Config{Publisher: pub, Subject: "dlq"}, func(_ context.Context, _ *natspkg.Msg) error {
		return want
	})
	err := h(context.Background(), &natspkg.Msg{Subject: "a", Data: []byte("x")})
	require.ErrorIs(t, err, want)
	assert.Empty(t, pub.subjects)
}

func TestApplyAutopsyHeaders(t *testing.T) {
	t.Parallel()
	cfg := AutopsyConfig{Enabled: true, IncludeStack: true}.withDefaults()
	headers := map[string][]string{}
	data := []byte(`{"id":1}`)
	applyAutopsyHeaders(headers, cfg, data, &autopsyInfo{
		Err:   "send message to dlq: bad payload",
		Stack: "goroutine 1 [running]:\nmain.main()",
	})

	sum := sha256.Sum256(data)
	assert.Equal(t, hex.EncodeToString(sum[:]), headers[HeaderAutopsyHash][0])
	assert.Contains(t, headers[HeaderAutopsyError][0], "bad payload")
	assert.Contains(t, headers[HeaderAutopsyStack][0], "goroutine")
}

func TestTruncateString(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "ab", truncateString("abcd", 2))
	assert.Equal(t, "abcd", truncateString("abcd", 10))
}

type recordingRec struct {
	autopsy bool
}

func (r *recordingRec) RecordDLQ(string, string, string, string, uint64) {}
func (r *recordingRec) RecordDLQAutopsy(string, string, string, string, string, uint64) {
	r.autopsy = true
}

func TestWithAutopsyPublishesHeaders(t *testing.T) {
	t.Parallel()
	rec := &recordingRec{}
	pub := &testPublisher{}
	h := With(Config{
		Publisher: pub,
		Subject:   "orders.dlq",
		Recorder:  rec,
		Autopsy:   AutopsyConfig{Enabled: true},
	}, func(_ context.Context, _ *natspkg.Msg) error {
		return ErrSendToDLQ
	})

	err := h(context.Background(), &natspkg.Msg{
		Subject: "orders.created",
		Data:    []byte(`{"id":9}`),
	})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "term after dlq")

	pub.mu.Lock()
	defer pub.mu.Unlock()
	require.Len(t, pub.msgs, 1)
	hdr := pub.msgs[0].Header
	require.NotEmpty(t, hdr[HeaderAutopsyHash])
	require.NotEmpty(t, hdr[HeaderAutopsyError])
	assert.Equal(t, "handler_requested", hdr[HeaderReason][0])
	assert.False(t, rec.autopsy) // Term failed before recorder
}
