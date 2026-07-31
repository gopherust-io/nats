package nats

import (
	"context"
	"sync/atomic"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

func TestRequestBytesRoundTrip(t *testing.T) {
	client, ctx := testClient(t)
	subject := uniqueDurable(t, "rr.bytes")

	_, err := client.Responder().Subscribe(ctx, subject, func(_ context.Context, msg *natspkg.Msg) error {
		return RespondBytes(msg, append(bytesconv.StringToBytes("echo:"), msg.Data...))
	})
	require.NoError(t, err)

	reply, err := client.Requester().RequestBytes(ctx, subject, bytesconv.StringToBytes("ping"))
	require.NoError(t, err)
	assert.Equal(t, bytesconv.StringToBytes("echo:ping"), reply.Data)
}

func TestRequestJSONIntoRoundTrip(t *testing.T) {
	client, ctx := testClient(t)
	subject := uniqueDurable(t, "rr.json")

	type payload struct {
		ID string `json:"id"`
	}

	_, err := client.Responder().Subscribe(ctx, subject, func(_ context.Context, msg *natspkg.Msg) error {
		var in payload
		require.NoError(t, Decode(msg.Data, JSON, &in))
		return RespondJSON(msg, payload{ID: "ok-" + in.ID})
	})
	require.NoError(t, err)

	var out payload
	require.NoError(t, client.Requester().RequestJSONInto(ctx, subject, payload{ID: "1"}, &out))
	assert.Equal(t, "ok-1", out.ID)
}

func TestRequestMsgPackIntoRoundTrip(t *testing.T) {
	client, ctx := testClient(t)
	subject := uniqueDurable(t, "rr.msgpack")

	type payload struct {
		N int `msgpack:"n"`
	}

	_, err := client.Responder().Subscribe(ctx, subject, func(_ context.Context, msg *natspkg.Msg) error {
		var in payload
		require.NoError(t, Decode(msg.Data, MessagePack, &in))
		return RespondMsgPack(msg, payload{N: in.N + 1})
	})
	require.NoError(t, err)

	var out payload
	require.NoError(t, client.Requester().RequestMsgPackInto(ctx, subject, payload{N: 7}, &out))
	assert.Equal(t, 8, out.N)
}

func TestRequestProtoIntoRoundTrip(t *testing.T) {
	client, ctx := testClient(t)
	subject := uniqueDurable(t, "rr.proto")

	_, err := client.Responder().Subscribe(ctx, subject, func(_ context.Context, msg *natspkg.Msg) error {
		var in wrapperspb.StringValue
		require.NoError(t, DecodeProto(msg.Data, &in))
		return RespondProto(msg, wrapperspb.String("echo:"+in.GetValue()))
	})
	require.NoError(t, err)

	var out wrapperspb.StringValue
	require.NoError(t, client.Requester().RequestProtoInto(ctx, subject, wrapperspb.String("hi"), &out))
	assert.Equal(t, "echo:hi", out.GetValue())
}

func TestRequestTimeout(t *testing.T) {
	client, ctx := testClientWithOptions(t, func(cfg *Config) {
		cfg.RequesterConfig.Timeout = 50 * time.Millisecond
	})
	subject := uniqueDurable(t, "rr.timeout")

	_, err := client.Requester().RequestBytes(ctx, subject, bytesconv.StringToBytes("x"))
	require.Error(t, err)
}

func TestRequestQueueSubscribeOneReply(t *testing.T) {
	client, ctx := testClient(t)
	subject := uniqueDurable(t, "rr.queue")
	queue := uniqueQueue(t, "rrq")

	var hits atomic.Int32
	handler := func(_ context.Context, msg *natspkg.Msg) error {
		hits.Add(1)
		return RespondBytes(msg, bytesconv.StringToBytes("ok"))
	}

	_, err := client.Responder().QueueSubscribe(ctx, queue, subject, handler)
	require.NoError(t, err)
	_, err = client.Responder().QueueSubscribe(ctx, queue, subject, handler)
	require.NoError(t, err)

	for range 10 {
		reply, err := client.Requester().RequestBytes(ctx, subject, bytesconv.StringToBytes("x"))
		require.NoError(t, err)
		assert.Equal(t, bytesconv.StringToBytes("ok"), reply.Data)
	}
	assert.Equal(t, int32(10), hits.Load())
}

func TestRequestInvalidSubject(t *testing.T) {
	client, ctx := testClient(t)
	_, err := client.Requester().RequestBytes(ctx, "", bytesconv.StringToBytes("x"))
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrEmptySubjectNotAllowed)
}

func TestRequestHeadersReachResponder(t *testing.T) {
	client, ctx := testClient(t)
	subject := uniqueDurable(t, "rr.hdr")

	seen := make(chan string, 1)
	_, err := client.Responder().Subscribe(ctx, subject, func(_ context.Context, msg *natspkg.Msg) error {
		seen <- msg.Header.Get("X-Test")
		return RespondBytes(msg, bytesconv.StringToBytes("ok"))
	})
	require.NoError(t, err)

	_, err = client.Requester().RequestMessage(ctx, subject, Message{
		Data:        bytesconv.StringToBytes("x"),
		MessageType: Raw,
		Header:      map[string][]string{"X-Test": {"abc"}},
	})
	require.NoError(t, err)
	assert.Equal(t, "abc", <-seen)
}

func TestRequestTracingSpan(t *testing.T) {
	client, ctx, sr := testClientWithTracing(t)
	subject := uniqueDurable(t, "rr.trace")

	_, err := client.Responder().Subscribe(ctx, subject, func(_ context.Context, msg *natspkg.Msg) error {
		return RespondBytes(msg, bytesconv.StringToBytes("ok"))
	})
	require.NoError(t, err)

	_, err = client.Requester().RequestBytes(ctx, subject, bytesconv.StringToBytes("x"))
	require.NoError(t, err)

	require.Eventually(t, func() bool {
		for _, s := range sr.Ended() {
			if s.Name() == "nats.request" {
				return true
			}
		}
		return false
	}, 2*time.Second, 20*time.Millisecond)
}
