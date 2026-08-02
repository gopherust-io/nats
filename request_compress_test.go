package nats

import (
	"context"
	"testing"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

type rrCompressPayload struct {
	Data string `json:"data" msgpack:"data"`
}

func TestRequestReplyCompressionRoundTrip(t *testing.T) {
	t.Parallel()
	body := largeCompressibleString(pubSubBenchPayloadBytes)
	require.Greater(t, len(body), MinPayloadCompressBytes)

	modes := []struct {
		name string
		mode PayloadCompressionMode
	}{
		{name: "gzip", mode: PayloadCompressionGzip},
		{name: "br", mode: PayloadCompressionBrotli},
	}
	for _, tc := range modes {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			client, ctx := testClientWithOptions(t, func(cfg *Config) {
				cfg.RequesterConfig.PayloadCompression = tc.mode
				cfg.RequesterConfig.PayloadDecompression = true
				cfg.ResponderConfig.PayloadCompression = tc.mode
				cfg.ResponderConfig.PayloadDecompression = true
			})
			subject := uniqueDurable(t, "rr.compress")

			_, err := client.Responder().Subscribe(ctx, subject, func(_ context.Context, msg *natspkg.Msg) error {
				var in rrCompressPayload
				require.NoError(t, Decode(msg.Data, JSON, &in))
				assert.Equal(t, body, in.Data)
				assert.Empty(t, msg.Header.Get(HeaderContentEncoding))
				return client.Responder().RespondJSON(msg, rrCompressPayload{Data: in.Data})
			})
			require.NoError(t, err)

			var out rrCompressPayload
			require.NoError(t, client.Requester().RequestJSONInto(ctx, subject, rrCompressPayload{Data: body}, &out))
			assert.Equal(t, body, out.Data)
		})
	}
}

func TestRequestReplyCompressionSetsContentEncodingOnWire(t *testing.T) {
	t.Parallel()
	body := largeCompressibleString(pubSubBenchPayloadBytes)
	client, ctx := testClientWithOptions(t, func(cfg *Config) {
		cfg.RequesterConfig.PayloadCompression = PayloadCompressionGzip
		cfg.RequesterConfig.PayloadDecompression = true
		// Leave request compressed for the handler so we can inspect the header.
		cfg.ResponderConfig.PayloadDecompression = false
		cfg.ResponderConfig.PayloadCompression = PayloadCompressionGzip
	})
	subject := uniqueDurable(t, "rr.wire")

	_, err := client.Responder().Subscribe(ctx, subject, func(_ context.Context, msg *natspkg.Msg) error {
		assert.Equal(t, EncodingGzip, msg.Header.Get(HeaderContentEncoding))
		require.NoError(t, maybeDecompressMsg(msg))
		var in rrCompressPayload
		require.NoError(t, Decode(msg.Data, JSON, &in))
		return client.Responder().RespondJSON(msg, in)
	})
	require.NoError(t, err)

	var out rrCompressPayload
	require.NoError(t, client.Requester().RequestJSONInto(ctx, subject, rrCompressPayload{Data: body}, &out))
	assert.Equal(t, body, out.Data)
}

func TestRequestReplyCompressionSkipsSmallPayload(t *testing.T) {
	t.Parallel()
	client, ctx := testClientWithOptions(t, func(cfg *Config) {
		cfg.RequesterConfig.PayloadCompression = PayloadCompressionGzip
		cfg.ResponderConfig.PayloadDecompression = false
	})
	subject := uniqueDurable(t, "rr.small")

	_, err := client.Responder().Subscribe(ctx, subject, func(_ context.Context, msg *natspkg.Msg) error {
		assert.Empty(t, msg.Header.Get(HeaderContentEncoding))
		return RespondBytes(msg, msg.Data)
	})
	require.NoError(t, err)

	reply, err := client.Requester().RequestJSON(ctx, subject, map[string]string{"id": "tiny"})
	require.NoError(t, err)
	assert.Empty(t, reply.Header.Get(HeaderContentEncoding))
}

func TestRequestReplyPayloadDecompressionOff(t *testing.T) {
	t.Parallel()
	body := largeCompressibleString(pubSubBenchPayloadBytes)
	client, ctx := testClientWithOptions(t, func(cfg *Config) {
		cfg.RequesterConfig.PayloadCompression = PayloadCompressionOff
		cfg.RequesterConfig.PayloadDecompression = false
		cfg.ResponderConfig.PayloadCompression = PayloadCompressionBrotli
		cfg.ResponderConfig.PayloadDecompression = true
	})
	subject := uniqueDurable(t, "rr.nodecomp")

	_, err := client.Responder().Subscribe(ctx, subject, func(_ context.Context, msg *natspkg.Msg) error {
		return client.Responder().RespondJSON(msg, rrCompressPayload{Data: body})
	})
	require.NoError(t, err)

	reply, err := client.Requester().RequestJSON(ctx, subject, rrCompressPayload{Data: "ping"})
	require.NoError(t, err)
	assert.Equal(t, EncodingBrotli, reply.Header.Get(HeaderContentEncoding))
	require.Greater(t, len(body), len(reply.Data))

	require.NoError(t, maybeDecompressMsg(reply))
	var out rrCompressPayload
	require.NoError(t, Decode(reply.Data, JSON, &out))
	assert.Equal(t, body, out.Data)
}

func TestRequestReplyCompressionMsgPackAndProto(t *testing.T) {
	t.Parallel()
	body := largeCompressibleString(pubSubBenchPayloadBytes)

	t.Run("msgpack", func(t *testing.T) {
		t.Parallel()
		client, ctx := testClientWithOptions(t, func(cfg *Config) {
			cfg.RequesterConfig.PayloadCompression = PayloadCompressionGzip
			cfg.ResponderConfig.PayloadCompression = PayloadCompressionGzip
		})
		subject := uniqueDurable(t, "rr.mp")
		_, err := client.Responder().Subscribe(ctx, subject, func(_ context.Context, msg *natspkg.Msg) error {
			var in rrCompressPayload
			require.NoError(t, Decode(msg.Data, MessagePack, &in))
			return client.Responder().RespondMsgPack(msg, in)
		})
		require.NoError(t, err)
		var out rrCompressPayload
		require.NoError(t, client.Requester().RequestMsgPackInto(ctx, subject, rrCompressPayload{Data: body}, &out))
		assert.Equal(t, body, out.Data)
	})

	t.Run("proto", func(t *testing.T) {
		t.Parallel()
		client, ctx := testClientWithOptions(t, func(cfg *Config) {
			cfg.RequesterConfig.PayloadCompression = PayloadCompressionBrotli
			cfg.ResponderConfig.PayloadCompression = PayloadCompressionBrotli
		})
		subject := uniqueDurable(t, "rr.proto")
		_, err := client.Responder().Subscribe(ctx, subject, func(_ context.Context, msg *natspkg.Msg) error {
			var in wrapperspb.StringValue
			require.NoError(t, DecodeProto(msg.Data, &in))
			return client.Responder().RespondProto(msg, &in)
		})
		require.NoError(t, err)
		var out wrapperspb.StringValue
		require.NoError(t, client.Requester().RequestProtoInto(ctx, subject, wrapperspb.String(body), &out))
		assert.Equal(t, body, out.GetValue())
	})
}
