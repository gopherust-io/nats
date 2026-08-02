package nats

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	natspkg "github.com/nats-io/nats.go"
)

func BenchmarkRequestReplyPayload(b *testing.B) {
	codecs := []struct {
		name string
		typ  MessageType
	}{
		{name: "json", typ: JSON},
		{name: "msgpack", typ: MessagePack},
		{name: "proto", typ: Proto},
	}
	modes := []struct {
		name string
		mode PayloadCompressionMode
	}{
		{name: "off", mode: PayloadCompressionOff},
		{name: "gzip", mode: PayloadCompressionGzip},
		{name: "br", mode: PayloadCompressionBrotli},
	}

	body := largeCompressibleString(pubSubBenchPayloadBytes)
	encoded := map[MessageType][]byte{}
	for _, c := range codecs {
		var (
			plain []byte
			err   error
		)
		switch c.typ {
		case JSON:
			plain, err = Encode(Message{Data: rrCompressPayload{Data: body}, MessageType: JSON})
		case MessagePack:
			plain, err = Encode(Message{Data: rrCompressPayload{Data: body}, MessageType: MessagePack})
		case Proto:
			plain, err = Encode(Message{Data: wrapperspb.String(body), MessageType: Proto})
		case Raw:
			err = fmt.Errorf("raw not supported")
		default:
			err = fmt.Errorf("unknown type")
		}
		require.NoError(b, err)
		require.Greater(b, len(plain), MinPayloadCompressBytes, "codec %s", c.name)
		encoded[c.typ] = plain
	}

	for _, codec := range codecs {
		for _, mode := range modes {
			b.Run(codec.name+"/"+mode.name, func(b *testing.B) {
				runRequestReplyPayloadBench(b, codec.typ, mode.mode, encoded[codec.typ], body)
			})
		}
	}
}

func runRequestReplyPayloadBench(b *testing.B, typ MessageType, mode PayloadCompressionMode, plainEncoded []byte, body string) {
	b.Helper()

	ctx := context.Background()
	cfg := DefaultConfig()
	cfg.Conn.Address = startTestNATSServer(b)
	cfg.Conn.AllowReconnect = false
	disableTelemetry(&cfg)
	cfg.RequesterConfig.PayloadCompression = mode
	cfg.RequesterConfig.PayloadDecompression = true
	cfg.ResponderConfig.PayloadCompression = mode
	cfg.ResponderConfig.PayloadDecompression = true

	client, err := NewClient(ctx, &cfg)
	require.NoError(b, err)
	b.Cleanup(func() { _ = client.Connector().Shutdown() })

	subject := uniqueDurable(b, "rr.bench")
	_, err = client.Responder().Subscribe(ctx, subject, func(_ context.Context, msg *natspkg.Msg) error {
		switch typ {
		case JSON:
			var in rrCompressPayload
			if decodeErr := Decode(msg.Data, JSON, &in); decodeErr != nil {
				return decodeErr
			}
			return client.Responder().RespondJSON(msg, in)
		case MessagePack:
			var in rrCompressPayload
			if decodeErr := Decode(msg.Data, MessagePack, &in); decodeErr != nil {
				return decodeErr
			}
			return client.Responder().RespondMsgPack(msg, in)
		case Proto:
			var in wrapperspb.StringValue
			if decodeErr := DecodeProto(msg.Data, &in); decodeErr != nil {
				return decodeErr
			}
			return client.Responder().RespondProto(msg, &in)
		case Raw:
			return fmt.Errorf("raw not supported")
		default:
			return fmt.Errorf("unknown type")
		}
	})
	require.NoError(b, err)

	requestOnce := func() error {
		switch typ {
		case JSON:
			var out rrCompressPayload
			return client.Requester().RequestJSONInto(ctx, subject, rrCompressPayload{Data: body}, &out)
		case MessagePack:
			var out rrCompressPayload
			return client.Requester().RequestMsgPackInto(ctx, subject, rrCompressPayload{Data: body}, &out)
		case Proto:
			var out wrapperspb.StringValue
			return client.Requester().RequestProtoInto(ctx, subject, wrapperspb.String(body), &out)
		case Raw:
			return fmt.Errorf("raw not supported")
		default:
			return fmt.Errorf("unknown type")
		}
	}

	require.NoError(b, requestOnce())

	wireBytes := len(plainEncoded)
	if mode != PayloadCompressionOff {
		out, hdr := applyPayloadCompression(mode, plainEncoded, nil)
		require.NotNil(b, hdr)
		require.Less(b, len(out), len(plainEncoded))
		wireBytes = len(out)
	}

	b.SetBytes(int64(len(plainEncoded)))
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if err := requestOnce(); err != nil {
			b.Fatal(err)
		}
	}

	b.StopTimer()
	b.ReportMetric(float64(wireBytes), "bytes_out")
	b.ReportMetric(float64(wireBytes)/float64(len(plainEncoded)), "ratio")
}
