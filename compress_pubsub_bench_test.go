package nats

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"

	natspkg "github.com/nats-io/nats.go"
)

const pubSubBenchPayloadBytes = 64 << 10

type pubSubBenchPayload struct {
	Data string `json:"data" msgpack:"data"`
}

func largeCompressibleString(n int) string {
	const chunk = "abcdefghijklmnopqrstuvwxyz0123456789"
	var b strings.Builder
	b.Grow(n)
	for b.Len() < n {
		need := n - b.Len()
		if need < len(chunk) {
			b.WriteString(chunk[:need])
			break
		}
		b.WriteString(chunk)
	}
	return b.String()
}

func BenchmarkPubSubPayload(b *testing.B) {
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
	jsonMsg := Message{Data: pubSubBenchPayload{Data: body}, MessageType: JSON}
	msgpackMsg := Message{Data: pubSubBenchPayload{Data: body}, MessageType: MessagePack}
	protoMsg := Message{Data: wrapperspb.String(body), MessageType: Proto}

	encoded := map[MessageType][]byte{}
	for _, c := range []Message{jsonMsg, msgpackMsg, protoMsg} {
		raw, err := Encode(c)
		if err != nil {
			b.Fatal(err)
		}
		if len(raw) <= MinPayloadCompressBytes {
			b.Fatalf("codec %v encoded size %d <= threshold %d", c.MessageType, len(raw), MinPayloadCompressBytes)
		}
		encoded[c.MessageType] = raw
	}

	for _, codec := range codecs {
		for _, mode := range modes {
			b.Run(fmt.Sprintf("%s/%s", codec.name, mode.name), func(b *testing.B) {
				runPubSubPayloadBench(b, codec.typ, mode.mode, encoded[codec.typ], body)
			})
		}
	}
}

func runPubSubPayloadBench(b *testing.B, typ MessageType, mode PayloadCompressionMode, plainEncoded []byte, body string) {
	b.Helper()

	ctx := context.Background()
	cfg := maxThroughputConfig()
	cfg.Conn.Address = startTestNATSServer(b)
	cfg.Conn.AllowReconnect = false
	disableTelemetry(&cfg)
	cfg.PublisherConfig.PayloadCompression = mode
	cfg.RuntimeConsumer.PayloadDecompression = true
	cfg.RuntimeConsumer.WorkerPoolEnabled = false

	client, err := NewClient(ctx, &cfg)
	require.NoError(b, err)
	b.Cleanup(func() { _ = client.Connector().Shutdown() })

	stream := uniqueStream(b, "PSBENCH")
	prefix := streamSubjectPrefix(stream)
	subject := prefix + "evt"
	durable := uniqueDurable(b, "psbench")

	_, err = client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage, Replicas: 1,
	})
	require.NoError(b, err)

	done := make(chan struct{}, 1)
	_, err = client.Consumer().SubscribeBound(ctx, stream, durable, prefix+">",
		func(_ context.Context, msg *natspkg.Msg) error {
			switch typ {
			case JSON, MessagePack:
				var got pubSubBenchPayload
				if decodeErr := DecodeMsg(msg, typ, &got); decodeErr != nil {
					return decodeErr
				}
				if got.Data != body {
					return fmt.Errorf("payload mismatch")
				}
			case Proto:
				var got wrapperspb.StringValue
				if decodeErr := DecodeMsg(msg, Proto, &got); decodeErr != nil {
					return decodeErr
				}
				if got.GetValue() != body {
					return fmt.Errorf("proto payload mismatch")
				}
			case Raw:
				// not used in this benchmark
			default:
				return fmt.Errorf("unknown type")
			}
			select {
			case done <- struct{}{}:
			default:
			}
			return nil
		})
	require.NoError(b, err)

	publish := func() error {
		switch typ {
		case JSON:
			return client.Publisher().PublishJSON(ctx, subject, pubSubBenchPayload{Data: body})
		case MessagePack:
			return client.Publisher().PublishMsgPack(ctx, subject, pubSubBenchPayload{Data: body})
		case Proto:
			return client.Publisher().PublishProto(ctx, subject, wrapperspb.String(body))
		case Raw:
			return fmt.Errorf("raw not supported")
		default:
			return fmt.Errorf("unknown type")
		}
	}

	waitOne := func() {
		select {
		case <-done:
		case <-time.After(testWaitShort):
			b.Fatal("timeout waiting for message")
		}
	}

	require.NoError(b, publish())
	waitOne()

	var wireBytes int
	if mode != PayloadCompressionOff {
		last, err := client.Replay().GetLastMsgForSubject(ctx, stream, subject)
		require.NoError(b, err)
		wireBytes = len(last.Data)
		enc := ""
		if last.Header != nil {
			if vs := last.Header[HeaderContentEncoding]; len(vs) > 0 {
				enc = vs[0]
			}
		}
		switch mode {
		case PayloadCompressionOff:
			// wireBytes stays plainEncoded size
		case PayloadCompressionGzip:
			require.Equal(b, EncodingGzip, enc)
		case PayloadCompressionBrotli:
			require.Equal(b, EncodingBrotli, enc)
		case PayloadCompressionAuto:
			// auto mode not used in this benchmark matrix
		}
		require.Less(b, wireBytes, len(plainEncoded))
	} else {
		wireBytes = len(plainEncoded)
	}

	b.SetBytes(int64(len(plainEncoded)))
	b.ReportAllocs()
	b.ResetTimer()

	for b.Loop() {
		if err := publish(); err != nil {
			b.Fatal(err)
		}
		waitOne()
	}

	b.StopTimer()
	b.ReportMetric(float64(wireBytes), "bytes_out")
	b.ReportMetric(float64(wireBytes)/float64(len(plainEncoded)), "ratio")
}
