package nats

import (
	"context"
	"crypto/rand"
	"strings"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func largeJSON(n int) []byte {
	if n <= 0 {
		return nil
	}
	var b strings.Builder
	b.Grow(n)
	b.WriteString(`{"data":"`)
	for b.Len() < n-2 {
		need := n - 2 - b.Len()
		chunk := "abcdefghijklmnopqrstuvwxyz0123456789"
		if need < len(chunk) {
			b.WriteString(chunk[:need])
			break
		}
		b.WriteString(chunk)
	}
	b.WriteString(`"}`)
	out := []byte(b.String())
	if len(out) > n {
		return out[:n]
	}
	for len(out) < n {
		out = append(out, 'x')
	}

	return out
}

func TestMaybeCompressPayloadThreshold(t *testing.T) {
	t.Parallel()

	exact := largeJSON(MinPayloadCompressBytes)
	out, enc, ok := MaybeCompressPayload(exact)
	assert.False(t, ok)
	assert.Empty(t, enc)
	assert.Equal(t, exact, out)

	above := largeJSON(MinPayloadCompressBytes + 1)
	out, enc, ok = MaybeCompressPayload(above)
	require.True(t, ok)
	assert.Contains(t, []string{EncodingBrotli, EncodingGzip}, enc)
	assert.Less(t, len(out), len(above))
}

func TestMaybeCompressPayloadRoundTrip(t *testing.T) {
	t.Parallel()

	plain := largeJSON(64 << 10)
	compressed, enc, ok := MaybeCompressPayload(plain)
	require.True(t, ok)
	require.NotEmpty(t, enc)

	got, err := DecompressPayload(compressed, enc)
	require.NoError(t, err)
	assert.Equal(t, plain, got)
}

func TestMaybeCompressPayloadPreferBrotli(t *testing.T) {
	t.Parallel()

	plain := largeJSON(64 << 10)
	_, enc, ok := MaybeCompressPayload(plain)
	require.True(t, ok)
	assert.Equal(t, EncodingBrotli, enc)
}

func TestMaybeCompressPayloadIncompressibleSkips(t *testing.T) {
	t.Parallel()

	plain := make([]byte, MinPayloadCompressBytes+1024)
	_, err := rand.Read(plain)
	require.NoError(t, err)

	out, enc, ok := MaybeCompressPayload(plain)
	// Random data rarely shrinks at best-speed; if it does, still a valid outcome.
	if ok {
		assert.Less(t, len(out), len(plain))
		assert.Contains(t, []string{EncodingBrotli, EncodingGzip}, enc)
	} else {
		assert.Empty(t, enc)
		assert.Equal(t, plain, out)
	}
}

func TestDecompressPayloadUnsupported(t *testing.T) {
	t.Parallel()

	_, err := DecompressPayload([]byte("x"), "zstd")
	require.ErrorIs(t, err, ErrUnsupportedContentEncoding)
}

func TestDecompressPayloadIdentity(t *testing.T) {
	t.Parallel()

	plain := []byte("hello")
	got, err := DecompressPayload(plain, "")
	require.NoError(t, err)
	assert.Equal(t, plain, got)

	got, err = DecompressPayload(plain, "identity")
	require.NoError(t, err)
	assert.Equal(t, plain, got)
}

func TestApplyPayloadCompressionSkipsExistingEncoding(t *testing.T) {
	t.Parallel()

	plain := largeJSON(64 << 10)
	hdr := map[string][]string{HeaderContentEncoding: {EncodingGzip}}
	out, outHdr := applyPayloadCompression(PayloadCompressionAuto, plain, hdr)
	assert.Equal(t, plain, out)
	assert.Equal(t, EncodingGzip, outHdr[HeaderContentEncoding][0])
}

func TestApplyPayloadCompressionOff(t *testing.T) {
	t.Parallel()

	plain := largeJSON(64 << 10)
	out, hdr := applyPayloadCompression(PayloadCompressionOff, plain, nil)
	assert.Equal(t, plain, out)
	assert.Nil(t, hdr)
}

func TestApplyPayloadCompressionForcedGzipBrotli(t *testing.T) {
	t.Parallel()

	plain := largeJSON(64 << 10)

	out, hdr := applyPayloadCompression(PayloadCompressionGzip, plain, nil)
	require.NotNil(t, hdr)
	assert.Equal(t, EncodingGzip, hdr[HeaderContentEncoding][0])
	assert.Less(t, len(out), len(plain))
	got, err := DecompressPayload(out, EncodingGzip)
	require.NoError(t, err)
	assert.Equal(t, plain, got)

	out, hdr = applyPayloadCompression(PayloadCompressionBrotli, plain, nil)
	require.NotNil(t, hdr)
	assert.Equal(t, EncodingBrotli, hdr[HeaderContentEncoding][0])
	assert.Less(t, len(out), len(plain))
	got, err = DecompressPayload(out, EncodingBrotli)
	require.NoError(t, err)
	assert.Equal(t, plain, got)
}

func TestApplyPayloadCompressionForcedSkipsSmall(t *testing.T) {
	t.Parallel()

	plain := largeJSON(MinPayloadCompressBytes)
	out, hdr := applyPayloadCompression(PayloadCompressionGzip, plain, nil)
	assert.Equal(t, plain, out)
	assert.Nil(t, hdr)
	out, hdr = applyPayloadCompression(PayloadCompressionBrotli, plain, nil)
	assert.Equal(t, plain, out)
	assert.Nil(t, hdr)
}

func TestMaybeDecompressMsg(t *testing.T) {
	t.Parallel()

	plain := largeJSON(40 << 10)
	compressed, enc, ok := MaybeCompressPayload(plain)
	require.True(t, ok)

	msg := &natspkg.Msg{
		Data:   append([]byte(nil), compressed...),
		Header: natspkg.Header{HeaderContentEncoding: []string{enc}},
	}
	require.NoError(t, maybeDecompressMsg(msg))
	assert.Equal(t, plain, msg.Data)
	assert.Empty(t, msg.Header.Get(HeaderContentEncoding))
}

func TestDecodeMsgDecompresses(t *testing.T) {
	t.Parallel()

	type payload struct {
		Data string `json:"data"`
	}
	plainObj := payload{Data: strings.Repeat("x", MinPayloadCompressBytes)}
	encoded, err := Encode(Message{Data: plainObj, MessageType: JSON})
	require.NoError(t, err)

	compressed, enc, ok := MaybeCompressPayload(encoded)
	require.True(t, ok)

	msg := &natspkg.Msg{
		Data: compressed,
		Header: natspkg.Header{
			HeaderContentType:     []string{ContentTypeJSON},
			HeaderContentEncoding: []string{enc},
		},
	}
	var got payload
	require.NoError(t, DecodeMsgWithDecompress(msg, 0, &got))
	assert.Equal(t, plainObj.Data, got.Data)
	assert.Empty(t, msg.Header.Get(HeaderContentEncoding))
}

func TestPublishConsumePayloadCompression(t *testing.T) {
	t.Parallel()

	client, ctx := testClientWithOptions(t, func(cfg *Config) {
		cfg.PublisherConfig.PayloadCompression = PayloadCompressionAuto
		cfg.RuntimeConsumer.PayloadDecompression = true
		cfg.RuntimeConsumer.WorkerPoolEnabled = false
	})

	stream := uniqueStream(t, "COMPRESS")
	prefix := streamSubjectPrefix(stream)
	subject := prefix + "big"
	durable := uniqueDurable(t, "compress-worker")

	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1,
	})
	require.NoError(t, err)

	type payload struct {
		Data string `json:"data"`
	}
	want := payload{Data: strings.Repeat("abcdefghijklmnopqrstuvwxyz", 2000)} // >32 KiB JSON

	done := make(chan payload, 1)
	_, err = client.Consumer().SubscribeBound(ctx, stream, durable, prefix+">",
		func(_ context.Context, msg *natspkg.Msg) error {
			assert.Empty(t, msg.Header.Get(HeaderContentEncoding))
			var got payload
			require.NoError(t, DecodeMsg(msg, 0, &got))
			done <- got
			return nil
		})
	require.NoError(t, err)

	require.NoError(t, client.Publisher().PublishJSON(ctx, subject, want))

	meta, err := client.Replay().GetLastMsgForSubject(ctx, stream, subject)
	require.NoError(t, err)
	require.NotNil(t, meta)
	assert.Equal(t, EncodingBrotli, natspkg.Header(meta.Header).Get(HeaderContentEncoding))
	assert.Less(t, len(meta.Data), MinPayloadCompressBytes)

	select {
	case got := <-done:
		assert.Equal(t, want.Data, got.Data)
	case <-time.After(testWaitShort):
		t.Fatal("timeout waiting for decompressed message")
	}
}
