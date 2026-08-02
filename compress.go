package nats

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"sync"

	"github.com/andybalholm/brotli"
	"github.com/klauspost/compress/gzip"

	natspkg "github.com/nats-io/nats.go"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

const (
	// MinPayloadCompressBytes is the exclusive lower bound for auto payload
	// compression (strictly greater than 32 KiB), matching nats-consol.
	MinPayloadCompressBytes = 32 << 10

	// MaxPayloadDecompressBytes caps expanded payload size (zip-bomb defense).
	MaxPayloadDecompressBytes = 64 << 20

	EncodingBrotli = "br"
	EncodingGzip   = "gzip"
)

var compressBufPool = sync.Pool{
	New: func() any {
		return new(bytes.Buffer)
	},
}

// MaybeCompressPayload compresses plain when larger than MinPayloadCompressBytes.
// Prefer brotli, then gzip (best-speed). Returns ok only when compressed size is
// strictly smaller than plain.
func MaybeCompressPayload(plain []byte) ([]byte, string, bool) {
	if len(plain) <= MinPayloadCompressBytes {
		return plain, "", false
	}

	if compressed, err := compressBrotli(plain); err == nil && len(compressed) < len(plain) {
		return compressed, EncodingBrotli, true
	}

	if compressed, err := compressGzip(plain); err == nil && len(compressed) < len(plain) {
		return compressed, EncodingGzip, true
	}

	return plain, "", false
}

// DecompressPayload expands data according to Content-Encoding (br / gzip).
// Empty or "identity" encoding returns data unchanged.
func DecompressPayload(data []byte, encoding string) ([]byte, error) {
	switch strings.ToLower(strings.TrimSpace(encoding)) {
	case "", "identity":
		return data, nil
	case EncodingBrotli:
		return decompressBrotli(data)
	case EncodingGzip:
		return decompressGzip(data)
	default:
		return nil, fmt.Errorf("%w: %q", ErrUnsupportedContentEncoding, encoding)
	}
}

const maxCompressPoolCap = 1 << 20 // drop pooled buffers larger than 1 MiB

func putCompressBuf(buf *bytes.Buffer) {
	if buf.Cap() > maxCompressPoolCap {
		return
	}
	compressBufPool.Put(buf)
}

func compressBrotli(plain []byte) ([]byte, error) {
	buf := compressBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	w := brotli.NewWriterLevel(buf, brotli.BestSpeed)
	if _, err := w.Write(plain); err != nil {
		_ = w.Close()
		putCompressBuf(buf)

		return nil, err
	}
	if err := w.Close(); err != nil {
		putCompressBuf(buf)

		return nil, err
	}
	out := append([]byte(nil), buf.Bytes()...)
	putCompressBuf(buf)

	return out, nil
}

func compressGzip(plain []byte) ([]byte, error) {
	buf := compressBufPool.Get().(*bytes.Buffer)
	buf.Reset()
	w, err := gzip.NewWriterLevel(buf, gzip.BestSpeed)
	if err != nil {
		putCompressBuf(buf)

		return nil, err
	}
	if _, err := w.Write(plain); err != nil {
		_ = w.Close()
		putCompressBuf(buf)

		return nil, err
	}
	if err := w.Close(); err != nil {
		putCompressBuf(buf)

		return nil, err
	}
	out := append([]byte(nil), buf.Bytes()...)
	putCompressBuf(buf)

	return out, nil
}

func decompressBrotli(data []byte) ([]byte, error) {
	r := brotli.NewReader(bytes.NewReader(data))
	out, err := io.ReadAll(io.LimitReader(r, int64(MaxPayloadDecompressBytes)+1))
	if err != nil {
		return nil, fmt.Errorf("brotli decompress: %w", err)
	}
	if len(out) > MaxPayloadDecompressBytes {
		return nil, fmt.Errorf("%w: brotli exceeds %d bytes", ErrPayloadTooLarge, MaxPayloadDecompressBytes)
	}

	return out, nil
}

func decompressGzip(data []byte) ([]byte, error) {
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("gzip decompress: %w", err)
	}
	defer r.Close()

	out, err := io.ReadAll(io.LimitReader(r, int64(MaxPayloadDecompressBytes)+1))
	if err != nil {
		return nil, fmt.Errorf("gzip decompress: %w", err)
	}
	if len(out) > MaxPayloadDecompressBytes {
		return nil, fmt.Errorf("%w: gzip exceeds %d bytes", ErrPayloadTooLarge, MaxPayloadDecompressBytes)
	}

	return out, nil
}

func contentEncodingFromHeader(h map[string][]string) string {
	if h == nil {
		return ""
	}
	vals := h[HeaderContentEncoding]
	if len(vals) == 0 {
		return ""
	}

	return vals[0]
}

func applyPayloadCompression(mode PayloadCompressionMode, data []byte, header map[string][]string) ([]byte, map[string][]string) {
	if mode == PayloadCompressionOff {
		return data, header
	}
	if !bytesconv.IsEmpty(contentEncodingFromHeader(header)) {
		return data, header
	}

	var (
		out []byte
		enc string
		ok  bool
	)
	switch mode {
	case PayloadCompressionOff:
		return data, header
	case PayloadCompressionAuto:
		out, enc, ok = MaybeCompressPayload(data)
	case PayloadCompressionGzip:
		out, enc, ok = compressPayloadAlgo(data, EncodingGzip)
	case PayloadCompressionBrotli:
		out, enc, ok = compressPayloadAlgo(data, EncodingBrotli)
	default:
		return data, header
	}
	if !ok {
		return data, header
	}

	if header == nil {
		header = make(map[string][]string, 1)
	}
	header[HeaderContentEncoding] = []string{enc}

	return out, header
}

// compressPayloadAlgo applies a single algorithm with the same threshold and
// shrink-only rules as MaybeCompressPayload.
func compressPayloadAlgo(plain []byte, encoding string) ([]byte, string, bool) {
	if len(plain) <= MinPayloadCompressBytes {
		return plain, "", false
	}
	var (
		compressed []byte
		err        error
	)
	switch encoding {
	case EncodingGzip:
		compressed, err = compressGzip(plain)
	case EncodingBrotli:
		compressed, err = compressBrotli(plain)
	default:
		return plain, "", false
	}
	if err != nil || len(compressed) >= len(plain) {
		return plain, "", false
	}

	return compressed, encoding, true
}

// maybeDecompressMsg expands Content-Encoding on msg in place and strips the header.
func maybeDecompressMsg(msg *natspkg.Msg) error {
	if msg == nil || msg.Header == nil {
		return nil
	}

	enc := msg.Header.Get(HeaderContentEncoding)
	if bytesconv.IsEmpty(enc) {
		return nil
	}

	out, err := DecompressPayload(msg.Data, enc)
	if err != nil {
		return err
	}

	msg.Data = out
	msg.Header.Del(HeaderContentEncoding)

	return nil
}
