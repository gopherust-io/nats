package nats

import (
	"fmt"
	"testing"
)

func BenchmarkPayloadCompression(b *testing.B) {
	sizes := []int{40 << 10, 64 << 10, 512 << 10}
	modes := []struct {
		fn   func([]byte) ([]byte, string, bool)
		name string
	}{
		{
			name: "off",
			fn: func(plain []byte) ([]byte, string, bool) {
				return plain, "", false
			},
		},
		{
			name: "gzip",
			fn: func(plain []byte) ([]byte, string, bool) {
				if len(plain) <= MinPayloadCompressBytes {
					return plain, "", false
				}
				out, err := compressGzip(plain)
				if err != nil || len(out) >= len(plain) {
					return plain, "", false
				}

				return out, EncodingGzip, true
			},
		},
		{
			name: "br",
			fn: func(plain []byte) ([]byte, string, bool) {
				if len(plain) <= MinPayloadCompressBytes {
					return plain, "", false
				}
				out, err := compressBrotli(plain)
				if err != nil || len(out) >= len(plain) {
					return plain, "", false
				}

				return out, EncodingBrotli, true
			},
		},
	}

	for _, size := range sizes {
		payload := largeJSON(size)
		for _, mode := range modes {
			b.Run(fmt.Sprintf("%s/%d", mode.name, size), func(b *testing.B) {
				b.SetBytes(int64(len(payload)))
				b.ReportAllocs()

				var outLen int
				b.ResetTimer()
				for b.Loop() {
					out, _, _ := mode.fn(payload)
					outLen = len(out)
				}
				b.ReportMetric(float64(outLen), "bytes_out")
				b.ReportMetric(float64(outLen)/float64(len(payload)), "ratio")
			})
		}
	}
}

func BenchmarkPayloadDecompression(b *testing.B) {
	plain := largeJSON(64 << 10)
	gzipped, err := compressGzip(plain)
	if err != nil {
		b.Fatal(err)
	}
	brotlied, err := compressBrotli(plain)
	if err != nil {
		b.Fatal(err)
	}

	b.Run("off", func(b *testing.B) {
		b.SetBytes(int64(len(plain)))
		b.ReportAllocs()
		for b.Loop() {
			_, _ = DecompressPayload(plain, "")
		}
	})

	b.Run("gzip", func(b *testing.B) {
		b.ReportMetric(float64(len(gzipped)), "bytes_in")
		b.SetBytes(int64(len(gzipped)))
		b.ReportAllocs()
		for b.Loop() {
			out, err := DecompressPayload(gzipped, EncodingGzip)
			if err != nil {
				b.Fatal(err)
			}
			if len(out) != len(plain) {
				b.Fatalf("len=%d want=%d", len(out), len(plain))
			}
		}
	})

	b.Run("br", func(b *testing.B) {
		b.ReportMetric(float64(len(brotlied)), "bytes_in")
		b.SetBytes(int64(len(brotlied)))
		b.ReportAllocs()
		for b.Loop() {
			out, err := DecompressPayload(brotlied, EncodingBrotli)
			if err != nil {
				b.Fatal(err)
			}
			if len(out) != len(plain) {
				b.Fatalf("len=%d want=%d", len(out), len(plain))
			}
		}
	})
}

func BenchmarkMaybeCompressPayloadAuto(b *testing.B) {
	payload := largeJSON(64 << 10)
	b.SetBytes(int64(len(payload)))
	b.ReportAllocs()

	var outLen int
	for b.Loop() {
		out, _, _ := MaybeCompressPayload(payload)
		outLen = len(out)
	}
	b.ReportMetric(float64(outLen), "bytes_out")
	b.ReportMetric(float64(outLen)/float64(len(payload)), "ratio")
}
