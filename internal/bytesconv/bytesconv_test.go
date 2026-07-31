package bytesconv

import (
	"bytes"
	"testing"
)

func TestIsEmpty(t *testing.T) {
	if !IsEmpty("") {
		t.Fatal("IsEmpty(\"\") = false, want true")
	}
	if IsEmpty("x") {
		t.Fatal("IsEmpty(\"x\") = true, want false")
	}
}

func TestStringToBytesEmpty(t *testing.T) {
	if got := StringToBytes(""); got != nil {
		t.Fatalf("StringToBytes(\"\") = %v, want nil", got)
	}
}

func TestBytesToStringEmpty(t *testing.T) {
	if got := BytesToString(nil); got != "" {
		t.Fatalf("BytesToString(nil) = %q, want \"\"", got)
	}
	if got := BytesToString([]byte{}); got != "" {
		t.Fatalf("BytesToString([]byte{}) = %q, want \"\"", got)
	}
}

func TestStringToBytesContent(t *testing.T) {
	s := "hello"
	b := StringToBytes(s)
	if string(b) != s {
		t.Fatalf("StringToBytes(%q) content = %q", s, b)
	}
}

func TestBytesToStringContent(t *testing.T) {
	b := []byte("world")
	s := BytesToString(b)
	if s != "world" {
		t.Fatalf("BytesToString = %q, want world", s)
	}
}

func TestStringToBytesSharesMemory(t *testing.T) {
	// Document that the slice aliases string data (read-only contract).
	s := "alias"
	b1 := StringToBytes(s)
	b2 := StringToBytes(s)
	if len(b1) == 0 || &b1[0] != &b2[0] {
		t.Fatal("StringToBytes should share backing with string")
	}
}

func TestRoundTripContent(t *testing.T) {
	orig := []byte("round-trip")
	s := BytesToString(orig)
	b := StringToBytes(s)
	if !bytes.Equal(b, orig) {
		t.Fatalf("round-trip = %q, want %q", b, orig)
	}
}

func BenchmarkStringToBytes(b *testing.B) {
	s := "benchmark-subject.orders.created"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = StringToBytes(s)
	}
}

func BenchmarkStringToBytesStd(b *testing.B) {
	s := "benchmark-subject.orders.created"
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = []byte(s)
	}
}

func BenchmarkBytesToString(b *testing.B) {
	buf := []byte("benchmark-subject.orders.created")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = BytesToString(buf)
	}
}

func BenchmarkBytesToStringStd(b *testing.B) {
	buf := []byte("benchmark-subject.orders.created")
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = string(buf)
	}
}
