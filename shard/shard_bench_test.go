package shard

import "testing"

func BenchmarkIndex(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Index("account-42", 8)
	}
}

func BenchmarkSubject(b *testing.B) {
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Subject("orders.shard", "acct-1", 8, "created")
	}
}
