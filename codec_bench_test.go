package nats

import (
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

// benchmarkOrderEvent is a representative JetStream payload used across codec benchmarks.
type benchmarkOrderEvent struct {
	ID        string  `json:"id"         msgpack:"id"`
	AccountID string  `json:"account_id" msgpack:"account_id"`
	Symbol    string  `json:"symbol"     msgpack:"symbol"`
	Status    string  `json:"status"     msgpack:"status"`
	Quantity  float64 `json:"quantity"   msgpack:"quantity"`
	Price     float64 `json:"price"      msgpack:"price"`
}

func benchmarkPayload() benchmarkOrderEvent {
	return benchmarkOrderEvent{
		ID:        "ord-8f3c2a91",
		AccountID: "acc-10482",
		Symbol:    "BTC-USD",
		Quantity:  0.25,
		Price:     67250.50,
		Status:    "filled",
	}
}

func benchmarkProtoPayload() *wrapperspb.StringValue {
	return wrapperspb.String("ord-8f3c2a91-filled-btc-usd-0.25@67250.50")
}

// BenchmarkCodecComparison runs encode, decode, and round-trip benchmarks for JSON,
// MessagePack, and Protobuf using the same logical order event.
func BenchmarkCodecComparison(b *testing.B) {
	jsonData, err := Encode(Message{Data: benchmarkPayload(), MessageType: JSON})
	if err != nil {
		b.Fatal(err)
	}
	msgpackData, err := Encode(Message{Data: benchmarkPayload(), MessageType: MessagePack})
	if err != nil {
		b.Fatal(err)
	}
	protoData, err := Encode(Message{Data: benchmarkProtoPayload(), MessageType: Proto})
	if err != nil {
		b.Fatal(err)
	}

	b.Logf("payload sizes: JSON=%d B, MessagePack=%d B, Proto=%d B",
		len(jsonData), len(msgpackData), len(protoData))

	b.Run("JSON/Encode", func(b *testing.B) {
		msg := Message{Data: benchmarkPayload(), MessageType: JSON}
		b.ReportAllocs()
		b.SetBytes(int64(len(jsonData)))
		b.ResetTimer()
		for range b.N {
			if _, err := Encode(msg); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("MessagePack/Encode", func(b *testing.B) {
		msg := Message{Data: benchmarkPayload(), MessageType: MessagePack}
		b.ReportAllocs()
		b.SetBytes(int64(len(msgpackData)))
		b.ResetTimer()
		for range b.N {
			if _, err := Encode(msg); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Proto/Encode", func(b *testing.B) {
		msg := Message{Data: benchmarkProtoPayload(), MessageType: Proto}
		b.ReportAllocs()
		b.SetBytes(int64(len(protoData)))
		b.ResetTimer()
		for range b.N {
			if _, err := Encode(msg); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("JSON/Decode", func(b *testing.B) {
		var dst benchmarkOrderEvent
		b.ReportAllocs()
		b.SetBytes(int64(len(jsonData)))
		b.ResetTimer()
		for range b.N {
			dst = benchmarkOrderEvent{}
			if err := Decode(jsonData, JSON, &dst); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("MessagePack/Decode", func(b *testing.B) {
		var dst benchmarkOrderEvent
		b.ReportAllocs()
		b.SetBytes(int64(len(msgpackData)))
		b.ResetTimer()
		for range b.N {
			dst = benchmarkOrderEvent{}
			if err := Decode(msgpackData, MessagePack, &dst); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Proto/Decode", func(b *testing.B) {
		var dst *wrapperspb.StringValue
		b.ReportAllocs()
		b.SetBytes(int64(len(protoData)))
		b.ResetTimer()
		for range b.N {
			dst = wrapperspb.String("")
			if err := DecodeProto(protoData, dst); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("JSON/RoundTrip", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			data, err := Encode(Message{Data: benchmarkPayload(), MessageType: JSON})
			if err != nil {
				b.Fatal(err)
			}
			var dst benchmarkOrderEvent
			if err := Decode(data, JSON, &dst); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("MessagePack/RoundTrip", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			data, err := Encode(Message{Data: benchmarkPayload(), MessageType: MessagePack})
			if err != nil {
				b.Fatal(err)
			}
			var dst benchmarkOrderEvent
			if err := Decode(data, MessagePack, &dst); err != nil {
				b.Fatal(err)
			}
		}
	})

	b.Run("Proto/RoundTrip", func(b *testing.B) {
		b.ReportAllocs()
		b.ResetTimer()
		for range b.N {
			data, err := Encode(Message{Data: benchmarkProtoPayload(), MessageType: Proto})
			if err != nil {
				b.Fatal(err)
			}
			dst := wrapperspb.String("")
			if err := DecodeProto(data, dst); err != nil {
				b.Fatal(err)
			}
		}
	})
}
