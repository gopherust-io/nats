package nats

import (
	"testing"

	"github.com/gopherust-io/nats/internal/bytesconv"
	natspkg "github.com/nats-io/nats.go"
)

func FuzzDecodeJSON(f *testing.F) {
	f.Add(bytesconv.StringToBytes(`{"id":"1"}`))
	f.Fuzz(func(t *testing.T, data []byte) {
		var dst map[string]any
		msg := &natspkg.Msg{
			Data:   data,
			Header: natspkg.Header{HeaderContentType: []string{ContentTypeJSON}},
		}
		_ = DecodeMsg(msg, JSON, &dst)
	})
}

func FuzzCommonWildcardSubject(f *testing.F) {
	f.Add("orders.created")
	f.Add("payments.settled")
	f.Fuzz(func(t *testing.T, s string) {
		_ = commonWildcardSubject([]string{s, "orders.created"})
		_ = consumerFilterSubjects([]string{s})
	})
}
