package nats

import (
	"errors"
	"fmt"

	natspkg "github.com/nats-io/nats.go"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

// Schema contract headers (optional; not a full registry).
const (
	HeaderSchema          = "X-NATS-Schema"
	HeaderSchemaVersion   = "X-NATS-Schema-Version"
	HeaderContractEncoding = "X-NATS-Contract-Encoding"
)

// SchemaExpectation validates optional contract headers on decode.
//
// goalign:ignore
type SchemaExpectation struct {
	Name     string
	Version  string
	Encoding string // optional; when empty, Content-Type / Nats-Content-Type is used
	Required bool
}

// ErrSchemaMismatch is returned when message headers do not match SchemaExpectation.
var ErrSchemaMismatch = errors.New("schema contract mismatch")

// ApplySchemaHeaders sets schema/contract headers on a Message before publish.
func ApplySchemaHeaders(msg *Message, name, version, encoding string) {
	if msg == nil {
		return
	}
	if msg.Header == nil {
		msg.Header = make(map[string][]string, 3)
	}
	if !bytesconv.IsEmpty(name) {
		msg.Header[HeaderSchema] = []string{name}
	}
	if !bytesconv.IsEmpty(version) {
		msg.Header[HeaderSchemaVersion] = []string{version}
	}
	if !bytesconv.IsEmpty(encoding) {
		msg.Header[HeaderContractEncoding] = []string{encoding}
	}
}

// SchemaFromHeader reads schema contract headers from a NATS message.
func SchemaFromHeader(h natspkg.Header) (name, version, encoding string) {
	if h == nil {
		return "", "", ""
	}

	return h.Get(HeaderSchema), h.Get(HeaderSchemaVersion), h.Get(HeaderContractEncoding)
}

// CheckSchema validates msg headers against expect. When expect.Required is false
// and headers are absent, validation succeeds.
func CheckSchema(msg *natspkg.Msg, expect SchemaExpectation) error {
	if msg == nil {
		return fmt.Errorf("check schema: message is nil")
	}
	name, version, encoding := SchemaFromHeader(msg.Header)
	if bytesconv.IsEmpty(name) && bytesconv.IsEmpty(version) && bytesconv.IsEmpty(encoding) {
		if expect.Required {
			return fmt.Errorf("%w: missing schema headers", ErrSchemaMismatch)
		}

		return nil
	}
	if !bytesconv.IsEmpty(expect.Name) && name != expect.Name {
		return fmt.Errorf("%w: schema name want %q got %q", ErrSchemaMismatch, expect.Name, name)
	}
	if !bytesconv.IsEmpty(expect.Version) && version != expect.Version {
		return fmt.Errorf("%w: schema version want %q got %q", ErrSchemaMismatch, expect.Version, version)
	}
	if !bytesconv.IsEmpty(expect.Encoding) && encoding != expect.Encoding {
		return fmt.Errorf("%w: encoding want %q got %q", ErrSchemaMismatch, expect.Encoding, encoding)
	}

	return nil
}

// DecodeMsgWithSchema runs CheckSchema then DecodeMsg.
func DecodeMsgWithSchema(msg *natspkg.Msg, typ MessageType, dst any, expect SchemaExpectation) error {
	if err := CheckSchema(msg, expect); err != nil {
		return err
	}

	return DecodeMsg(msg, typ, dst)
}
