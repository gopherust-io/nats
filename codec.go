package nats

import (
	"bytes"
	"fmt"
	"sync"

	"github.com/bytedance/sonic"
	natspkg "github.com/nats-io/nats.go"
	"github.com/vmihailenco/msgpack/v5"
	"google.golang.org/protobuf/proto"
)

var (
	headerValueJSON    = []string{ContentTypeJSON}
	headerValueProto   = []string{ContentTypeProto}
	headerValueMsgPack = []string{ContentTypeMsgPack}
)

type encodeScratch struct {
	buf bytes.Buffer
}

var encodeScratchPool = sync.Pool{
	New: func() any {
		return &encodeScratch{}
	},
}

func Encode(msg Message) ([]byte, error) {
	switch msg.MessageType {
	case JSON:
		return encodeJSONPooled(msg.Data)
	case Proto:
		protoMsg, ok := msg.Data.(proto.Message)
		if !ok {
			return nil, fmt.Errorf("encode proto: %w", ErrInvalidTypeAssertion)
		}

		return proto.Marshal(protoMsg)
	case MessagePack:
		return encodeMsgPackPooled(msg.Data)
	case Raw:
		raw, ok := msg.Data.([]byte)
		if !ok {
			return nil, fmt.Errorf("encode raw: %w", ErrInvalidTypeAssertion)
		}
		// Copy so async publish cannot race caller buffer reuse.
		out := make([]byte, len(raw))
		copy(out, raw)

		return out, nil
	default:
		return nil, fmt.Errorf("encode: %w", ErrInvalidMessageType)
	}
}

func encodeJSONPooled(data any) ([]byte, error) {
	scratch := encodeScratchPool.Get().(*encodeScratch)
	scratch.buf.Reset()
	err := sonic.ConfigDefault.NewEncoder(&scratch.buf).Encode(data)
	if err != nil {
		encodeScratchPool.Put(scratch)

		return nil, err
	}
	// sonic Encoder appends a trailing newline; trim for wire compatibility with Marshal.
	b := scratch.buf.Bytes()
	if n := len(b); n > 0 && b[n-1] == '\n' {
		b = b[:n-1]
	}
	out := make([]byte, len(b))
	copy(out, b)
	encodeScratchPool.Put(scratch)

	return out, nil
}

func encodeMsgPackPooled(data any) ([]byte, error) {
	scratch := encodeScratchPool.Get().(*encodeScratch)
	scratch.buf.Reset()
	enc := msgpack.NewEncoder(&scratch.buf)
	err := enc.Encode(data)
	if err != nil {
		encodeScratchPool.Put(scratch)

		return nil, err
	}
	b := scratch.buf.Bytes()
	out := make([]byte, len(b))
	copy(out, b)
	encodeScratchPool.Put(scratch)

	return out, nil
}

func Decode(data []byte, typ MessageType, dst any) error {
	switch typ {
	case JSON:
		return sonic.Unmarshal(data, dst)
	case Proto:
		protoMsg, ok := dst.(proto.Message)
		if !ok {
			return fmt.Errorf("decode proto: %w", ErrInvalidTypeAssertion)
		}

		return proto.Unmarshal(data, protoMsg)
	case MessagePack:
		return msgpack.Unmarshal(data, dst)
	default:
		return fmt.Errorf("decode: %w", ErrInvalidMessageType)
	}
}

func DecodeProto(data []byte, msg proto.Message) error {
	return proto.Unmarshal(data, msg)
}

// DecodeMsg decodes msg into dst. Content-Encoding is expanded only when
// decompress is desired by the caller via DecodeMsgWithDecompress, or when
// the consumer already expanded the payload (header stripped). Prefer
// processMessage PayloadDecompression for the hot path; this helper does not
// auto-decompress so it cannot bypass RuntimeConsumerConfig.PayloadDecompression.
func DecodeMsg(msg *natspkg.Msg, typ MessageType, dst any) error {
	if typ == 0 {
		typ = MessageTypeFromHeader(msg.Header)
	}

	return Decode(msg.Data, typ, dst)
}

// DecodeMsgWithDecompress expands Content-Encoding (size-capped) then decodes.
func DecodeMsgWithDecompress(msg *natspkg.Msg, typ MessageType, dst any) error {
	if err := maybeDecompressMsg(msg); err != nil {
		return err
	}

	return DecodeMsg(msg, typ, dst)
}

func MessageTypeFromHeader(h natspkg.Header) MessageType {
	if h == nil {
		return JSON
	}

	switch h.Get(HeaderContentType) {
	case ContentTypeProto:
		return Proto
	case ContentTypeMsgPack:
		return MessagePack
	default:
		return JSON
	}
}

func validMessageType(typ MessageType) bool {
	switch typ {
	case JSON, Proto, MessagePack, Raw:
		return true
	default:
		return false
	}
}

func applyContentTypeHeader(msg *Message) {
	if msg.Header == nil {
		msg.Header = make(map[string][]string, 1)
	}

	switch msg.MessageType {
	case Proto:
		msg.Header[HeaderContentType] = headerValueProto
	case MessagePack:
		msg.Header[HeaderContentType] = headerValueMsgPack
	case Raw:
		// Preserve caller headers; do not invent a content type.
	case JSON:
		msg.Header[HeaderContentType] = headerValueJSON
	default:
		msg.Header[HeaderContentType] = headerValueJSON
	}
}

func needsContentTypeHeader(msg Message) bool {
	if msg.MessageType == Raw {
		return false
	}

	if msg.MessageType == JSON || msg.MessageType == 0 {
		if len(msg.Header) == 0 {
			return false
		}

		_, ok := msg.Header[HeaderContentType]

		return !ok
	}

	if len(msg.Header) == 0 {
		return true
	}

	_, ok := msg.Header[HeaderContentType]

	return !ok
}
