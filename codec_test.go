package nats

import (
	"testing"

	"github.com/gopherust-io/nats/internal/bytesconv"
	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

func TestCodecJSONRoundTrip(t *testing.T) {
	type payload struct {
		ID string `json:"id"`
	}
	original := payload{ID: "abc"}
	data, err := Encode(Message{Data: original, MessageType: JSON})
	require.NoError(t, err)

	var decoded payload
	require.NoError(t, Decode(data, JSON, &decoded))
	assert.Equal(t, original, decoded)
}

func TestCodecMessagePackRoundTrip(t *testing.T) {
	original := map[string]int{"count": 3}
	data, err := Encode(Message{Data: original, MessageType: MessagePack})
	require.NoError(t, err)

	var decoded map[string]int
	require.NoError(t, Decode(data, MessagePack, &decoded))
	assert.Equal(t, original, decoded)
}

func TestCodecProtoRoundTrip(t *testing.T) {
	original := wrapperspb.String("hello")
	data, err := Encode(Message{Data: original, MessageType: Proto})
	require.NoError(t, err)

	decoded := wrapperspb.String("")
	require.NoError(t, DecodeProto(data, decoded))
	assert.Equal(t, "hello", decoded.GetValue())
}

func TestEncodeRaw(t *testing.T) {
	payload := []byte{0x01, 0x02, 0xff}
	data, err := Encode(Message{Data: payload, MessageType: Raw})
	require.NoError(t, err)
	assert.Equal(t, payload, data)

	_, err = Encode(Message{Data: "not-bytes", MessageType: Raw})
	require.ErrorIs(t, err, ErrInvalidTypeAssertion)
}

func TestEncodeErrors(t *testing.T) {
	_, err := Encode(Message{MessageType: MessageType(0)})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidMessageType)

	_, err = Encode(Message{MessageType: Proto, Data: "bad"})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidTypeAssertion)
}

func TestDecodeErrors(t *testing.T) {
	err := Decode(bytesconv.StringToBytes("{}"), MessageType(0), &struct{}{})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidMessageType)

	err = Decode([]byte{}, Proto, &struct{}{})
	require.Error(t, err)
	require.ErrorIs(t, err, ErrInvalidTypeAssertion)
}

func TestDecodeMsgUsesHeaderType(t *testing.T) {
	type payload struct {
		Name string `json:"name"`
	}
	body, err := Encode(Message{Data: payload{Name: "nats"}, MessageType: JSON})
	require.NoError(t, err)

	msg := &natspkg.Msg{
		Data:   body,
		Header: natspkg.Header{HeaderContentType: []string{ContentTypeJSON}},
	}

	var decoded payload
	require.NoError(t, DecodeMsg(msg, 0, &decoded))
	assert.Equal(t, "nats", decoded.Name)
}

func TestMessageTypeFromHeader(t *testing.T) {
	assert.Equal(t, JSON, MessageTypeFromHeader(nil))
	assert.Equal(t, JSON, MessageTypeFromHeader(natspkg.Header{}))
	assert.Equal(t, Proto, MessageTypeFromHeader(natspkg.Header{
		HeaderContentType: []string{ContentTypeProto},
	}))
	assert.Equal(t, MessagePack, MessageTypeFromHeader(natspkg.Header{
		HeaderContentType: []string{ContentTypeMsgPack},
	}))
	assert.Equal(t, JSON, MessageTypeFromHeader(natspkg.Header{
		HeaderContentType: []string{"unknown"},
	}))
}

func TestValidMessageType(t *testing.T) {
	assert.True(t, validMessageType(JSON))
	assert.True(t, validMessageType(Proto))
	assert.True(t, validMessageType(MessagePack))
	assert.False(t, validMessageType(MessageType(0)))
	assert.False(t, validMessageType(MessageType(99)))
}

func TestNeedsContentTypeHeader(t *testing.T) {
	assert.False(t, needsContentTypeHeader(Message{MessageType: JSON}))
	assert.True(t, needsContentTypeHeader(Message{MessageType: Proto}))
	assert.False(t, needsContentTypeHeader(Message{
		MessageType: JSON,
		Header:      map[string][]string{HeaderContentType: headerValueJSON},
	}))
	assert.True(t, needsContentTypeHeader(Message{
		MessageType: JSON,
		Header:      map[string][]string{"X-Trace": {"1"}},
	}))
	assert.True(t, needsContentTypeHeader(Message{
		MessageType: Proto,
		Header:      map[string][]string{"X-Trace": {"1"}},
	}))
}

func TestApplyContentTypeHeader(t *testing.T) {
	t.Run("proto", func(t *testing.T) {
		msg := Message{MessageType: Proto}
		applyContentTypeHeader(&msg)
		assert.Equal(t, ContentTypeProto, msg.Header[HeaderContentType][0])
	})

	t.Run("msgpack", func(t *testing.T) {
		msg := Message{MessageType: MessagePack}
		applyContentTypeHeader(&msg)
		assert.Equal(t, ContentTypeMsgPack, msg.Header[HeaderContentType][0])
	})

	t.Run("json", func(t *testing.T) {
		msg := Message{MessageType: JSON}
		applyContentTypeHeader(&msg)
		assert.Equal(t, ContentTypeJSON, msg.Header[HeaderContentType][0])
	})

	t.Run("existing header map", func(t *testing.T) {
		msg := Message{
			MessageType: Proto,
			Header:      map[string][]string{"trace": {"1"}},
		}
		applyContentTypeHeader(&msg)
		assert.Equal(t, ContentTypeProto, msg.Header[HeaderContentType][0])
		assert.Equal(t, []string{"1"}, msg.Header["trace"])
	})
}
