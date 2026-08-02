package nats

import (
	"errors"

	natspkg "github.com/nats-io/nats.go"
)

type MessageType int

const (
	JSON        MessageType = 1
	Proto       MessageType = 2
	MessagePack MessageType = 3
	// Raw publishes Data as []byte without encoding (used by DLQ passthrough).
	Raw MessageType = 4
)

const (
	HeaderContentType     = "Nats-Content-Type"
	HeaderMsgID           = "Nats-Msg-Id"
	HeaderTraceID         = "Trace-Id"
	HeaderContentEncoding = "Content-Encoding"
	ContentTypeJSON       = "json"
	ContentTypeProto      = "protobuf"
	ContentTypeMsgPack    = "msgpack"
)

// PayloadCompressionMode selects publisher-side payload compression.
type PayloadCompressionMode uint8

const (
	// PayloadCompressionOff never compresses (default).
	PayloadCompressionOff PayloadCompressionMode = iota
	// PayloadCompressionAuto compresses payloads strictly larger than
	// MinPayloadCompressBytes with br then gzip when the result shrinks.
	PayloadCompressionAuto
	// PayloadCompressionGzip forces gzip when the payload is above the threshold
	// and the compressed body is smaller.
	PayloadCompressionGzip
	// PayloadCompressionBrotli forces brotli (br) when the payload is above the
	// threshold and the compressed body is smaller.
	PayloadCompressionBrotli
)

type BackpressureMode uint8

const (
	BackpressureBlock BackpressureMode = iota + 1
	BackpressureNak
	BackpressureTerm
	BackpressureDrop
)

// Deliver and replay policies mirror github.com/nats-io/nats.go constants.
type (
	DeliverPolicy   = natspkg.DeliverPolicy
	ReplayPolicy    = natspkg.ReplayPolicy
	AckPolicy       = natspkg.AckPolicy
	RetentionPolicy = natspkg.RetentionPolicy
	StorageType     = natspkg.StorageType
	DiscardPolicy   = natspkg.DiscardPolicy
)

const (
	DeliverAll             = natspkg.DeliverAllPolicy
	DeliverLast            = natspkg.DeliverLastPolicy
	DeliverNew             = natspkg.DeliverNewPolicy
	DeliverByStartSequence = natspkg.DeliverByStartSequencePolicy
	DeliverByStartTime     = natspkg.DeliverByStartTimePolicy
	DeliverLastPerSubject  = natspkg.DeliverLastPerSubjectPolicy

	ReplayInstant  = natspkg.ReplayInstantPolicy
	ReplayOriginal = natspkg.ReplayOriginalPolicy

	AckExplicit = natspkg.AckExplicitPolicy
	AckNone     = natspkg.AckNonePolicy
	AckAll      = natspkg.AckAllPolicy

	LimitsPolicy    = natspkg.LimitsPolicy
	InterestPolicy  = natspkg.InterestPolicy
	WorkQueuePolicy = natspkg.WorkQueuePolicy

	FileStorage   = natspkg.FileStorage
	MemoryStorage = natspkg.MemoryStorage

	DiscardOld = natspkg.DiscardOld
	DiscardNew = natspkg.DiscardNew
)

var (
	ErrInvalidMessageType         = errors.New("invalid message type")
	ErrInvalidTypeAssertion       = errors.New("invalid type assertion")
	ErrPoolFull                   = errors.New("worker pool queue full")
	ErrAsyncPublishPendingLimit   = errors.New("async publish pending limit exceeded")
	ErrUnsupportedContentEncoding = errors.New("unsupported content encoding")
	ErrPayloadTooLarge            = errors.New("decompressed payload exceeds size limit")
)
