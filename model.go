package nats

import (
	"errors"
	"time"
)

type Message struct {
	Data   any
	Header map[string][]string
	// Expect applies optimistic concurrency PubOpts (ExpectedStream / LastSeq / LastMsgId).
	Expect      *PublishExpectation
	MessageType MessageType
}

// StoredMessage is a JetStream stream message loaded by sequence (peek / range export).
type StoredMessage struct {
	Time        time.Time
	Header      map[string][]string
	Subject     string
	Data        []byte
	Sequence    uint64
	MessageType MessageType
}

// ReplayConsumerResult is returned after ResetConsumer / CreateReplayConsumer.
type ReplayConsumerResult struct {
	StartTime *time.Time
	UntilTime *time.Time
	Durable   string
	StartSeq  uint64
	UntilSeq  uint64
	Limit     int
}

// PublishExpectation is optimistic concurrency for JetStream publish.
// Zero LastSeq / LastSeqPerSubject are ignored unless the corresponding Set* helper is used
// (pointers distinguish "unset" from "expect zero").
type PublishExpectation struct {
	LastSeq           *uint64
	LastSeqPerSubject *uint64
	Stream            string
	LastMsgID         string
}

// WithMsgID sets the JetStream deduplication header (requires stream DuplicateWindow).
func (m Message) WithMsgID(id string) Message {
	if m.Header == nil {
		m.Header = make(map[string][]string)
	}

	m.Header[HeaderMsgID] = []string{id}

	return m
}

// WithExpectedStream requires the message to land in the named stream.
func (m Message) WithExpectedStream(stream string) Message {
	if m.Expect == nil {
		m.Expect = &PublishExpectation{}
	}
	m.Expect.Stream = stream

	return m
}

// WithExpectedLastSeq requires the stream's last sequence to equal seq before accept.
func (m Message) WithExpectedLastSeq(seq uint64) Message {
	if m.Expect == nil {
		m.Expect = &PublishExpectation{}
	}
	m.Expect.LastSeq = &seq

	return m
}

// WithExpectedLastSeqPerSubject requires the subject's last sequence to equal seq.
func (m Message) WithExpectedLastSeqPerSubject(seq uint64) Message {
	if m.Expect == nil {
		m.Expect = &PublishExpectation{}
	}
	m.Expect.LastSeqPerSubject = &seq

	return m
}

// WithExpectedLastMsgID requires the stream's last message ID to equal id.
func (m Message) WithExpectedLastMsgID(id string) Message {
	if m.Expect == nil {
		m.Expect = &PublishExpectation{}
	}
	m.Expect.LastMsgID = id

	return m
}

const empty = ""

var (
	ErrEmptySubjectNotAllowed       = errors.New("empty subject not allowed")
	ErrEmptyConfigNotAllowed        = errors.New("empty config not allowed")
	ErrEmptyAddressNotAllowed       = errors.New("empty address not allowed")
	ErrInvalidSubscription          = errors.New("invalid subscription")
	ErrNatsConnectionNotEstablished = errors.New("NATS connection is not established")
	ErrInvalidStreamName            = errors.New("invalid stream name")
	ErrInvalidDurableName           = errors.New("invalid durable name")
	ErrInvalidQueueName             = errors.New("invalid queue name")
	ErrInvalidBucketName            = errors.New("invalid kv bucket name")
	ErrInvalidSubject               = errors.New("invalid subject")
	ErrInvalidKVKey                 = errors.New("invalid kv key")
	ErrBackpressureHandled          = errors.New("message handled by backpressure policy")
	ErrDrainTimeout                 = errors.New("connection drain timed out")
	ErrJetStreamV2Required          = errors.New("operation requires jetstream v2 API")
	ErrInvalidNKeySeed              = errors.New("invalid nkey seed")
	ErrConsumerRecreateRequired     = errors.New("consumer recreate required; delete and recreate explicitly to change immutable settings")
	ErrConflictingAuth              = errors.New(
		"conflicting auth: set only one of Address userinfo, Seed, User/Password, Secret, or CredentialsFile",
	)
	ErrSupervisorGiveUp   = errors.New("subscription supervisor gave up after max retries")
	ErrConsumerStall      = errors.New("consumer soft-liveness stall: pending rising without process activity")
	ErrInvalidReplayBound = errors.New("invalid replay bound")
)

// Consumer metadata keys for intended replay bounds (JetStream has no server-side end).
const (
	MetaReplayUntilSeq = "replay_until_seq"
	MetaReplayLimit    = "replay_limit"
)

// DefaultMsgRangeMax is the default cap for GetMsgRange / GetMsgRangeByTime.
const DefaultMsgRangeMax = 1000
