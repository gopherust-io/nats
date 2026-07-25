package nats

import (
	"context"
	"errors"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockStreamManager struct {
	err      error
	streamEr error
	msgs     map[uint64]*natspkg.RawStreamMsg
	lastBy   map[string]*natspkg.RawStreamMsg
	lastSeq  uint64
}

func (m *mockStreamManager) CreateOrUpdateStream(context.Context, StreamConfig) (*natspkg.StreamInfo, error) {
	return nil, errors.New("not implemented")
}

func (m *mockStreamManager) AddStream(context.Context, *natspkg.StreamConfig) (*natspkg.StreamInfo, error) {
	return nil, errors.New("not implemented")
}

func (m *mockStreamManager) UpdateStream(context.Context, *natspkg.StreamConfig) (*natspkg.StreamInfo, error) {
	return nil, errors.New("not implemented")
}

func (m *mockStreamManager) DeleteStream(context.Context, string) error {
	return errors.New("not implemented")
}

func (m *mockStreamManager) StreamInfo(context.Context, string) (*natspkg.StreamInfo, error) {
	if m.streamEr != nil {
		return nil, m.streamEr
	}

	return &natspkg.StreamInfo{State: natspkg.StreamState{LastSeq: m.lastSeq}}, nil
}

func (m *mockStreamManager) ListStreamsPage(context.Context, int, int) ([]*natspkg.StreamInfo, int, error) {
	return nil, 0, errors.New("not implemented")
}

func (m *mockStreamManager) PurgeStream(context.Context, string, ...PurgeOpt) error {
	return errors.New("not implemented")
}

func (m *mockStreamManager) GetMsg(_ context.Context, _ string, seq uint64) (*natspkg.RawStreamMsg, error) {
	if m.err != nil {
		return nil, m.err
	}

	msg, ok := m.msgs[seq]
	if !ok {
		return nil, natspkg.ErrMsgNotFound
	}

	return msg, nil
}

func (m *mockStreamManager) GetLastMsg(_ context.Context, _, subject string) (*natspkg.RawStreamMsg, error) {
	if m.err != nil {
		return nil, m.err
	}

	msg, ok := m.lastBy[subject]
	if !ok {
		return nil, natspkg.ErrMsgNotFound
	}

	return msg, nil
}

func (m *mockStreamManager) GetNextMsgAfter(ctx context.Context, stream string, seq uint64) (*natspkg.RawStreamMsg, error) {
	info, err := m.StreamInfo(ctx, stream)
	if err != nil {
		return nil, err
	}

	for next := seq + 1; next <= info.State.LastSeq; next++ {
		msg, getErr := m.GetMsg(ctx, stream, next)
		if getErr == nil {
			return msg, nil
		}

		if !errors.Is(getErr, natspkg.ErrMsgNotFound) {
			return nil, getErr
		}
	}

	return nil, natspkg.ErrMsgNotFound
}

type mockConsumerManager struct {
	createFn func(ctx context.Context, stream string, cfg DurableConsumerConfig) (*natspkg.ConsumerInfo, error)
	deleteFn func(ctx context.Context, stream, durable string) error
	infoFn   func(ctx context.Context, stream, durable string) (*natspkg.ConsumerInfo, error)
}

func (m *mockConsumerManager) CreateOrUpdateConsumer(
	ctx context.Context,
	stream string,
	cfg DurableConsumerConfig,
) (*natspkg.ConsumerInfo, error) {
	if m.createFn != nil {
		return m.createFn(ctx, stream, cfg)
	}

	return &natspkg.ConsumerInfo{Name: cfg.Durable}, nil
}

func (m *mockConsumerManager) AddConsumer(context.Context, string, *natspkg.ConsumerConfig) (*natspkg.ConsumerInfo, error) {
	return nil, errors.New("not implemented")
}

func (m *mockConsumerManager) UpdateConsumer(context.Context, string, *natspkg.ConsumerConfig) (*natspkg.ConsumerInfo, error) {
	return nil, errors.New("not implemented")
}

func (m *mockConsumerManager) DeleteConsumer(ctx context.Context, stream, durable string) error {
	if m.deleteFn != nil {
		return m.deleteFn(ctx, stream, durable)
	}

	return nil
}

func (m *mockConsumerManager) ConsumerInfo(ctx context.Context, stream, durable string) (*natspkg.ConsumerInfo, error) {
	if m.infoFn != nil {
		return m.infoFn(ctx, stream, durable)
	}

	return nil, natspkg.ErrConsumerNotFound
}

func (m *mockConsumerManager) ConsumerNames(context.Context, string) ([]string, error) {
	return nil, errors.New("not implemented")
}

func (m *mockConsumerManager) ListConsumers(context.Context, string) ([]*natspkg.ConsumerInfo, error) {
	return nil, errors.New("not implemented")
}

func (m *mockConsumerManager) ListConsumersPage(context.Context, string, int, int) ([]*natspkg.ConsumerInfo, int, error) {
	return nil, 0, errors.New("not implemented")
}

func (m *mockConsumerManager) PauseConsumer(context.Context, string, string, time.Time) error {
	return errors.New("not implemented")
}

func (m *mockConsumerManager) ResumeConsumer(context.Context, string, string) error {
	return errors.New("not implemented")
}

func TestReplayGetMsgJSONDefault(t *testing.T) {
	r := newReplay(&mockStreamManager{
		msgs: map[uint64]*natspkg.RawStreamMsg{
			1: {Data: []byte(`{"id":"1"}`)},
		},
	}, &mockConsumerManager{})

	msg, err := r.GetMsg(context.Background(), "STREAM", 1)
	require.NoError(t, err)
	assert.Equal(t, JSON, msg.MessageType)
	data, ok := msg.Data.([]byte)
	require.True(t, ok)
	assert.JSONEq(t, `{"id":"1"}`, string(data))
}

func TestReplayGetMsgFromHeader(t *testing.T) {
	r := newReplay(&mockStreamManager{
		msgs: map[uint64]*natspkg.RawStreamMsg{
			1: {
				Data:   []byte{0x01},
				Header: natspkg.Header{HeaderContentType: []string{ContentTypeProto}},
			},
		},
	}, &mockConsumerManager{})

	msg, err := r.GetMsg(context.Background(), "STREAM", 1)
	require.NoError(t, err)
	assert.Equal(t, Proto, msg.MessageType)
}

func TestReplayGetMsgNotFound(t *testing.T) {
	r := newReplay(&mockStreamManager{msgs: map[uint64]*natspkg.RawStreamMsg{}}, &mockConsumerManager{})
	_, err := r.GetMsg(context.Background(), "STREAM", 99)
	require.Error(t, err)
}

func TestReplayGetLastMsgForSubject(t *testing.T) {
	r := newReplay(&mockStreamManager{
		lastBy: map[string]*natspkg.RawStreamMsg{
			"orders.created": {Data: []byte(`{"ok":true}`)},
		},
	}, &mockConsumerManager{})

	msg, err := r.GetLastMsgForSubject(context.Background(), "STREAM", "orders.created")
	require.NoError(t, err)
	data, ok := msg.Data.([]byte)
	require.True(t, ok)
	assert.JSONEq(t, `{"ok":true}`, string(data))
}

func TestReplayGetNextMsgAfterSkipsGaps(t *testing.T) {
	r := newReplay(&mockStreamManager{
		lastSeq: 5,
		msgs: map[uint64]*natspkg.RawStreamMsg{
			4: {Data: []byte(`{"n":4}`)},
		},
	}, &mockConsumerManager{})

	msg, err := r.GetNextMsgAfter(context.Background(), "STREAM", 2)
	require.NoError(t, err)
	data, ok := msg.Data.([]byte)
	require.True(t, ok)
	assert.JSONEq(t, `{"n":4}`, string(data))
}

func TestReplayResetConsumerPreservesExistingConfig(t *testing.T) {
	var created DurableConsumerConfig
	r := newReplay(&mockStreamManager{}, &mockConsumerManager{
		infoFn: func(context.Context, string, string) (*natspkg.ConsumerInfo, error) {
			return &natspkg.ConsumerInfo{
				Config: natspkg.ConsumerConfig{
					Durable:       "replay-durable",
					FilterSubject: "orders.>",
					AckPolicy:     AckExplicit,
					MaxAckPending: 777,
					AckWait:       45 * time.Second,
					MaxDeliver:    5,
					DeliverPolicy: DeliverAll,
				},
			}, nil
		},
		createFn: func(_ context.Context, _ string, cfg DurableConsumerConfig) (*natspkg.ConsumerInfo, error) {
			created = cfg
			return &natspkg.ConsumerInfo{Name: cfg.Durable}, nil
		},
	})

	err := r.ResetConsumer(context.Background(), "ORDERS", "replay-durable",
		FromSeq(100), WithReplayPolicy(ReplayInstant))
	require.NoError(t, err)
	assert.Equal(t, 777, created.MaxAckPending)
	assert.Equal(t, 45*time.Second, created.AckWait)
	assert.Equal(t, 5, created.MaxDeliver)
	assert.Equal(t, "orders.>", created.FilterSubject)
	assert.Equal(t, DeliverByStartSequence, created.DeliverPolicy)
	assert.Equal(t, uint64(100), created.OptStartSeq)
	assert.Equal(t, ReplayInstant, created.ReplayPolicy)
}

func TestReplayResetConsumerWhenMissing(t *testing.T) {
	var created DurableConsumerConfig
	r := newReplay(&mockStreamManager{}, &mockConsumerManager{
		infoFn: func(context.Context, string, string) (*natspkg.ConsumerInfo, error) {
			return nil, natspkg.ErrConsumerNotFound
		},
		createFn: func(_ context.Context, _ string, cfg DurableConsumerConfig) (*natspkg.ConsumerInfo, error) {
			created = cfg
			return &natspkg.ConsumerInfo{Name: cfg.Durable}, nil
		},
	})

	err := r.ResetConsumer(context.Background(), "ORDERS", "missing",
		WithFilterSubject("orders.>"), FromNew())
	require.NoError(t, err)
	assert.Equal(t, "missing", created.Durable)
	assert.Equal(t, "orders.>", created.FilterSubject)
	assert.Equal(t, AckExplicit, created.AckPolicy)
}

func TestReplayResetConsumerPropagatesInfoError(t *testing.T) {
	r := newReplay(&mockStreamManager{}, &mockConsumerManager{
		infoFn: func(context.Context, string, string) (*natspkg.ConsumerInfo, error) {
			return nil, assert.AnError
		},
	})

	err := r.ResetConsumer(context.Background(), "ORDERS", "replay-durable")
	require.Error(t, err)
	assert.ErrorIs(t, err, assert.AnError)
}

func TestReplayCreateReplayConsumerSideCar(t *testing.T) {
	var created DurableConsumerConfig
	var deleted bool
	r := newReplay(&mockStreamManager{}, &mockConsumerManager{
		infoFn: func(_ context.Context, _, durable string) (*natspkg.ConsumerInfo, error) {
			if durable != "orders-processor" {
				return nil, natspkg.ErrConsumerNotFound
			}
			return &natspkg.ConsumerInfo{
				Config: natspkg.ConsumerConfig{
					Durable:       "orders-processor",
					FilterSubject: "orders.>",
					MaxAckPending: 1000,
					AckPolicy:     AckExplicit,
				},
			}, nil
		},
		deleteFn: func(context.Context, string, string) error {
			deleted = true
			return nil
		},
		createFn: func(_ context.Context, _ string, cfg DurableConsumerConfig) (*natspkg.ConsumerInfo, error) {
			created = cfg
			return &natspkg.ConsumerInfo{Name: cfg.Durable}, nil
		},
	})

	name, err := r.CreateReplayConsumer(context.Background(), "ORDERS", "orders-processor",
		WithReplayDurable("orders-processor-replay"), FromSeq(50))
	require.NoError(t, err)
	assert.Equal(t, "orders-processor-replay", name)
	assert.Equal(t, 1000, created.MaxAckPending)
	assert.Equal(t, "orders.>", created.FilterSubject)
	assert.False(t, deleted, "source durable must not be deleted")
}

func TestReplayCreateReplayConsumerRejectsSameName(t *testing.T) {
	r := newReplay(&mockStreamManager{}, &mockConsumerManager{
		infoFn: func(context.Context, string, string) (*natspkg.ConsumerInfo, error) {
			return &natspkg.ConsumerInfo{Config: natspkg.ConsumerConfig{Durable: "same"}}, nil
		},
	})

	_, err := r.CreateReplayConsumer(context.Background(), "ORDERS", "same", WithReplayDurable("same"))
	require.Error(t, err)
}

func TestApplyReplayOpts(t *testing.T) {
	cfg := applyReplayOpts([]ReplayOpt{FromSeq(42), WithReplayPolicy(ReplayOriginal)})
	assert.Equal(t, DeliverByStartSequence, cfg.DeliverPolicy)
	assert.Equal(t, uint64(42), cfg.OptStartSeq)
	assert.Equal(t, ReplayOriginal, cfg.ReplayPolicy)

	start := time.Date(2024, 1, 2, 3, 4, 5, 0, time.UTC)
	cfg = applyReplayOpts([]ReplayOpt{FromTime(start), WithFilterSubject("orders.>")})
	assert.Equal(t, DeliverByStartTime, cfg.DeliverPolicy)
	require.NotNil(t, cfg.OptStartTime)
	assert.True(t, cfg.OptStartTime.Equal(start))
	assert.Equal(t, "orders.>", cfg.FilterSubject)
	assert.Zero(t, cfg.OptStartSeq)

	cfg = applyReplayOpts([]ReplayOpt{FromBeginning()})
	assert.Equal(t, DeliverAll, cfg.DeliverPolicy)

	cfg = applyReplayOpts([]ReplayOpt{FromNew(), WithFilterSubjects("a.>", "b.>")})
	assert.Equal(t, DeliverNew, cfg.DeliverPolicy)
	assert.Equal(t, []string{"a.>", "b.>"}, cfg.FilterSubjects)
	assert.Empty(t, cfg.FilterSubject)
}

func TestNewClientInvalidNKeySeed(t *testing.T) {
	cfg := DefaultConfig()
	cfg.Conn.Address = "nats://127.0.0.1:4222"
	cfg.Conn.Seed = "not-a-valid-seed"
	cfg.Conn.InitialRetryAttempts = 0
	disableTelemetry(&cfg)

	_, err := NewClient(context.Background(), &cfg)
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrInvalidNKeySeed)
}
