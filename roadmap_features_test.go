package nats

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPullMultiFilterNoCommonPrefix(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "MFILTER")
	prefix := streamSubjectPrefix(stream)
	subjectA := prefix + "orders"
	subjectB := prefix + "payments"
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1,
	})
	require.NoError(t, err)

	durable := uniqueDurable(t, "mfilter-pull")
	_, err = client.Consumers().CreateOrUpdateConsumer(ctx, stream, DurableConsumerConfig{
		Durable:        durable,
		FilterSubjects: []string{subjectA, subjectB},
	})
	require.NoError(t, err)

	require.NoError(t, client.Publisher().PublishJSON(ctx, subjectB, map[string]string{"k": "b"}))

	pull, err := client.Consumer().Pull(stream, durable)
	require.NoError(t, err)
	defer pull.Close()

	msgs, err := pull.Fetch(ctx, 1, WithFetchMaxWait(500*time.Millisecond))
	require.NoError(t, err)
	require.Len(t, msgs, 1)
	assert.Equal(t, subjectB, msgs[0].Subject)
}

func TestSetupWorkerFilterSubjects(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "SETUPMF")
	prefix := streamSubjectPrefix(stream)
	subjectA := prefix + "a"
	subjectB := prefix + "b"
	durable := uniqueDurable(t, "setup-mf")
	received := make(chan string, 2)

	_, err := client.SetupWorker(ctx, WorkerSetup{
		Stream: StreamConfig{Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1},
		Consumer: DurableConsumerConfig{
			Durable:        durable,
			FilterSubjects: []string{subjectA, subjectB},
		},
		Queue: uniqueQueue(t, "setup-mf-q"),
	}, func(_ context.Context, msg *natspkg.Msg) error {
		received <- msg.Subject
		return nil
	})
	require.NoError(t, err)

	info, err := client.Consumers().ConsumerInfo(ctx, stream, durable)
	require.NoError(t, err)
	assert.Equal(t, []string{subjectA, subjectB}, info.Config.FilterSubjects)

	require.NoError(t, client.Publisher().PublishJSON(ctx, subjectB, map[string]string{"id": "1"}))
	select {
	case got := <-received:
		assert.Equal(t, subjectB, got)
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for multi-filter setup worker")
	}
}

func TestOrderedSubscribe(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "ORDERED")
	prefix := streamSubjectPrefix(stream)
	subject := prefix + "events"
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1,
	})
	require.NoError(t, err)

	got := make(chan struct{}, 1)
	sub, err := client.OrderedSubscribe(ctx, stream, OrderedConsumerConfig{
		FilterSubject: subject,
	}, func(_ context.Context, msg *natspkg.Msg) error {
		got <- struct{}{}
		return nil
	})
	require.NoError(t, err)
	defer sub.Stop()

	require.NoError(t, client.Publisher().PublishJSON(ctx, subject, map[string]string{"ok": "1"}))
	select {
	case <-got:
	case <-time.After(2 * time.Second):
		t.Fatal("timeout waiting for ordered message")
	}
}

func TestSchemaHelpers(t *testing.T) {
	t.Parallel()
	m := Message{Data: map[string]string{"a": "b"}, MessageType: JSON}
	ApplySchemaHeaders(&m, "orders.v1", "1", "json")
	require.Equal(t, "orders.v1", m.Header[HeaderSchema][0])

	msg := &natspkg.Msg{Header: natspkg.Header{}}
	msg.Header.Set(HeaderSchema, "orders.v1")
	msg.Header.Set(HeaderSchemaVersion, "1")
	require.NoError(t, CheckSchema(msg, SchemaExpectation{Name: "orders.v1", Version: "1"}))
	require.ErrorIs(t, CheckSchema(msg, SchemaExpectation{Name: "other", Required: true}), ErrSchemaMismatch)
}

func TestCircuitBreaker(t *testing.T) {
	t.Parallel()
	b := NewCircuitBreaker(CircuitConfig{Enabled: true, OpenOnLevel: PressureCritical})
	require.NotNil(t, b)
	assert.False(t, b.ShouldPause(AdaptivePressureDecision{Level: PressureWarm}))
	assert.True(t, b.ShouldPause(AdaptivePressureDecision{Level: PressureCritical}))
	assert.Equal(t, int64(1), b.Opens())
	assert.True(t, b.IsOpen())
	assert.Equal(t, time.Second, b.PauseDuration())
	assert.Nil(t, NewCircuitBreaker(CircuitConfig{}))
	assert.Equal(t, time.Duration(0), (*CircuitBreaker)(nil).PauseDuration())
	assert.Equal(t, int64(0), (*CircuitBreaker)(nil).Opens())
}

func TestSchemaHelpersExtended(t *testing.T) {
	t.Parallel()
	name, ver, enc := SchemaFromHeader(nil)
	assert.Equal(t, "", name)
	assert.Equal(t, "", ver)
	assert.Equal(t, "", enc)

	require.Error(t, CheckSchema(nil, SchemaExpectation{Required: true}))
	require.NoError(t, CheckSchema(&natspkg.Msg{}, SchemaExpectation{}))
	require.ErrorIs(t, CheckSchema(&natspkg.Msg{}, SchemaExpectation{Required: true}), ErrSchemaMismatch)

	msg := &natspkg.Msg{
		Header: natspkg.Header{
			HeaderSchema:           []string{"orders.v1"},
			HeaderSchemaVersion:    []string{"2"},
			HeaderContractEncoding: []string{"json"},
		},
		Data: []byte(`{"x":1}`),
	}
	require.ErrorIs(t, CheckSchema(msg, SchemaExpectation{Version: "1"}), ErrSchemaMismatch)
	require.ErrorIs(t, CheckSchema(msg, SchemaExpectation{Encoding: "msgpack"}), ErrSchemaMismatch)

	var dst map[string]int
	require.NoError(t, DecodeMsgWithSchema(msg, JSON, &dst, SchemaExpectation{Name: "orders.v1"}))
	assert.Equal(t, 1, dst["x"])
	require.ErrorIs(t, DecodeMsgWithSchema(msg, JSON, &dst, SchemaExpectation{Name: "other"}), ErrSchemaMismatch)
}

func TestDelayedSubjectAndPublish(t *testing.T) {
	t.Parallel()
	dest, err := delayedSubject("orders.created", 1500*time.Millisecond, SchedulePublishConfig{})
	require.NoError(t, err)
	assert.Equal(t, "orders.created.delay.1500", dest)

	dest, err = delayedSubject("orders.created", time.Second, SchedulePublishConfig{SubjectTemplate: "%s@%d"})
	require.NoError(t, err)
	assert.Equal(t, "orders.created@1000", dest)

	_, err = delayedSubject("", time.Second, SchedulePublishConfig{})
	require.Error(t, err)
	_, err = delayedSubject("orders.created", -time.Second, SchedulePublishConfig{})
	require.Error(t, err)

	cli, ctx := testClient(t)
	stream := uniqueStream(t, "DELAY")
	prefix := streamSubjectPrefix(stream)
	_, err = cli.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1,
	})
	require.NoError(t, err)
	rawClient, ok := cli.(*client)
	require.True(t, ok)
	require.NoError(t, rawClient.PublishDelayedJSON(
		ctx, prefix+"job", 250*time.Millisecond, map[string]string{"id": "1"}, SchedulePublishConfig{},
	))
}

func TestAttachToHandlerDisabledAndNil(t *testing.T) {
	t.Parallel()
	client, _ := testClient(t)
	inc := client.Incidents()
	base := func(context.Context, *natspkg.Msg) error { return assert.AnError }
	wrapped := AttachIncidentsHandler(inc, "S", "D", CapsuleAutoConfig{Enabled: false}, base)
	require.ErrorIs(t, wrapped(context.Background(), &natspkg.Msg{Subject: "x"}), assert.AnError)

	passthru := AttachIncidentsHandler(nil, "S", "D", CapsuleAutoConfig{Enabled: true}, base)
	require.ErrorIs(t, passthru(context.Background(), &natspkg.Msg{}), assert.AnError)
	assert.Nil(t, AttachIncidentsHandler(inc, "S", "D", CapsuleAutoConfig{Enabled: true}, nil))

	impl, ok := inc.(*incidents)
	require.True(t, ok)
	assert.Nil(t, impl.AttachToHandler("S", "D", CapsuleAutoConfig{Enabled: true}, nil))
	passthrough := impl.AttachToHandler("S", "D", CapsuleAutoConfig{Enabled: false}, base)
	require.ErrorIs(t, passthrough(context.Background(), &natspkg.Msg{}), assert.AnError)
}

func TestAttachToHandlerCapturesOnError(t *testing.T) {
	t.Parallel()
	cli, ctx := testClient(t)
	stream := uniqueStream(t, "ATTACH")
	prefix := streamSubjectPrefix(stream)
	subject := prefix + "evt"
	_, err := cli.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage, Replicas: 1,
	})
	require.NoError(t, err)
	require.NoError(t, cli.Publisher().PublishJSON(ctx, subject, map[string]string{"k": "v"}))

	ready := make(chan *Capsule, 1)
	store := uniqueName(t, "attstore")
	index := uniqueName(t, "attidx")
	handler := cli.Incidents().(*incidents).AttachToHandler(stream, "worker", CapsuleAutoConfig{
		Enabled:     true,
		StoreBucket: store,
		IndexBucket: index,
		Window:      5,
		OnReady:     func(c *Capsule) { ready <- c },
	}, func(context.Context, *natspkg.Msg) error {
		return assert.AnError
	})
	require.ErrorIs(t, handler(ctx, &natspkg.Msg{Subject: subject, Data: []byte(`{"k":"v"}`)}), assert.AnError)
	select {
	case capsule := <-ready:
		require.NotEmpty(t, capsule.ID)
	case <-time.After(3 * time.Second):
		t.Fatal("expected OnReady capsule")
	}
}

func TestCtxUtilAndOrderedOpts(t *testing.T) {
	t.Parallel()
	// Intentionally exercise nil-ctx fast paths used by CRUD helpers.
	require.NoError(t, ctxReady(nil))                                         //nolint:staticcheck // SA1012: nil ctx is a supported fast path
	require.NoError(t, runWithContext(nil, func() error { return nil }))      //nolint:staticcheck // SA1012
	v, err := runWithContextValue(nil, func() (int, error) { return 7, nil }) //nolint:staticcheck // SA1012
	require.NoError(t, err)
	assert.Equal(t, 7, v)

	cli, ctx := testClient(t)
	stream := uniqueStream(t, "ORDCFG")
	prefix := streamSubjectPrefix(stream)
	_, err = cli.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Replicas: 1,
	})
	require.NoError(t, err)
	sub, err := cli.OrderedSubscribe(ctx, stream, OrderedConsumerConfig{
		FilterSubjects:   []string{prefix + "a", prefix + "b"},
		HasDeliverPolicy: true,
		DeliverPolicy:    DeliverAll,
		HasReplayPolicy:  true,
		ReplayPolicy:     ReplayInstant,
		HeadersOnly:      true,
		NamePrefix:       "ord",
	}, func(context.Context, *natspkg.Msg) error { return nil })
	require.NoError(t, err)
	sub.Stop()
}

func TestPullV2ProcessConcurrency(t *testing.T) {
	t.Parallel()
	cli, base := testClient(t)
	stream := uniqueStream(t, "PCONC")
	prefix := streamSubjectPrefix(stream)
	durable := uniqueDurable(t, "pconc")
	_, err := cli.Streams().CreateOrUpdateStream(base, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage, Replicas: 1,
	})
	require.NoError(t, err)
	_, err = cli.Consumers().CreateOrUpdateConsumer(base, stream, DurableConsumerConfig{
		Durable: durable, FilterSubject: prefix + ">",
	})
	require.NoError(t, err)
	require.NoError(t, cli.Publisher().PublishJSON(base, prefix+"m", map[string]int{"n": 1}))

	pull, err := cli.Consumer().Pull(stream, durable)
	require.NoError(t, err)
	defer pull.Close()

	ctx, cancel := context.WithTimeout(base, 2*time.Second)
	defer cancel()
	var handled atomic.Int32
	err = pull.Process(ctx, func(context.Context, *natspkg.Msg) error {
		handled.Add(1)
		cancel()
		return nil
	}, WithFetchBatch(1), WithProcessMaxWait(500*time.Millisecond), WithProcessConcurrency(2))
	require.Error(t, err)
	assert.GreaterOrEqual(t, handled.Load(), int32(1))
}

func TestPullV2FetchNoWaitAndOrderedStopped(t *testing.T) {
	t.Parallel()
	client, ctx := testClient(t)
	stream := uniqueStream(t, "NOWAIT")
	prefix := streamSubjectPrefix(stream)
	durable := uniqueDurable(t, "nowait")
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage, Replicas: 1,
	})
	require.NoError(t, err)
	_, err = client.Consumers().CreateOrUpdateConsumer(ctx, stream, DurableConsumerConfig{
		Durable: durable, FilterSubject: prefix + ">",
	})
	require.NoError(t, err)

	pull, err := client.Consumer().Pull(stream, durable)
	require.NoError(t, err)
	defer pull.Close()
	msgs, err := pull.FetchNoWait(1)
	require.NoError(t, err)
	assert.Empty(t, msgs)

	sub, err := client.OrderedSubscribe(ctx, stream, OrderedConsumerConfig{FilterSubject: prefix + "evt"},
		func(context.Context, *natspkg.Msg) error { return nil })
	require.NoError(t, err)
	stopped := sub.Stopped()
	sub.Stop()
	select {
	case <-stopped:
	case <-time.After(2 * time.Second):
		t.Fatal("ordered subscription Stopped channel not closed")
	}
}

func TestCRUDContextCanceled(t *testing.T) {
	t.Parallel()
	client, _ := testClient(t)
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := client.Streams().CreateOrUpdateStream(ctx, StreamConfig{Name: "X", Subjects: []string{"x.>"}})
	require.ErrorIs(t, err, context.Canceled)
}

func TestCoverageHelpers(t *testing.T) {
	t.Parallel()
	assert.Equal(t, "manual", TriggerManual.String())
	assert.Equal(t, "dlq", TriggerDLQ.String())
	ApplySchemaHeaders(nil, "n", "v", "e")

	msg := Message{}.WithExpectedLastSeq(1).WithExpectedLastSeqPerSubject(2).WithExpectedLastMsgID("mid")
	require.NotNil(t, msg.Expect)
	require.NotNil(t, msg.Expect.LastSeq)
	assert.Equal(t, uint64(1), *msg.Expect.LastSeq)
	require.NotNil(t, msg.Expect.LastSeqPerSubject)
	assert.Equal(t, uint64(2), *msg.Expect.LastSeqPerSubject)
	assert.Equal(t, "mid", msg.Expect.LastMsgID)

	require.Error(t, InProgress(nil))
	require.Error(t, NakWithDelay(nil, time.Second))
	require.Error(t, TermWithReason(nil, "x"))
	require.Error(t, TermWithReason(&natspkg.Msg{Subject: "s"}, ""))
	require.Error(t, TermWithReason(&natspkg.Msg{Subject: "s"}, "poison"))

	assert.True(t, isFetchIdleErr(natspkg.ErrTimeout))
	assert.True(t, isFetchIdleErr(errors.New("no messages")))
	assert.False(t, isFetchIdleErr(assert.AnError))
	assert.False(t, isFetchIdleErr(nil))
	assert.Nil(t, jetstreamMsgToNATS(nil))
}

func TestLegacyPullEmptyDurable(t *testing.T) {
	t.Parallel()
	cli, ctx := testClient(t)
	stream := uniqueStream(t, "LEGACY")
	prefix := streamSubjectPrefix(stream)
	_, err := cli.Streams().CreateOrUpdateStream(ctx, StreamConfig{
		Name: stream, Subjects: []string{prefix + ">"}, Storage: MemoryStorage, Replicas: 1,
	})
	require.NoError(t, err)
	require.NoError(t, cli.Publisher().PublishJSON(ctx, prefix+"m", map[string]string{"a": "1"}))

	pull, err := cli.Consumer().Pull(stream, "")
	require.NoError(t, err)
	defer pull.Close()

	msgs, err := pull.Fetch(ctx, 1, WithFetchMaxWait(2*time.Second))
	require.NoError(t, err)
	require.NotEmpty(t, msgs)
	_ = msgs[0].Ack()

	_, err = pull.FetchNoWait(1)
	if err != nil {
		assert.True(t, isFetchIdleErr(err), "got %v", err)
	}
}
