package nats

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

type ConsumerManager interface {
	CreateOrUpdateConsumer(ctx context.Context, stream string, cfg DurableConsumerConfig) (*natspkg.ConsumerInfo, error)
	AddConsumer(ctx context.Context, stream string, cfg *natspkg.ConsumerConfig) (*natspkg.ConsumerInfo, error)
	UpdateConsumer(ctx context.Context, stream string, cfg *natspkg.ConsumerConfig) (*natspkg.ConsumerInfo, error)
	DeleteConsumer(ctx context.Context, stream, durable string) error
	ConsumerInfo(ctx context.Context, stream, durable string) (*natspkg.ConsumerInfo, error)
	ConsumerNames(ctx context.Context, stream string) ([]string, error)
	ListConsumers(ctx context.Context, stream string) ([]*natspkg.ConsumerInfo, error)
	ListConsumersPage(ctx context.Context, stream string, offset, limit int) ([]*natspkg.ConsumerInfo, int, error)
	PauseConsumer(ctx context.Context, stream, durable string, pauseUntil time.Time) error
	ResumeConsumer(ctx context.Context, stream, durable string) error
}

type consumerManager struct {
	js    natspkg.JetStreamContext
	jsNew jetstream.JetStream
}

func newConsumerManager(conn *natspkg.Conn, js natspkg.JetStreamContext) ConsumerManager {
	mgr := &consumerManager{js: js}
	if jsNew, err := jetstream.New(conn); err == nil {
		mgr.jsNew = jsNew
	}

	return mgr
}

func toNatsConsumerConfig(cfg DurableConsumerConfig) *natspkg.ConsumerConfig {
	cc := &natspkg.ConsumerConfig{
		Durable:           cfg.Durable,
		FilterSubject:     cfg.FilterSubject,
		FilterSubjects:    cfg.FilterSubjects,
		DeliverSubject:    cfg.DeliverSubject,
		DeliverGroup:      cfg.DeliverGroup,
		ReplayPolicy:      cfg.ReplayPolicy,
		AckPolicy:         cfg.AckPolicy,
		MaxDeliver:        cfg.MaxDeliver,
		AckWait:           cfg.AckWait,
		MaxAckPending:     cfg.MaxAckPending,
		RateLimit:         cfg.RateLimit,
		Heartbeat:         cfg.Heartbeat,
		InactiveThreshold: cfg.InactiveThreshold,
		FlowControl:       cfg.FlowControl,
		Replicas:          cfg.Replicas,
		MemoryStorage:     cfg.MemStorage,
		MaxWaiting:        cfg.MaxWaiting,
		OptStartSeq:       cfg.OptStartSeq,
		OptStartTime:      cfg.OptStartTime,
	}
	if len(cfg.Metadata) > 0 {
		cc.Metadata = make(map[string]string, len(cfg.Metadata))
		for k, v := range cfg.Metadata {
			cc.Metadata[k] = v
		}
	}

	// DeliverAll is zero; only copy when explicitly set so updates can omit it.
	if cfg.HasDeliverPolicy {
		cc.DeliverPolicy = cfg.DeliverPolicy
	}

	if !cfg.HasAckPolicy && cc.AckPolicy == 0 {
		cc.AckPolicy = AckExplicit
	}

	if !cfg.HasReplayPolicy && cc.ReplayPolicy == 0 {
		cc.ReplayPolicy = ReplayInstant
	}

	return cc
}

func (m *consumerManager) CreateOrUpdateConsumer(
	_ context.Context,
	stream string,
	cfg DurableConsumerConfig,
) (*natspkg.ConsumerInfo, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, err
	}

	if err := ValidateDurableName(cfg.Durable); err != nil {
		return nil, err
	}

	if !bytesconv.IsEmpty(cfg.FilterSubject) {
		if err := ValidateSubject(cfg.FilterSubject); err != nil {
			return nil, err
		}
	}

	if err := ValidateSubjects(cfg.FilterSubjects); err != nil {
		return nil, err
	}

	cc := toNatsConsumerConfig(cfg)

	info, err := m.js.AddConsumer(stream, cc)
	if err == nil {
		return info, nil
	}

	existing, infoErr := m.js.ConsumerInfo(stream, cfg.Durable)
	if infoErr != nil {
		return nil, fmt.Errorf("create consumer stream=%q durable=%q: %w", stream, cfg.Durable, err)
	}

	// Omitted deliver policy keeps the existing value (DeliverAll is a valid zero).
	if !cfg.HasDeliverPolicy {
		cc.DeliverPolicy = existing.Config.DeliverPolicy
	}

	if existing.Config.DeliverPolicy != cc.DeliverPolicy {
		return nil, fmt.Errorf(
			"create or update consumer stream=%q durable=%q: deliver policy %v -> %v: %w",
			stream, cfg.Durable, existing.Config.DeliverPolicy, cc.DeliverPolicy, ErrConsumerRecreateRequired)
	}

	info, err = m.js.UpdateConsumer(stream, cc)
	if err != nil {
		return nil, fmt.Errorf("update consumer stream=%q durable=%q: %w", stream, cfg.Durable, err)
	}

	return info, nil
}

func (m *consumerManager) AddConsumer(_ context.Context, stream string, cfg *natspkg.ConsumerConfig) (*natspkg.ConsumerInfo, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, err
	}

	if cfg == nil {
		return nil, fmt.Errorf("add consumer stream=%q: %w", stream, ErrEmptyConfigNotAllowed)
	}

	if !bytesconv.IsEmpty(cfg.Durable) {
		if err := ValidateDurableName(cfg.Durable); err != nil {
			return nil, err
		}
	}
	if !bytesconv.IsEmpty(cfg.FilterSubject) {
		if err := ValidateSubject(cfg.FilterSubject); err != nil {
			return nil, err
		}
	}
	if err := ValidateSubjects(cfg.FilterSubjects); err != nil {
		return nil, err
	}

	info, err := m.js.AddConsumer(stream, cfg)
	if err != nil {
		return nil, fmt.Errorf("add consumer stream=%q durable=%q: %w", stream, cfg.Durable, err)
	}

	return info, nil
}

func (m *consumerManager) UpdateConsumer(_ context.Context, stream string, cfg *natspkg.ConsumerConfig) (*natspkg.ConsumerInfo, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, err
	}

	if cfg == nil {
		return nil, fmt.Errorf("update consumer stream=%q: %w", stream, ErrEmptyConfigNotAllowed)
	}

	if !bytesconv.IsEmpty(cfg.Durable) {
		if err := ValidateDurableName(cfg.Durable); err != nil {
			return nil, err
		}
	}

	info, err := m.js.UpdateConsumer(stream, cfg)
	if err != nil {
		return nil, fmt.Errorf("update consumer stream=%q durable=%q: %w", stream, cfg.Durable, err)
	}

	return info, nil
}

func (m *consumerManager) DeleteConsumer(_ context.Context, stream, durable string) error {
	if err := ValidateStreamName(stream); err != nil {
		return err
	}

	if err := ValidateDurableName(durable); err != nil {
		return err
	}

	if err := m.js.DeleteConsumer(stream, durable); err != nil {
		return fmt.Errorf("delete consumer stream=%q durable=%q: %w", stream, durable, err)
	}

	return nil
}

func (m *consumerManager) ConsumerInfo(_ context.Context, stream, durable string) (*natspkg.ConsumerInfo, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, err
	}

	if err := ValidateDurableName(durable); err != nil {
		return nil, err
	}

	info, err := m.js.ConsumerInfo(stream, durable)
	if err != nil {
		return nil, fmt.Errorf("consumer info stream=%q durable=%q: %w", stream, durable, err)
	}

	return info, nil
}

func (m *consumerManager) ConsumerNames(_ context.Context, stream string) ([]string, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, err
	}

	names := make([]string, 0)

	for name := range m.js.ConsumerNames(stream) {
		names = append(names, name)
	}

	sort.Strings(names)

	return names, nil
}

func (m *consumerManager) ListConsumers(ctx context.Context, stream string) ([]*natspkg.ConsumerInfo, error) {
	infos, _, err := m.ListConsumersPage(ctx, stream, 0, -1)

	return infos, err
}

func (m *consumerManager) ListConsumersPage(ctx context.Context, stream string, offset, limit int) ([]*natspkg.ConsumerInfo, int, error) {
	names, err := m.ConsumerNames(ctx, stream)
	if err != nil {
		return nil, 0, err
	}

	return pageInfos(names, offset, limit,
		func(name string) (*natspkg.ConsumerInfo, error) {
			info, infoErr := m.js.ConsumerInfo(stream, name)
			if infoErr != nil {
				if errors.Is(infoErr, natspkg.ErrConsumerNotFound) {
					return nil, infoErr
				}

				return nil, fmt.Errorf("list consumers stream=%q name=%q: %w", stream, name, infoErr)
			}

			return info, nil
		},
		func(err error) bool { return errors.Is(err, natspkg.ErrConsumerNotFound) },
	)
}

func (m *consumerManager) PauseConsumer(ctx context.Context, stream, durable string, pauseUntil time.Time) error {
	if err := ValidateStreamName(stream); err != nil {
		return err
	}

	if err := ValidateDurableName(durable); err != nil {
		return err
	}

	if m.jsNew == nil {
		return fmt.Errorf("pause consumer stream=%q durable=%q: %w", stream, durable, ErrJetStreamV2Required)
	}

	_, err := m.jsNew.PauseConsumer(ctx, stream, durable, pauseUntil)
	if err != nil {
		return fmt.Errorf("pause consumer stream=%q durable=%q: %w", stream, durable, err)
	}

	return nil
}

func (m *consumerManager) ResumeConsumer(ctx context.Context, stream, durable string) error {
	if err := ValidateStreamName(stream); err != nil {
		return err
	}

	if err := ValidateDurableName(durable); err != nil {
		return err
	}

	if m.jsNew == nil {
		return fmt.Errorf("resume consumer stream=%q durable=%q: %w", stream, durable, ErrJetStreamV2Required)
	}

	_, err := m.jsNew.ResumeConsumer(ctx, stream, durable)
	if err != nil {
		return fmt.Errorf("resume consumer stream=%q durable=%q: %w", stream, durable, err)
	}

	return nil
}
