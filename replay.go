package nats

import (
	"context"
	"errors"
	"fmt"

	natspkg "github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"
)

type Replay interface {
	// ResetConsumer seeks an existing durable to a new deliver position while
	// preserving ack limits, filters, and other consumer settings when present.
	ResetConsumer(ctx context.Context, stream, durable string, opts ...ReplayOpt) error
	// CreateReplayConsumer creates a side-car durable for backfill. The live
	// sourceDurable is left untouched. When WithReplayDurable is omitted, a
	// unique name is generated from sourceDurable.
	CreateReplayConsumer(ctx context.Context, stream, sourceDurable string, opts ...ReplayOpt) (durable string, err error)
	GetMsg(ctx context.Context, stream string, seq uint64) (*Message, error)
	GetLastMsgForSubject(ctx context.Context, stream, subject string) (*Message, error)
	GetNextMsgAfter(ctx context.Context, stream string, seq uint64) (*Message, error)
}

type replay struct {
	streams   StreamManager
	consumers ConsumerManager
}

func newReplay(streams StreamManager, consumers ConsumerManager) Replay {
	return &replay{streams: streams, consumers: consumers}
}

func (r *replay) ResetConsumer(ctx context.Context, stream, durable string, opts ...ReplayOpt) error {
	if err := ValidateStreamName(stream); err != nil {
		return err
	}

	if err := ValidateDurableName(durable); err != nil {
		return err
	}

	cfg := applyReplayOpts(opts)
	if err := validateReplayFilters(cfg); err != nil {
		return err
	}

	base, err := r.baseDurableConfig(ctx, stream, durable)
	if err != nil {
		return err
	}

	merged := applyReplayConfig(base, durable, cfg)

	if err := r.recreateDurable(ctx, stream, durable, merged); err != nil {
		return fmt.Errorf("reset consumer for replay stream=%q durable=%q: %w", stream, durable, err)
	}

	return nil
}

func (r *replay) CreateReplayConsumer(
	ctx context.Context,
	stream, sourceDurable string,
	opts ...ReplayOpt,
) (string, error) {
	if err := ValidateStreamName(stream); err != nil {
		return empty, err
	}

	if err := ValidateDurableName(sourceDurable); err != nil {
		return empty, err
	}

	cfg := applyReplayOpts(opts)
	if err := validateReplayFilters(cfg); err != nil {
		return empty, err
	}

	base, err := r.baseDurableConfig(ctx, stream, sourceDurable)
	if err != nil {
		return empty, err
	}

	target := cfg.Durable
	if target == empty {
		target = sourceDurable + "-replay-" + nuid.Next()
	}

	if nameErr := ValidateDurableName(target); nameErr != nil {
		return empty, nameErr
	}

	if target == sourceDurable {
		return empty, fmt.Errorf("create replay consumer: target durable must differ from source %q", sourceDurable)
	}

	merged := applyReplayConfig(base, target, cfg)

	if err := r.recreateDurable(ctx, stream, target, merged); err != nil {
		return empty, fmt.Errorf("create replay consumer stream=%q source=%q target=%q: %w",
			stream, sourceDurable, target, err)
	}

	return target, nil
}

// recreateDurable deletes an existing durable (if any) then creates it.
// Seeking / side-car replay intentionally resets consumer state; callers that
// only want in-place updates should use Consumers().CreateOrUpdateConsumer.
func (r *replay) recreateDurable(ctx context.Context, stream, durable string, cfg DurableConsumerConfig) error {
	_, infoErr := r.consumers.ConsumerInfo(ctx, stream, durable)
	switch {
	case infoErr == nil:
		if delErr := r.consumers.DeleteConsumer(ctx, stream, durable); delErr != nil {
			return delErr
		}
	case errors.Is(infoErr, natspkg.ErrConsumerNotFound):
		// create fresh
	default:
		return infoErr
	}

	_, err := r.consumers.CreateOrUpdateConsumer(ctx, stream, cfg)

	return err
}

func (r *replay) baseDurableConfig(ctx context.Context, stream, durable string) (DurableConsumerConfig, error) {
	info, err := r.consumers.ConsumerInfo(ctx, stream, durable)
	if err != nil {
		if errors.Is(err, natspkg.ErrConsumerNotFound) {
			return DurableConsumerConfig{Durable: durable, AckPolicy: AckExplicit}, nil
		}

		return DurableConsumerConfig{}, err
	}

	return durableConfigFromNATS(info.Config), nil
}

func durableConfigFromNATS(cc natspkg.ConsumerConfig) DurableConsumerConfig {
	cfg := DurableConsumerConfig{
		Durable:           cc.Durable,
		FilterSubject:     cc.FilterSubject,
		FilterSubjects:    append([]string(nil), cc.FilterSubjects...),
		DeliverPolicy:     cc.DeliverPolicy,
		ReplayPolicy:      cc.ReplayPolicy,
		AckPolicy:         cc.AckPolicy,
		MaxDeliver:        cc.MaxDeliver,
		AckWait:           cc.AckWait,
		MaxAckPending:     cc.MaxAckPending,
		RateLimit:         cc.RateLimit,
		Heartbeat:         cc.Heartbeat,
		InactiveThreshold: cc.InactiveThreshold,
		FlowControl:       cc.FlowControl,
		Replicas:          cc.Replicas,
		MemStorage:        cc.MemoryStorage,
		MaxWaiting:        cc.MaxWaiting,
		OptStartSeq:       cc.OptStartSeq,
	}
	if cc.OptStartTime != nil {
		t := *cc.OptStartTime
		cfg.OptStartTime = &t
	}

	return cfg
}

func applyReplayConfig(base DurableConsumerConfig, durable string, cfg ReplayConfig) DurableConsumerConfig {
	out := base
	out.Durable = durable

	if cfg.FilterSubject != empty {
		out.FilterSubject = cfg.FilterSubject
		out.FilterSubjects = nil
	}

	if len(cfg.FilterSubjects) > 0 {
		out.FilterSubjects = append([]string(nil), cfg.FilterSubjects...)
		out.FilterSubject = empty
	}

	if cfg.DeliverPolicy != 0 {
		out.DeliverPolicy = cfg.DeliverPolicy
	}

	if cfg.ReplayPolicy != 0 {
		out.ReplayPolicy = cfg.ReplayPolicy
	}

	out.OptStartSeq = cfg.OptStartSeq
	if cfg.OptStartTime != nil {
		t := *cfg.OptStartTime
		out.OptStartTime = &t
	} else if cfg.DeliverPolicy != DeliverByStartTime {
		out.OptStartTime = nil
	}

	if out.AckPolicy == 0 {
		out.AckPolicy = AckExplicit
	}

	return out
}

func validateReplayFilters(cfg ReplayConfig) error {
	if cfg.FilterSubject != empty {
		if err := ValidateSubject(cfg.FilterSubject); err != nil {
			return err
		}
	}

	return ValidateSubjects(cfg.FilterSubjects)
}

func (r *replay) GetMsg(ctx context.Context, stream string, seq uint64) (*Message, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, err
	}

	raw, err := r.streams.GetMsg(ctx, stream, seq)
	if err != nil {
		return nil, fmt.Errorf("replay get msg stream=%q seq=%d: %w", stream, seq, err)
	}

	return messageFromRaw(raw), nil
}

func (r *replay) GetLastMsgForSubject(ctx context.Context, stream, subject string) (*Message, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, err
	}

	raw, err := r.streams.GetLastMsg(ctx, stream, subject)
	if err != nil {
		return nil, fmt.Errorf("replay get last msg stream=%q subject=%q: %w", stream, subject, err)
	}

	return messageFromRaw(raw), nil
}

func (r *replay) GetNextMsgAfter(ctx context.Context, stream string, seq uint64) (*Message, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, err
	}

	raw, err := r.streams.GetNextMsgAfter(ctx, stream, seq)
	if err != nil {
		return nil, fmt.Errorf("replay get next msg stream=%q after=%d: %w", stream, seq, err)
	}

	return messageFromRaw(raw), nil
}

func messageFromRaw(raw *natspkg.RawStreamMsg) *Message {
	msg := &Message{
		Data:        raw.Data,
		MessageType: JSON,
	}
	if raw.Header != nil {
		msg.Header = raw.Header
		msg.MessageType = MessageTypeFromHeader(raw.Header)
	}

	return msg
}
