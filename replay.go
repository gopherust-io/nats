package nats

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	natspkg "github.com/nats-io/nats.go"
	"github.com/nats-io/nuid"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

type Replay interface {
	// ResetConsumer seeks an existing durable to a new deliver position while
	// preserving ack limits, filters, and other consumer settings when present.
	ResetConsumer(ctx context.Context, stream, durable string, opts ...ReplayOpt) (ReplayConsumerResult, error)
	// CreateReplayConsumer creates a side-car durable for backfill. The live
	// sourceDurable is left untouched. When WithReplayDurable is omitted, a
	// unique name is generated from sourceDurable.
	CreateReplayConsumer(ctx context.Context, stream, sourceDurable string, opts ...ReplayOpt) (ReplayConsumerResult, error)
	GetMsg(ctx context.Context, stream string, seq uint64) (*StoredMessage, error)
	GetLastMsgForSubject(ctx context.Context, stream, subject string) (*StoredMessage, error)
	GetNextMsgAfter(ctx context.Context, stream string, seq uint64) (*StoredMessage, error)
	GetMsgRange(ctx context.Context, stream string, startSeq, endSeq uint64, opts ...MsgRangeOpt) ([]*StoredMessage, bool, error)
	GetMsgRangeByTime(ctx context.Context, stream string, start, end time.Time, opts ...MsgRangeOpt) ([]*StoredMessage, bool, error)
	FindFirstSeqAtOrAfter(ctx context.Context, stream string, t time.Time) (uint64, error)
	FindLastSeqAtOrBefore(ctx context.Context, stream string, t time.Time) (uint64, error)
}

type replay struct {
	streams   StreamManager
	consumers ConsumerManager
}

func newReplay(streams StreamManager, consumers ConsumerManager) Replay {
	return &replay{streams: streams, consumers: consumers}
}

func (r *replay) ResetConsumer(ctx context.Context, stream, durable string, opts ...ReplayOpt) (ReplayConsumerResult, error) {
	if err := ValidateStreamName(stream); err != nil {
		return ReplayConsumerResult{}, err
	}

	if err := ValidateDurableName(durable); err != nil {
		return ReplayConsumerResult{}, err
	}

	cfg := applyReplayOpts(opts)
	if err := validateReplayFilters(cfg); err != nil {
		return ReplayConsumerResult{}, err
	}

	if err := validateReplayBounds(cfg); err != nil {
		return ReplayConsumerResult{}, err
	}

	base, err := r.baseDurableConfig(ctx, stream, durable)
	if err != nil {
		return ReplayConsumerResult{}, err
	}

	resolved, err := r.resolveReplayBounds(ctx, stream, cfg)
	if err != nil {
		return ReplayConsumerResult{}, err
	}

	merged := applyReplayConfig(base, durable, resolved)
	applyReplayBoundMetadata(&merged, resolved)

	if err := r.recreateDurable(ctx, stream, durable, merged); err != nil {
		return ReplayConsumerResult{}, fmt.Errorf("reset consumer for replay stream=%q durable=%q: %w", stream, durable, err)
	}

	return replayResult(durable, resolved), nil
}

func (r *replay) CreateReplayConsumer(
	ctx context.Context,
	stream, sourceDurable string,
	opts ...ReplayOpt,
) (ReplayConsumerResult, error) {
	if err := ValidateStreamName(stream); err != nil {
		return ReplayConsumerResult{}, err
	}

	if err := ValidateDurableName(sourceDurable); err != nil {
		return ReplayConsumerResult{}, err
	}

	cfg := applyReplayOpts(opts)
	if err := validateReplayFilters(cfg); err != nil {
		return ReplayConsumerResult{}, err
	}

	if err := validateReplayBounds(cfg); err != nil {
		return ReplayConsumerResult{}, err
	}

	base, err := r.baseDurableConfig(ctx, stream, sourceDurable)
	if err != nil {
		return ReplayConsumerResult{}, err
	}

	target := cfg.Durable
	if bytesconv.IsEmpty(target) {
		target = sourceDurable + "-replay-" + nuid.Next()
	}

	if nameErr := ValidateDurableName(target); nameErr != nil {
		return ReplayConsumerResult{}, nameErr
	}

	if target == sourceDurable {
		return ReplayConsumerResult{}, fmt.Errorf("create replay consumer: target durable must differ from source %q", sourceDurable)
	}

	resolved, err := r.resolveReplayBounds(ctx, stream, cfg)
	if err != nil {
		return ReplayConsumerResult{}, err
	}

	merged := applyReplayConfig(base, target, resolved)
	applyReplayBoundMetadata(&merged, resolved)

	if err := r.recreateDurable(ctx, stream, target, merged); err != nil {
		return ReplayConsumerResult{}, fmt.Errorf("create replay consumer stream=%q source=%q target=%q: %w",
			stream, sourceDurable, target, err)
	}

	return replayResult(target, resolved), nil
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
			return DurableConsumerConfig{Durable: durable, AckPolicy: AckExplicit, HasAckPolicy: true}, nil
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
		HasAckPolicy:      true,
		HasDeliverPolicy:  true,
		HasReplayPolicy:   true,
	}
	if cc.OptStartTime != nil {
		t := *cc.OptStartTime
		cfg.OptStartTime = &t
	}
	if len(cc.Metadata) > 0 {
		cfg.Metadata = make(map[string]string, len(cc.Metadata))
		for k, v := range cc.Metadata {
			cfg.Metadata[k] = v
		}
	}

	return cfg
}

func applyReplayConfig(base DurableConsumerConfig, durable string, cfg ReplayConfig) DurableConsumerConfig {
	out := base
	out.Durable = durable

	if !bytesconv.IsEmpty(cfg.FilterSubject) {
		out.FilterSubject = cfg.FilterSubject
		out.FilterSubjects = nil
	}

	if len(cfg.FilterSubjects) > 0 {
		out.FilterSubjects = append([]string(nil), cfg.FilterSubjects...)
		out.FilterSubject = empty
	}

	if cfg.deliverSet {
		out.DeliverPolicy = cfg.DeliverPolicy
		out.HasDeliverPolicy = true
	}

	if cfg.replaySet {
		out.ReplayPolicy = cfg.ReplayPolicy
		out.HasReplayPolicy = true
	}

	if cfg.optStartSeqSet {
		out.OptStartSeq = cfg.OptStartSeq
	}

	if cfg.optStartTimeSet {
		if cfg.OptStartTime != nil {
			t := *cfg.OptStartTime
			out.OptStartTime = &t
		} else {
			out.OptStartTime = nil
		}
	}

	if !out.HasAckPolicy && out.AckPolicy == 0 {
		out.AckPolicy = AckExplicit
		out.HasAckPolicy = true
	}

	return out
}

func applyReplayBoundMetadata(cfg *DurableConsumerConfig, replayCfg ReplayConfig) {
	if !replayCfg.untilSeqSet && !replayCfg.limitSet {
		return
	}
	meta := make(map[string]string, len(cfg.Metadata)+2)
	for k, v := range cfg.Metadata {
		meta[k] = v
	}
	if replayCfg.untilSeqSet && replayCfg.UntilSeq > 0 {
		meta[MetaReplayUntilSeq] = strconv.FormatUint(replayCfg.UntilSeq, 10)
	}
	if replayCfg.limitSet && replayCfg.Limit > 0 {
		meta[MetaReplayLimit] = strconv.Itoa(replayCfg.Limit)
	}
	cfg.Metadata = meta
}

func validateReplayFilters(cfg ReplayConfig) error {
	if !bytesconv.IsEmpty(cfg.FilterSubject) {
		if err := ValidateSubject(cfg.FilterSubject); err != nil {
			return err
		}
	}

	return ValidateSubjects(cfg.FilterSubjects)
}

func validateReplayBounds(cfg ReplayConfig) error {
	if cfg.limitSet && cfg.Limit < 1 {
		return fmt.Errorf("%w: limit must be >= 1", ErrInvalidReplayBound)
	}

	if cfg.deliverSet && cfg.DeliverPolicy == DeliverNew && (cfg.untilSeqSet || cfg.untilTimeSet || cfg.limitSet) {
		return fmt.Errorf("%w: bounds are not valid with FromNew", ErrInvalidReplayBound)
	}

	if cfg.untilSeqSet && cfg.optStartSeqSet && cfg.DeliverPolicy == DeliverByStartSequence && cfg.UntilSeq < cfg.OptStartSeq {
		return fmt.Errorf("%w: untilSeq %d < startSeq %d", ErrInvalidReplayBound, cfg.UntilSeq, cfg.OptStartSeq)
	}

	if cfg.untilTimeSet && cfg.OptStartTime != nil && cfg.UntilTime != nil && cfg.UntilTime.Before(*cfg.OptStartTime) {
		return fmt.Errorf("%w: untilTime before start time", ErrInvalidReplayBound)
	}

	return nil
}

func (r *replay) resolveReplayBounds(ctx context.Context, stream string, cfg ReplayConfig) (ReplayConfig, error) {
	out := cfg
	if out.untilTimeSet && out.UntilTime != nil && !out.untilSeqSet {
		seq, err := r.FindLastSeqAtOrBefore(ctx, stream, *out.UntilTime)
		if err != nil {
			return ReplayConfig{}, fmt.Errorf("resolve untilTime: %w", err)
		}
		out.UntilSeq = seq
		out.untilSeqSet = true
	}

	if out.limitSet && out.Limit == 1 && out.optStartSeqSet && out.OptStartSeq > 0 && !out.untilSeqSet {
		out.UntilSeq = out.OptStartSeq
		out.untilSeqSet = true
	}

	return out, nil
}

func replayResult(durable string, cfg ReplayConfig) ReplayConsumerResult {
	res := ReplayConsumerResult{Durable: durable}
	if cfg.optStartSeqSet {
		res.StartSeq = cfg.OptStartSeq
	}
	if cfg.untilSeqSet {
		res.UntilSeq = cfg.UntilSeq
	}
	if cfg.limitSet {
		res.Limit = cfg.Limit
	}
	if cfg.OptStartTime != nil {
		t := *cfg.OptStartTime
		res.StartTime = &t
	}
	if cfg.UntilTime != nil {
		t := *cfg.UntilTime
		res.UntilTime = &t
	}

	return res
}

func (r *replay) GetMsg(ctx context.Context, stream string, seq uint64) (*StoredMessage, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, err
	}

	raw, err := r.streams.GetMsg(ctx, stream, seq)
	if err != nil {
		return nil, fmt.Errorf("replay get msg stream=%q seq=%d: %w", stream, seq, err)
	}

	return storedMessageFromRaw(raw), nil
}

func (r *replay) GetLastMsgForSubject(ctx context.Context, stream, subject string) (*StoredMessage, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, err
	}

	raw, err := r.streams.GetLastMsg(ctx, stream, subject)
	if err != nil {
		return nil, fmt.Errorf("replay get last msg stream=%q subject=%q: %w", stream, subject, err)
	}

	return storedMessageFromRaw(raw), nil
}

func (r *replay) GetNextMsgAfter(ctx context.Context, stream string, seq uint64) (*StoredMessage, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, err
	}

	raw, err := r.streams.GetNextMsgAfter(ctx, stream, seq)
	if err != nil {
		return nil, fmt.Errorf("replay get next msg stream=%q after=%d: %w", stream, seq, err)
	}

	return storedMessageFromRaw(raw), nil
}

func (r *replay) GetMsgRange(
	ctx context.Context,
	stream string,
	startSeq, endSeq uint64,
	opts ...MsgRangeOpt,
) ([]*StoredMessage, bool, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, false, err
	}

	if startSeq == 0 || endSeq == 0 {
		return nil, false, fmt.Errorf("%w: startSeq and endSeq must be >= 1", ErrInvalidReplayBound)
	}

	if endSeq < startSeq {
		return nil, false, fmt.Errorf("%w: endSeq %d < startSeq %d", ErrInvalidReplayBound, endSeq, startSeq)
	}

	cfg := applyMsgRangeOpts(opts)
	out := make([]*StoredMessage, 0)
	truncated := false

	var cur uint64
	raw, err := r.streams.GetMsg(ctx, stream, startSeq)
	switch {
	case err == nil:
		out = append(out, storedMessageFromRaw(raw))
		cur = startSeq
	case errors.Is(err, natspkg.ErrMsgNotFound):
		next, nextErr := r.streams.GetNextMsgAfter(ctx, stream, startSeq-1)
		if nextErr != nil {
			if errors.Is(nextErr, natspkg.ErrMsgNotFound) {
				return out, false, nil
			}

			return nil, false, fmt.Errorf("replay get msg range stream=%q start=%d: %w", stream, startSeq, nextErr)
		}
		if next.Sequence > endSeq {
			return out, false, nil
		}
		out = append(out, storedMessageFromRaw(next))
		cur = next.Sequence
	default:
		return nil, false, fmt.Errorf("replay get msg range stream=%q start=%d: %w", stream, startSeq, err)
	}

	for {
		if len(out) >= cfg.max {
			if cur < endSeq {
				next, nextErr := r.streams.GetNextMsgAfter(ctx, stream, cur)
				if nextErr == nil && next.Sequence <= endSeq {
					truncated = true
				} else if nextErr != nil && !errors.Is(nextErr, natspkg.ErrMsgNotFound) {
					return nil, false, nextErr
				}
			}

			break
		}
		if cur >= endSeq {
			break
		}
		next, nextErr := r.streams.GetNextMsgAfter(ctx, stream, cur)
		if nextErr != nil {
			if errors.Is(nextErr, natspkg.ErrMsgNotFound) {
				break
			}

			return nil, false, fmt.Errorf("replay get msg range stream=%q after=%d: %w", stream, cur, nextErr)
		}
		if next.Sequence > endSeq {
			break
		}
		out = append(out, storedMessageFromRaw(next))
		cur = next.Sequence
	}

	return out, truncated, nil
}

func (r *replay) GetMsgRangeByTime(
	ctx context.Context,
	stream string,
	start, end time.Time,
	opts ...MsgRangeOpt,
) ([]*StoredMessage, bool, error) {
	if end.Before(start) {
		return nil, false, fmt.Errorf("%w: end time before start time", ErrInvalidReplayBound)
	}

	startSeq, err := r.FindFirstSeqAtOrAfter(ctx, stream, start)
	if err != nil {
		return nil, false, err
	}

	endSeq, err := r.FindLastSeqAtOrBefore(ctx, stream, end)
	if err != nil {
		return nil, false, err
	}

	if endSeq < startSeq {
		return []*StoredMessage{}, false, nil
	}

	return r.GetMsgRange(ctx, stream, startSeq, endSeq, opts...)
}

func (r *replay) FindFirstSeqAtOrAfter(ctx context.Context, stream string, t time.Time) (uint64, error) {
	if err := ValidateStreamName(stream); err != nil {
		return 0, err
	}

	info, err := r.streams.StreamInfo(ctx, stream)
	if err != nil {
		return 0, err
	}

	first, last := info.State.FirstSeq, info.State.LastSeq
	if last == 0 || first == 0 {
		return 0, natspkg.ErrMsgNotFound
	}

	lo, hi := first, last
	var found uint64
	for lo <= hi {
		mid := lo + (hi-lo)/2
		msg, getErr := r.getExistingAtOrNear(ctx, stream, mid, first, last)
		if getErr != nil {
			return 0, getErr
		}
		if !msg.Time.Before(t) {
			found = msg.Sequence
			if msg.Sequence == 0 {
				break
			}
			hi = msg.Sequence - 1
			if hi < first {
				break
			}
		} else {
			lo = msg.Sequence + 1
		}
	}

	if found == 0 {
		return 0, natspkg.ErrMsgNotFound
	}

	return found, nil
}

func (r *replay) FindLastSeqAtOrBefore(ctx context.Context, stream string, t time.Time) (uint64, error) {
	if err := ValidateStreamName(stream); err != nil {
		return 0, err
	}

	info, err := r.streams.StreamInfo(ctx, stream)
	if err != nil {
		return 0, err
	}

	first, last := info.State.FirstSeq, info.State.LastSeq
	if last == 0 || first == 0 {
		return 0, natspkg.ErrMsgNotFound
	}

	lo, hi := first, last
	var found uint64
	for lo <= hi {
		mid := lo + (hi-lo)/2
		msg, getErr := r.getExistingAtOrNear(ctx, stream, mid, first, last)
		if getErr != nil {
			return 0, getErr
		}
		if !msg.Time.After(t) {
			found = msg.Sequence
			lo = msg.Sequence + 1
		} else {
			if msg.Sequence == 0 {
				break
			}
			hi = msg.Sequence - 1
			if hi < first {
				break
			}
		}
	}

	if found == 0 {
		return 0, natspkg.ErrMsgNotFound
	}

	return found, nil
}

func (r *replay) getExistingAtOrNear(
	ctx context.Context,
	stream string,
	seq, first, last uint64,
) (*natspkg.RawStreamMsg, error) {
	if seq < first {
		seq = first
	}
	if seq > last {
		seq = last
	}

	msg, err := r.streams.GetMsg(ctx, stream, seq)
	if err == nil {
		return msg, nil
	}
	if !errors.Is(err, natspkg.ErrMsgNotFound) {
		return nil, err
	}

	next, nextErr := r.streams.GetNextMsgAfter(ctx, stream, seq)
	if nextErr == nil {
		return next, nil
	}
	if !errors.Is(nextErr, natspkg.ErrMsgNotFound) {
		return nil, nextErr
	}

	// walk backward from seq
	for prev := seq; prev >= first; prev-- {
		m, getErr := r.streams.GetMsg(ctx, stream, prev)
		if getErr == nil {
			return m, nil
		}
		if !errors.Is(getErr, natspkg.ErrMsgNotFound) {
			return nil, getErr
		}
		if prev == 0 {
			break
		}
	}

	return nil, natspkg.ErrMsgNotFound
}

func storedMessageFromRaw(raw *natspkg.RawStreamMsg) *StoredMessage {
	msg := &StoredMessage{
		Sequence:    raw.Sequence,
		Subject:     raw.Subject,
		Time:        raw.Time,
		Data:        raw.Data,
		MessageType: JSON,
	}
	if raw.Header != nil {
		msg.Header = raw.Header
		msg.MessageType = MessageTypeFromHeader(raw.Header)
	}

	return msg
}
