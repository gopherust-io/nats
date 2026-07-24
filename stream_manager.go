package nats

import (
	"context"
	"errors"
	"fmt"

	natspkg "github.com/nats-io/nats.go"
)

type StreamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg StreamConfig) (*natspkg.StreamInfo, error)
	DeleteStream(ctx context.Context, name string) error
	StreamInfo(ctx context.Context, name string) (*natspkg.StreamInfo, error)
	ListStreams(ctx context.Context) ([]*natspkg.StreamInfo, error)
	PurgeStream(ctx context.Context, name string, opts ...PurgeOpt) error
	GetMsg(ctx context.Context, stream string, seq uint64) (*natspkg.RawStreamMsg, error)
	GetLastMsg(ctx context.Context, stream, subject string) (*natspkg.RawStreamMsg, error)
	GetNextMsgAfter(ctx context.Context, stream string, seq uint64) (*natspkg.RawStreamMsg, error)
}

type PurgeOpt func(*natspkg.StreamPurgeRequest)

type streamManager struct {
	js natspkg.JetStreamContext
}

func newStreamManager(js natspkg.JetStreamContext) StreamManager {
	return &streamManager{js: js}
}

func toNatsStreamConfig(cfg StreamConfig) *natspkg.StreamConfig {
	sc := &natspkg.StreamConfig{
		Name:         cfg.Name,
		Description:  cfg.Description,
		Subjects:     cfg.Subjects,
		Replicas:     cfg.Replicas,
		MaxMsgs:      cfg.MaxMsgs,
		MaxBytes:     cfg.MaxBytes,
		MaxAge:       cfg.MaxAge,
		MaxMsgSize:   cfg.MaxMsgSize,
		NoAck:        cfg.NoAck,
		MaxConsumers: cfg.MaxConsumers,
	}
	if cfg.Retention != 0 {
		sc.Retention = cfg.Retention
	}

	if cfg.Storage != 0 {
		sc.Storage = cfg.Storage
	}

	if cfg.Discard != 0 {
		sc.Discard = cfg.Discard
	}

	if cfg.DuplicateWindow > 0 {
		sc.Duplicates = cfg.DuplicateWindow
	}

	if cfg.Mirror != nil {
		sc.Mirror = cfg.Mirror
	}

	if len(cfg.Sources) > 0 {
		sc.Sources = cfg.Sources
	}

	if sc.Replicas == 0 {
		sc.Replicas = defaultStreamReplicas
	}

	return sc
}

func (s *streamManager) CreateOrUpdateStream(_ context.Context, cfg StreamConfig) (*natspkg.StreamInfo, error) {
	if err := ValidateStreamName(cfg.Name); err != nil {
		return nil, err
	}

	if err := ValidateSubjects(cfg.Subjects); err != nil {
		return nil, err
	}

	sc := toNatsStreamConfig(cfg)

	info, addErr := s.js.AddStream(sc)
	if addErr == nil {
		return info, nil
	}

	info, updateErr := s.js.UpdateStream(sc)
	if updateErr != nil {
		return nil, fmt.Errorf("create or update stream %q: add: %w; update: %w", cfg.Name, addErr, updateErr)
	}

	return info, nil
}

func (s *streamManager) DeleteStream(_ context.Context, name string) error {
	if err := ValidateStreamName(name); err != nil {
		return err
	}

	if err := s.js.DeleteStream(name); err != nil {
		return fmt.Errorf("delete stream %q: %w", name, err)
	}

	return nil
}

func (s *streamManager) StreamInfo(_ context.Context, name string) (*natspkg.StreamInfo, error) {
	if err := ValidateStreamName(name); err != nil {
		return nil, err
	}

	info, err := s.js.StreamInfo(name)
	if err != nil {
		return nil, fmt.Errorf("stream info %q: %w", name, err)
	}

	return info, nil
}

func (s *streamManager) ListStreams(_ context.Context) ([]*natspkg.StreamInfo, error) {
	infos := make([]*natspkg.StreamInfo, 0)

	for name := range s.js.StreamNames() {
		info, err := s.js.StreamInfo(name)
		if err != nil {
			// Concurrent create/delete (e.g. KV buckets) can race the name listing.
			if errors.Is(err, natspkg.ErrStreamNotFound) {
				continue
			}

			return nil, fmt.Errorf("list streams: info %q: %w", name, err)
		}

		infos = append(infos, info)
	}

	return infos, nil
}

func (s *streamManager) PurgeStream(_ context.Context, name string, opts ...PurgeOpt) error {
	if err := ValidateStreamName(name); err != nil {
		return err
	}

	req := &natspkg.StreamPurgeRequest{}
	for _, opt := range opts {
		opt(req)
	}

	if req.Subject != empty {
		if err := ValidateSubject(req.Subject); err != nil {
			return err
		}
	}

	if err := s.js.PurgeStream(name, req); err != nil {
		return fmt.Errorf("purge stream %q: %w", name, err)
	}

	return nil
}

func PurgeSubject(subject string) PurgeOpt {
	return func(r *natspkg.StreamPurgeRequest) {
		r.Subject = subject
	}
}

func (s *streamManager) GetMsg(_ context.Context, stream string, seq uint64) (*natspkg.RawStreamMsg, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, err
	}

	msg, err := s.js.GetMsg(stream, seq)
	if err != nil {
		return nil, fmt.Errorf("get msg stream=%q seq=%d: %w", stream, seq, err)
	}

	return msg, nil
}

func (s *streamManager) GetLastMsg(_ context.Context, stream, subject string) (*natspkg.RawStreamMsg, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, err
	}

	if err := ValidateSubject(subject); err != nil {
		return nil, err
	}

	msg, err := s.js.GetLastMsg(stream, subject)
	if err != nil {
		return nil, fmt.Errorf("get last msg stream=%q subject=%q: %w", stream, subject, err)
	}

	return msg, nil
}

func (s *streamManager) GetNextMsgAfter(ctx context.Context, stream string, seq uint64) (*natspkg.RawStreamMsg, error) {
	if err := ValidateStreamName(stream); err != nil {
		return nil, err
	}

	info, err := s.StreamInfo(ctx, stream)
	if err != nil {
		return nil, err
	}

	last := info.State.LastSeq
	for next := seq + 1; next <= last; next++ {
		msg, getErr := s.js.GetMsg(stream, next)
		if getErr == nil {
			return msg, nil
		}

		if !errors.Is(getErr, natspkg.ErrMsgNotFound) {
			return nil, fmt.Errorf("get next msg stream=%q after=%d at=%d: %w", stream, seq, next, getErr)
		}
	}

	return nil, fmt.Errorf("get next msg stream=%q after=%d: %w", stream, seq, natspkg.ErrMsgNotFound)
}
