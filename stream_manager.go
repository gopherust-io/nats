package nats

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/gopherust-io/nats/internal/bytesconv"
	natspkg "github.com/nats-io/nats.go"
)

// StreamManager manages JetStream streams (≤10 methods for interfacebloat).
// StreamNames is available via ListStreamsPage; ListStreams is a convenience
// wrapper implemented outside the interface.
type StreamManager interface {
	CreateOrUpdateStream(ctx context.Context, cfg StreamConfig) (*natspkg.StreamInfo, error)
	AddStream(ctx context.Context, cfg *natspkg.StreamConfig) (*natspkg.StreamInfo, error)
	UpdateStream(ctx context.Context, cfg *natspkg.StreamConfig) (*natspkg.StreamInfo, error)
	DeleteStream(ctx context.Context, name string) error
	StreamInfo(ctx context.Context, name string) (*natspkg.StreamInfo, error)
	ListStreamsPage(ctx context.Context, offset, limit int) ([]*natspkg.StreamInfo, int, error)
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

func (s *streamManager) AddStream(_ context.Context, cfg *natspkg.StreamConfig) (*natspkg.StreamInfo, error) {
	if cfg == nil {
		return nil, fmt.Errorf("add stream: %w", ErrEmptyConfigNotAllowed)
	}

	if err := ValidateStreamName(cfg.Name); err != nil {
		return nil, err
	}

	info, err := s.js.AddStream(cfg)
	if err != nil {
		return nil, fmt.Errorf("add stream %q: %w", cfg.Name, err)
	}

	return info, nil
}

func (s *streamManager) UpdateStream(_ context.Context, cfg *natspkg.StreamConfig) (*natspkg.StreamInfo, error) {
	if cfg == nil {
		return nil, fmt.Errorf("update stream: %w", ErrEmptyConfigNotAllowed)
	}

	if err := ValidateStreamName(cfg.Name); err != nil {
		return nil, err
	}

	info, err := s.js.UpdateStream(cfg)
	if err != nil {
		return nil, fmt.Errorf("update stream %q: %w", cfg.Name, err)
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

func (s *streamManager) streamNames() []string {
	names := make([]string, 0)

	for name := range s.js.StreamNames() {
		names = append(names, name)
	}

	sort.Strings(names)

	return names
}

// ListStreams returns all stream infos (convenience; not on StreamManager interface).
func ListStreams(ctx context.Context, s StreamManager) ([]*natspkg.StreamInfo, error) {
	infos, _, err := s.ListStreamsPage(ctx, 0, -1)

	return infos, err
}

// StreamNames returns sorted stream names via a full ListStreamsPage.
func StreamNames(ctx context.Context, s StreamManager) ([]string, error) {
	infos, _, err := s.ListStreamsPage(ctx, 0, -1)
	if err != nil {
		return nil, err
	}

	names := make([]string, 0, len(infos))
	for _, info := range infos {
		if info != nil {
			names = append(names, info.Config.Name)
		}
	}

	return names, nil
}

func (s *streamManager) ListStreamsPage(_ context.Context, offset, limit int) ([]*natspkg.StreamInfo, int, error) {
	names := s.streamNames()

	return pageInfos(names, offset, limit,
		func(name string) (*natspkg.StreamInfo, error) {
			info, infoErr := s.js.StreamInfo(name)
			if infoErr != nil {
				if errors.Is(infoErr, natspkg.ErrStreamNotFound) {
					return nil, infoErr
				}

				return nil, fmt.Errorf("list streams: info %q: %w", name, infoErr)
			}

			return info, nil
		},
		func(err error) bool { return errors.Is(err, natspkg.ErrStreamNotFound) },
	)
}

func (s *streamManager) PurgeStream(_ context.Context, name string, opts ...PurgeOpt) error {
	if err := ValidateStreamName(name); err != nil {
		return err
	}

	req := &natspkg.StreamPurgeRequest{}
	for _, opt := range opts {
		opt(req)
	}

	if !bytesconv.IsEmpty(req.Subject) {
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
