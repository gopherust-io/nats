package dlq

import (
	"context"
	"fmt"
	"strings"
	"time"

	natspkg "github.com/nats-io/nats.go"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

// MessageSource loads DLQ (or other) stored messages for redrive.
type MessageSource interface {
	GetMsg(ctx context.Context, stream string, seq uint64) (*natspkg.RawStreamMsg, error)
	GetNextMsgAfter(ctx context.Context, stream string, seq uint64) (*natspkg.RawStreamMsg, error)
}

// ReplayConfig configures batch redrive from a DLQ stream.
//
// goalign:ignore
type ReplayConfig struct {
	SourceStream string
	DestSubject  string
	// ReasonFilter matches X-NATS-DLQ-Reason when non-empty.
	ReasonFilter string
	// SubjectFilter matches X-NATS-Original-Subject when non-empty.
	SubjectFilter   string
	StartSeq        uint64
	Limit           int
	RateLimit       time.Duration
	DryRun          bool
	StripDLQHeaders bool
}

// ReplayResult summarizes a redrive run.
type ReplayResult struct {
	Considered int
	Matched    int
	Published  int
	Skipped    int
	LastSeq    uint64
}

// ReplayToStream redrives messages from a DLQ (or any) stream onto DestSubject.
func ReplayToStream(ctx context.Context, src MessageSource, pub Publisher, cfg ReplayConfig) (*ReplayResult, error) {
	if src == nil || pub == nil {
		return nil, fmt.Errorf("dlq replay: source and publisher are required")
	}
	if bytesconv.IsEmpty(cfg.SourceStream) {
		return nil, fmt.Errorf("dlq replay: SourceStream is required")
	}
	if bytesconv.IsEmpty(cfg.DestSubject) {
		return nil, fmt.Errorf("dlq replay: DestSubject is required")
	}
	if cfg.Limit <= 0 {
		cfg.Limit = 100
	}
	if cfg.StartSeq == 0 {
		cfg.StartSeq = 1
	}

	out := &ReplayResult{}
	seq := cfg.StartSeq
	for out.Considered < cfg.Limit {
		if err := ctx.Err(); err != nil {
			return out, err
		}
		var raw *natspkg.RawStreamMsg
		var fetchErr error
		if out.Considered == 0 {
			raw, fetchErr = src.GetMsg(ctx, cfg.SourceStream, seq)
			if fetchErr != nil {
				raw, fetchErr = src.GetNextMsgAfter(ctx, cfg.SourceStream, seq-1)
			}
		} else {
			raw, fetchErr = src.GetNextMsgAfter(ctx, cfg.SourceStream, seq)
		}
		if fetchErr != nil || raw == nil {
			break
		}
		seq = raw.Sequence
		out.LastSeq = seq
		out.Considered++

		headers := cloneHeaderMap(raw.Header)
		if !matchReplayFilters(headers, cfg) {
			out.Skipped++

			continue
		}
		out.Matched++

		dest := cfg.DestSubject
		if orig := headerGet(headers, HeaderOriginalSubject); !bytesconv.IsEmpty(orig) && cfg.DestSubject == "" {
			dest = orig
		}
		if cfg.StripDLQHeaders {
			stripDLQHeaders(headers)
		}
		if cfg.DryRun {
			continue
		}
		if err := pub.PublishRaw(ctx, dest, RawPublish{Header: headers, Data: append([]byte(nil), raw.Data...)}); err != nil {
			return out, fmt.Errorf("dlq replay publish seq=%d: %w", seq, err)
		}
		out.Published++
		if cfg.RateLimit > 0 {
			select {
			case <-ctx.Done():
				return out, ctx.Err()
			case <-time.After(cfg.RateLimit):
			}
		}
	}

	return out, nil
}

func matchReplayFilters(headers map[string][]string, cfg ReplayConfig) bool {
	if !bytesconv.IsEmpty(cfg.ReasonFilter) {
		if !strings.EqualFold(headerGet(headers, HeaderReason), cfg.ReasonFilter) {
			return false
		}
	}
	if !bytesconv.IsEmpty(cfg.SubjectFilter) {
		if headerGet(headers, HeaderOriginalSubject) != cfg.SubjectFilter {
			return false
		}
	}

	return true
}

func stripDLQHeaders(headers map[string][]string) {
	for _, k := range []string{
		HeaderOriginalSubject, HeaderStream, HeaderSequence, HeaderNumDelivered,
		HeaderReason, HeaderConsumer, HeaderAutopsyError, HeaderAutopsyHash, HeaderAutopsyStack,
	} {
		delete(headers, k)
	}
}

func headerGet(headers map[string][]string, key string) string {
	if headers == nil {
		return ""
	}
	if vals, ok := headers[key]; ok && len(vals) > 0 {
		return vals[0]
	}
	// Case-insensitive fallback.
	for k, vals := range headers {
		if strings.EqualFold(k, key) && len(vals) > 0 {
			return vals[0]
		}
	}

	return ""
}

func cloneHeaderMap(h natspkg.Header) map[string][]string {
	if h == nil {
		return nil
	}
	out := make(map[string][]string, len(h))
	for k, vals := range h {
		out[k] = append([]string(nil), vals...)
	}

	return out
}
