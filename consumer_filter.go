package nats

import (
	"context"
	"strings"

	"github.com/gopherust-io/nats/internal/bytesconv"
	natspkg "github.com/nats-io/nats.go"
	"github.com/rs/zerolog"
)

// consumerFilterSubject resolves the pull-subscribe subject from consumer config.
// When multiple filter subjects share a prefix, a wildcard subject is derived.
func consumerFilterSubject(cfg natspkg.ConsumerConfig) string {
	if !bytesconv.IsEmpty(cfg.FilterSubject) {
		return cfg.FilterSubject
	}

	return consumerFilterSubjects(cfg.FilterSubjects)
}

func consumerFilterSubjects(subjects []string) string {
	switch len(subjects) {
	case 0:
		return ">"
	case 1:
		return subjects[0]
	default:
		if wild := commonWildcardSubject(subjects); !bytesconv.IsEmpty(wild) {
			return wild
		}

		zerolog.Ctx(context.Background()).Warn().
			Any("filter_subjects", subjects).
			Str("selected", subjects[0]).
			Msg("pull consumer filter subjects share no common prefix; using first filter only")

		return subjects[0]
	}
}

func commonWildcardSubject(subjects []string) string {
	if len(subjects) == 0 {
		return ""
	}

	prefix := subjects[0]
	for _, s := range subjects[1:] {
		prefix = commonPrefix(prefix, s)
		if bytesconv.IsEmpty(prefix) {
			return ""
		}
	}

	if i := strings.LastIndex(prefix, "."); i >= 0 {
		return prefix[:i+1] + ">"
	}

	if !bytesconv.IsEmpty(prefix) && !strings.HasSuffix(prefix, ">") {
		return prefix + ".>"
	}

	return prefix
}

func commonPrefix(first, second string) string {
	n := min(len(second), len(first))

	i := 0
	for i < n && first[i] == second[i] {
		i++
	}

	return first[:i]
}
