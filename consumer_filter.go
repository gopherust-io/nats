package nats

import (
	"context"
	"strings"

	natspkg "github.com/nats-io/nats.go"
	"github.com/rs/zerolog"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

// consumerFilterSubject resolves the pull-subscribe subject from consumer config.
// When multiple filter subjects share a token prefix, a wildcard subject is derived.
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
			Msg("pull consumer filter subjects share no common token prefix; using first filter only")

		return subjects[0]
	}
}

// commonWildcardSubject returns a token-aware wildcard covering all subjects, or "".
// "orders.created"+"orders.updated" → "orders.>"; "shared"+"sharedx" → "" (not "shared.>").
func commonWildcardSubject(subjects []string) string {
	if len(subjects) == 0 {
		return ""
	}

	tokens := make([][]string, len(subjects))
	minLen := -1
	for i, s := range subjects {
		tokens[i] = strings.Split(s, ".")
		if minLen < 0 || len(tokens[i]) < minLen {
			minLen = len(tokens[i])
		}
	}
	if minLen <= 0 {
		return ""
	}

	common := 0
	for i := range minLen {
		tok := tokens[0][i]
		match := true
		for _, t := range tokens[1:] {
			if t[i] != tok {
				match = false

				break
			}
		}
		if !match {
			break
		}
		common++
	}
	if common == 0 {
		return ""
	}

	prefix := strings.Join(tokens[0][:common], ".")
	allExact := true
	for _, t := range tokens {
		if len(t) != common {
			allExact = false

			break
		}
	}
	if allExact {
		return prefix
	}

	return prefix + ".>"
}
