package nats

import (
	"fmt"
	"regexp"
	"strings"
	"unicode"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

// validBucketRe matches JetStream KV bucket names (nats.go validBucketRe).
var validBucketRe = regexp.MustCompile(`^[a-zA-Z0-9_-]+$`)

// validKVKeyRe matches JetStream KV keys (nats.go validKeyRe).
var validKVKeyRe = regexp.MustCompile(`^[-/_=.a-zA-Z0-9]+$`)

const (
	maxNameLen              = 255
	asciiSpace              = 32
	asciiDEL                = 127
	maxSubjectLenMultiplier = 4
)

// ValidateStreamName checks JetStream stream naming rules.
// Names must be non-empty, filesystem-friendly, and must not contain
// whitespace, '.', '*', '>', '/', or '\'.
func ValidateStreamName(name string) error {
	if err := validateAssetName(name, "stream"); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidStreamName, err)
	}

	return nil
}

// ValidateDurableName checks JetStream durable/consumer naming rules
// (same constraints as stream names).
func ValidateDurableName(name string) error {
	if err := validateAssetName(name, "durable"); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidDurableName, err)
	}

	return nil
}

// ValidateQueueName checks queue group naming rules (same constraints as durables).
func ValidateQueueName(name string) error {
	if err := validateAssetName(name, "queue"); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidQueueName, err)
	}

	return nil
}

// ValidateBucketName checks JetStream KV bucket naming rules
// (alphanumeric, underscore, hyphen only).
func ValidateBucketName(name string) error {
	if bytesconv.IsEmpty(name) {
		return fmt.Errorf("%w: bucket name is empty", ErrInvalidBucketName)
	}

	if len(name) > maxNameLen {
		return fmt.Errorf("%w: bucket name exceeds %d characters", ErrInvalidBucketName, maxNameLen)
	}

	if !validBucketRe.MatchString(name) {
		return fmt.Errorf("%w: bucket name %q must match %s", ErrInvalidBucketName, name, validBucketRe.String())
	}

	return nil
}

// ValidateKVKey checks JetStream KV key naming rules.
func ValidateKVKey(key string) error {
	if bytesconv.IsEmpty(key) {
		return fmt.Errorf("%w: key is empty", ErrInvalidKVKey)
	}

	if len(key) > maxNameLen {
		return fmt.Errorf("%w: key exceeds %d characters", ErrInvalidKVKey, maxNameLen)
	}

	if !validKVKeyRe.MatchString(key) {
		return fmt.Errorf("%w: key %q must match %s", ErrInvalidKVKey, key, validKVKeyRe.String())
	}

	return nil
}

// ValidateSubject checks that subject is a valid NATS subject (wildcards allowed).
func ValidateSubject(subject string) error {
	if err := validateSubject(subject, true); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSubject, err)
	}

	return nil
}

// ValidatePublishSubject checks that subject is a valid literal publish subject
// (no wildcards).
func ValidatePublishSubject(subject string) error {
	if err := validateSubject(subject, false); err != nil {
		return fmt.Errorf("%w: %w", ErrInvalidSubject, err)
	}

	return nil
}

// ValidateSubjects validates a list of stream/filter subjects (wildcards allowed).
func ValidateSubjects(subjects []string) error {
	if len(subjects) == 0 {
		return nil
	}

	for _, subject := range subjects {
		if err := ValidateSubject(subject); err != nil {
			return err
		}
	}

	return nil
}

func validateAssetName(name, kind string) error {
	if bytesconv.IsEmpty(name) {
		return fmt.Errorf("%s name is empty", kind)
	}

	if len(name) > maxNameLen {
		return fmt.Errorf("%s name exceeds %d characters", kind, maxNameLen)
	}

	if strings.ContainsAny(name, " \t\r\n\f.*>\\/") {
		return fmt.Errorf("%s name %q contains forbidden characters (whitespace . * > / \\)", kind, name)
	}

	for _, r := range name {
		if r < asciiSpace || r == asciiDEL {
			return fmt.Errorf("%s name %q contains non-printable characters", kind, name)
		}
	}

	return nil
}

func validateSubject(subject string, allowWildcards bool) error {
	if bytesconv.IsEmpty(subject) {
		return fmt.Errorf("subject is empty")
	}

	if len(subject) > maxNameLen*maxSubjectLenMultiplier {
		return fmt.Errorf("subject exceeds maximum length")
	}

	// Tokenize without strings.Split to avoid a []string alloc on every publish.
	start := 0
	fullWildcardSeen := false

	for i := 0; i <= len(subject); i++ {
		if i < len(subject) && subject[i] != '.' {
			continue
		}

		if fullWildcardSeen {
			return fmt.Errorf("subject %q has tokens after '>'", subject)
		}

		if start == i {
			return fmt.Errorf("subject %q has empty token", subject)
		}

		token := subject[start:i]
		if hasWhitespace(token) {
			return fmt.Errorf("subject %q has whitespace in token %q", subject, token)
		}

		switch token {
		case ">":
			if !allowWildcards {
				return fmt.Errorf("publish subject %q must not contain wildcards", subject)
			}

			if i != len(subject) {
				return fmt.Errorf("subject %q: '>' must be the final token", subject)
			}

			fullWildcardSeen = true
		case "*":
			if !allowWildcards {
				return fmt.Errorf("publish subject %q must not contain wildcards", subject)
			}
		default:
			if strings.ContainsAny(token, "*>") {
				return fmt.Errorf("subject %q: wildcards must be standalone tokens", subject)
			}
		}

		start = i + 1
	}

	return nil
}

func hasWhitespace(s string) bool {
	for _, r := range s {
		if unicode.IsSpace(r) {
			return true
		}
	}

	return false
}
