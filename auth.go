package nats

import (
	"fmt"
	"net/url"
	"strings"
)

// validateAuthConfig ensures at most one auth mechanism is configured.
func validateAuthConfig(conn Connection) error {
	hasSeed := conn.Seed != empty
	hasUserPass := conn.User != empty || conn.Password != empty
	hasToken := conn.Secret != empty
	hasCreds := conn.CredentialsFile != empty

	n := 0
	if hasSeed {
		n++
	}
	if hasUserPass {
		n++
	}
	if hasToken {
		n++
	}
	if hasCreds {
		n++
	}

	if n > 1 {
		return ErrConflictingAuth
	}

	if (conn.User != empty) != (conn.Password != empty) {
		return fmt.Errorf("auth: User and Password must both be set or both empty")
	}

	return nil
}

// redactURLString strips userinfo from NATS URLs for safe logging/status.
func redactURLString(raw string) string {
	if raw == empty {
		return raw
	}

	parts := strings.Split(raw, ",")
	for i, part := range parts {
		part = strings.TrimSpace(part)
		u, err := url.Parse(part)
		if err != nil || u.User == nil {
			parts[i] = part
			continue
		}
		u.User = nil
		parts[i] = u.String()
	}

	return strings.Join(parts, ",")
}
