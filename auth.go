package nats

import (
	"fmt"
	"net/url"
	"strings"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

// validateAuthConfig ensures at most one auth mechanism is configured.
// URL userinfo in Address counts as an auth mechanism and must not combine
// with Seed / User+Password / Secret / CredentialsFile (nats.go prefers URL userinfo).
func validateAuthConfig(conn Connection) error {
	hasSeed := !bytesconv.IsEmpty(conn.Seed)
	hasUserPass := !bytesconv.IsEmpty(conn.User) || !bytesconv.IsEmpty(conn.Password)
	hasToken := !bytesconv.IsEmpty(conn.Secret)
	hasCreds := !bytesconv.IsEmpty(conn.CredentialsFile)
	hasURLAuth := addressHasUserinfo(conn.Address)

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
	if hasURLAuth {
		n++
	}

	if n > 1 {
		return ErrConflictingAuth
	}

	if (!bytesconv.IsEmpty(conn.User)) != (!bytesconv.IsEmpty(conn.Password)) {
		return fmt.Errorf("auth: User and Password must both be set or both empty")
	}

	return nil
}

func addressHasUserinfo(address string) bool {
	if bytesconv.IsEmpty(address) {
		return false
	}
	for _, part := range strings.Split(address, ",") {
		part = strings.TrimSpace(part)
		u, err := url.Parse(part)
		if err != nil || u.User == nil {
			continue
		}
		if !bytesconv.IsEmpty(u.User.Username()) {
			return true
		}
	}

	return false
}

// redactURLString strips userinfo from NATS URLs for safe logging/status.
func redactURLString(raw string) string {
	if bytesconv.IsEmpty(raw) {
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
