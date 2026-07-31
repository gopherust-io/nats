package nats

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

const defaultMonitoringTimeout = 10 * time.Second

// Monitoring fetches NATS server monitoring HTTP endpoints (/varz, /jsz, …).
type Monitoring interface {
	// Fetch GETs baseURL+path. baseURL is the per-cluster monitoring root
	// (e.g. http://127.0.0.1:8222); path is typically "/jsz" or "/varz".
	Fetch(ctx context.Context, baseURL, path string) ([]byte, error)
}

type monitoringClient struct {
	http *http.Client
}

func newMonitoring(timeout time.Duration) Monitoring {
	if timeout <= 0 {
		timeout = defaultMonitoringTimeout
	}

	return &monitoringClient{
		http: &http.Client{Timeout: timeout},
	}
}

func (m *monitoringClient) Fetch(ctx context.Context, baseURL, path string) ([]byte, error) {
	baseURL = strings.TrimRight(baseURL, "/")
	if bytesconv.IsEmpty(baseURL) {
		return nil, fmt.Errorf("monitoring fetch: empty base URL")
	}

	if bytesconv.IsEmpty(path) {
		path = "/"
	} else if !strings.HasPrefix(path, "/") {
		path = "/" + path
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, baseURL+path, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("monitoring request: %w", err)
	}

	resp, err := m.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("monitoring request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("monitoring read: %w", err)
	}

	if resp.StatusCode >= http.StatusBadRequest {
		return nil, fmt.Errorf("monitoring %s: status %d: %s", path, resp.StatusCode, bytesconv.BytesToString(body))
	}

	return body, nil
}
