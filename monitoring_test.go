package nats

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMonitoringRejectsOversizeBody(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(strings.Repeat("x", 128)))
	}))
	t.Cleanup(srv.Close)

	m := newMonitoring(0).(*monitoringClient)
	m.maxBody = 64

	_, err := m.Fetch(context.Background(), srv.URL, "/jsz")
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrMonitoringBodyTooLarge)
}

func TestMonitoringAcceptsBodyWithinLimit(t *testing.T) {
	t.Parallel()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"ok":true}`))
	}))
	t.Cleanup(srv.Close)

	m := newMonitoring(0)
	body, err := m.Fetch(context.Background(), srv.URL, "/jsz")
	require.NoError(t, err)
	assert.Contains(t, string(body), `"ok":true`)
}

func TestValidateMonitoringFetchURL(t *testing.T) {
	t.Parallel()

	ctx := t.Context()
	require.Error(t, validateMonitoringFetchURL(ctx, mustURL(t, "http://169.254.169.254/latest/meta-data")))
	require.Error(t, validateMonitoringFetchURL(ctx, mustURL(t, "http://metadata.google.internal/")))
	require.Error(t, validateMonitoringFetchURL(ctx, mustURL(t, "ftp://127.0.0.1/jsz")))
	require.NoError(t, validateMonitoringFetchURL(ctx, mustURL(t, "http://127.0.0.1:8222/jsz")))
	require.NoError(t, validateMonitoringFetchURL(ctx, mustURL(t, "http://10.0.0.5:8222/varz")))
}

func mustURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return u
}
