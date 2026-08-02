package nats

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/gopherust-io/nats/internal/bytesconv"
)

const (
	defaultMonitoringTimeout = 10 * time.Second
	defaultMonitoringMaxBody = 8 << 20 // 8 MiB
)

// ErrMonitoringBodyTooLarge is returned when a monitoring response exceeds the size cap.
var ErrMonitoringBodyTooLarge = errors.New("monitoring response body too large")

// Monitoring fetches NATS server monitoring HTTP endpoints (/varz, /jsz, …).
type Monitoring interface {
	// Fetch GETs baseURL+path. baseURL is the per-cluster monitoring root
	// (e.g. http://127.0.0.1:8222); path is typically "/jsz" or "/varz".
	Fetch(ctx context.Context, baseURL, path string) ([]byte, error)
}

type monitoringClient struct {
	http    *http.Client
	maxBody int64
}

func newMonitoring(timeout time.Duration) Monitoring {
	if timeout <= 0 {
		timeout = defaultMonitoringTimeout
	}

	var transport *http.Transport
	if base, ok := http.DefaultTransport.(*http.Transport); ok {
		transport = base.Clone()
	} else {
		transport = &http.Transport{}
	}
	transport.DialContext = dialMonitoringContext

	client := &http.Client{
		Timeout:   timeout,
		Transport: transport,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			if len(via) >= 5 {
				return errors.New("monitoring redirect: stopped after 5 redirects")
			}
			if err := validateMonitoringFetchURL(req.Context(), req.URL); err != nil {
				return err
			}

			return nil
		},
	}

	return &monitoringClient{
		http:    client,
		maxBody: defaultMonitoringMaxBody,
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

	rawURL := baseURL + path
	parsed, err := url.Parse(rawURL)
	if err != nil {
		return nil, fmt.Errorf("monitoring request: %w", err)
	}
	if validateErr := validateMonitoringFetchURL(ctx, parsed); validateErr != nil {
		return nil, validateErr
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, http.NoBody)
	if err != nil {
		return nil, fmt.Errorf("monitoring request: %w", err)
	}

	resp, err := m.http.Do(req)
	if err != nil {
		return nil, fmt.Errorf("monitoring request: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()

	maxBody := m.maxBody
	if maxBody <= 0 {
		maxBody = defaultMonitoringMaxBody
	}
	limited := io.LimitReader(resp.Body, maxBody+1)
	body, err := io.ReadAll(limited)
	if err != nil {
		return nil, fmt.Errorf("monitoring read: %w", err)
	}
	if int64(len(body)) > maxBody {
		return nil, fmt.Errorf("%w: exceeds %d bytes", ErrMonitoringBodyTooLarge, maxBody)
	}

	if resp.StatusCode >= http.StatusBadRequest {
		snippet := body
		const maxErrBody = 256
		if len(snippet) > maxErrBody {
			snippet = snippet[:maxErrBody]
		}

		return nil, fmt.Errorf("monitoring %s: status %d: %s", path, resp.StatusCode, bytesconv.BytesToString(snippet))
	}

	return body, nil
}

func dialMonitoringContext(ctx context.Context, network, addr string) (net.Conn, error) {
	host, port, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return nil, fmt.Errorf("monitoring dns: %w", err)
	}
	var dialer net.Dialer
	var lastErr error
	for _, ipAddr := range ips {
		if isBlockedMonitoringIP(ipAddr.IP) {
			lastErr = errors.New("monitoring url host not allowed")

			continue
		}
		conn, dialErr := dialer.DialContext(ctx, network, net.JoinHostPort(ipAddr.IP.String(), port))
		if dialErr == nil {
			return conn, nil
		}
		lastErr = dialErr
	}
	if lastErr == nil {
		lastErr = errors.New("monitoring url host not allowed")
	}

	return nil, lastErr
}

func isBlockedMonitoringIP(ip net.IP) bool {
	if ip == nil || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || ip.IsUnspecified() {
		return true
	}
	// Cloud metadata / IMDS addresses beyond link-local.
	if ip4 := ip.To4(); ip4 != nil {
		// Alibaba cloud metadata
		if ip4[0] == 100 && ip4[1] == 100 && ip4[2] == 100 && ip4[3] == 200 {
			return true
		}
	}
	// AWS IMDS IPv6
	if ip.Equal(net.ParseIP("fd00:ec2::254")) {
		return true
	}

	return false
}

// validateMonitoringFetchURL blocks link-local / cloud-metadata targets after
// parse and on redirects. Loopback and RFC1918 remain allowed for self-hosted labs.
func validateMonitoringFetchURL(ctx context.Context, fetchURL *url.URL) error {
	if fetchURL == nil {
		return errors.New("monitoring url host not allowed")
	}
	switch strings.ToLower(fetchURL.Scheme) {
	case "http", "https":
	default:
		return errors.New("monitoring url scheme must be http or https")
	}
	host := strings.ToLower(strings.TrimSpace(fetchURL.Hostname()))
	if bytesconv.IsEmpty(host) {
		return errors.New("monitoring url host not allowed")
	}
	switch host {
	case "metadata.google.internal", "metadata.goog", "instance-data":
		return errors.New("monitoring url host not allowed")
	}
	if ip := net.ParseIP(host); ip != nil {
		if isBlockedMonitoringIP(ip) {
			return errors.New("monitoring url host not allowed")
		}

		return nil
	}
	addrs, err := net.DefaultResolver.LookupIPAddr(ctx, host)
	if err != nil {
		return fmt.Errorf("monitoring dns: %w", err)
	}
	for _, ipAddr := range addrs {
		if isBlockedMonitoringIP(ipAddr.IP) {
			return errors.New("monitoring url host not allowed")
		}
	}

	return nil
}
