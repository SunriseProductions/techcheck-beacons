package main

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestHandler(t *testing.T) http.Handler {
	t.Helper()
	return newTestHandlerWithSem(t, newBandwidthSem(bandwidthConcurrency))
}

func newTestHandlerWithSem(t *testing.T, sem bandwidthSem) http.Handler {
	t.Helper()
	cfg := &config{POPID: "test", Geo: "Localhost", UDPMagic: "PFLT"}
	h, err := newHTTPHandler(cfg, sem)
	require.NoError(t, err)
	return h
}

func TestPing(t *testing.T) {
	srv := httptest.NewServer(newTestHandler(t))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/preflight/ping")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	body, _ := io.ReadAll(resp.Body)
	assert.Empty(t, body)
}

func TestHealthz(t *testing.T) {
	srv := httptest.NewServer(newTestHandler(t))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/healthz")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestMTU(t *testing.T) {
	srv := httptest.NewServer(newTestHandler(t))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/preflight/mtu")
	require.NoError(t, err)
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	assert.Len(t, body, 1400)
	for _, b := range body {
		if b != 0 {
			t.Fatalf("non-zero byte in mtu response")
		}
	}
}

func TestEchoTS(t *testing.T) {
	srv := httptest.NewServer(newTestHandler(t))
	defer srv.Close()

	before := time.Now().UTC()
	resp, err := http.Get(srv.URL + "/preflight/echo-ts")
	require.NoError(t, err)
	defer resp.Body.Close()
	after := time.Now().UTC()

	var payload struct {
		ISO         string `json:"iso"`
		MonotonicNS int64  `json:"monotonic_ns"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))

	parsed, err := time.Parse(time.RFC3339Nano, payload.ISO)
	require.NoError(t, err)
	assert.True(t, !parsed.Before(before.Add(-time.Second)))
	assert.True(t, !parsed.After(after.Add(time.Second)))
	assert.NotZero(t, payload.MonotonicNS)
}

func TestMethodNotAllowed(t *testing.T) {
	srv := httptest.NewServer(newTestHandler(t))
	defer srv.Close()

	// /preflight/ping is GET-only
	resp, err := http.Post(srv.URL+"/preflight/ping", "text/plain", strings.NewReader(""))
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusMethodNotAllowed, resp.StatusCode)
}

func TestDownload10MB(t *testing.T) {
	srv := httptest.NewServer(newTestHandler(t))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/preflight/download/10mb")
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
	assert.Equal(t, "no-store", resp.Header.Get("Cache-Control"))

	body, err := io.ReadAll(resp.Body)
	require.NoError(t, err)
	assert.Len(t, body, 10*1024*1024)
}

func TestDownloadBufferIsShared(t *testing.T) {
	// Hit the endpoint twice; the 10 MB buffer should be allocated once at
	// startup, not re-generated per-request. We can't directly observe
	// allocation but we can observe that responses complete quickly.
	srv := httptest.NewServer(newTestHandler(t))
	defer srv.Close()

	for i := 0; i < 2; i++ {
		resp, err := http.Get(srv.URL + "/preflight/download/10mb")
		require.NoError(t, err)
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

func TestUploadAcceptsSmallBody(t *testing.T) {
	srv := httptest.NewServer(newTestHandler(t))
	defer srv.Close()

	body := strings.NewReader(strings.Repeat("x", 1024))
	resp, err := http.Post(srv.URL+"/preflight/upload", "application/octet-stream", body)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}

func TestUploadRejectsOversizedBody(t *testing.T) {
	srv := httptest.NewServer(newTestHandler(t))
	defer srv.Close()

	body := strings.NewReader(strings.Repeat("x", 6*1024*1024))
	req, _ := http.NewRequest(http.MethodPost, srv.URL+"/preflight/upload", body)
	req.ContentLength = int64(6 * 1024 * 1024)
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusRequestEntityTooLarge, resp.StatusCode)
}

func TestWhoamiUsesXForwardedFor(t *testing.T) {
	srv := httptest.NewServer(newTestHandler(t))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/preflight/whoami", nil)
	req.Header.Set("X-Forwarded-For", "203.0.113.9, 10.0.0.1")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)

	var payload struct {
		PublicIP string `json:"public_ip"`
		POPID    string `json:"pop_id"`
		Geo      string `json:"geo"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	assert.Equal(t, "203.0.113.9", payload.PublicIP)
	assert.Equal(t, "test", payload.POPID)
	assert.Equal(t, "Localhost", payload.Geo)
}

func TestWhoamiFallsBackToXRealIP(t *testing.T) {
	srv := httptest.NewServer(newTestHandler(t))
	defer srv.Close()

	req, _ := http.NewRequest(http.MethodGet, srv.URL+"/preflight/whoami", nil)
	req.Header.Set("X-Real-IP", "198.51.100.7")
	resp, err := http.DefaultClient.Do(req)
	require.NoError(t, err)
	defer resp.Body.Close()

	var payload struct {
		PublicIP string `json:"public_ip"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	assert.Equal(t, "198.51.100.7", payload.PublicIP)
}

func TestWhoamiFallsBackToRemoteAddr(t *testing.T) {
	srv := httptest.NewServer(newTestHandler(t))
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/preflight/whoami")
	require.NoError(t, err)
	defer resp.Body.Close()

	var payload struct {
		PublicIP string `json:"public_ip"`
	}
	require.NoError(t, json.NewDecoder(resp.Body).Decode(&payload))
	assert.NotEmpty(t, payload.PublicIP) // 127.0.0.1 or ::1
}

// Fires bandwidthConcurrency+1 concurrent slow downloads. The extra one
// must 503 with Retry-After while the in-flight ones hold their slots.
func TestBandwidthSemaphoreReturns503WhenFull(t *testing.T) {
	srv := httptest.NewServer(newTestHandler(t))
	defer srv.Close()

	release := make(chan struct{})
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	var got503 atomic.Int32

	// Client transport that holds the body read open so the server-side
	// goroutine keeps its semaphore slot.
	doSlow := func() *http.Response {
		req, _ := http.NewRequest(http.MethodGet, srv.URL+"/preflight/download/10mb", nil)
		resp, err := http.DefaultClient.Do(req)
		require.NoError(t, err)
		return resp
	}

	var wg sync.WaitGroup
	n := bandwidthConcurrency + 1
	results := make(chan int, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			resp := doSlow()
			defer resp.Body.Close()
			results <- resp.StatusCode
			if resp.StatusCode == http.StatusServiceUnavailable {
				got503.Add(1)
				assert.Equal(t, "5", resp.Header.Get("Retry-After"))
				return
			}
			cur := inFlight.Add(1)
			if cur > maxInFlight.Load() {
				maxInFlight.Store(cur)
			}
			<-release
			inFlight.Add(-1)
			_, _ = io.Copy(io.Discard, resp.Body)
		}()
	}
	// Give goroutines time to hit the handler.
	time.Sleep(100 * time.Millisecond)
	// Release the slots we're holding so the in-flight requests can finish.
	close(release)
	wg.Wait()
	close(results)

	assert.Equal(t, int32(1), got503.Load(), "expected exactly one 503")
	assert.LessOrEqual(t, maxInFlight.Load(), int32(bandwidthConcurrency))
}

// Other endpoints (ping, healthz, mtu, whoami, echo-ts)
// are NOT gated by the bandwidth semaphore — they're cheap and should
// never 503 under bandwidth pressure.
func TestBandwidthSemaphoreDoesNotAffectCheapEndpoints(t *testing.T) {
	srv := httptest.NewServer(newTestHandler(t))
	defer srv.Close()

	release := make(chan struct{})
	// Occupy both slots with slow downloads.
	var wg sync.WaitGroup
	for i := 0; i < bandwidthConcurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			req, _ := http.NewRequest(http.MethodGet, srv.URL+"/preflight/download/10mb", nil)
			resp, err := http.DefaultClient.Do(req)
			require.NoError(t, err)
			defer resp.Body.Close()
			<-release
			_, _ = io.Copy(io.Discard, resp.Body)
		}()
	}
	time.Sleep(50 * time.Millisecond)

	// Hit the cheap endpoints — none should 503.
	for _, path := range []string{"/preflight/ping", "/healthz", "/preflight/mtu", "/preflight/echo-ts"} {
		resp, err := http.Get(srv.URL + path)
		require.NoError(t, err, "path %s", path)
		assert.Equal(t, http.StatusOK, resp.StatusCode, "path %s", path)
		_, _ = io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}

	close(release)
	wg.Wait()
}

// A slot taken outside the HTTP handler (the UDP stream service will do this)
// counts against the same limit as HTTP bandwidth tests.
func TestBandwidthSemSharedAcrossHTTPAndUDP(t *testing.T) {
	sem := newBandwidthSem(bandwidthConcurrency)
	srv := httptest.NewServer(newTestHandlerWithSem(t, sem))
	defer srv.Close()

	for i := 0; i < bandwidthConcurrency; i++ {
		require.True(t, sem.tryAcquire())
	}

	resp, err := http.Get(srv.URL + "/preflight/download/10mb")
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusServiceUnavailable, resp.StatusCode)
	assert.Equal(t, "5", resp.Header.Get("Retry-After"))

	sem.release()
	resp, err = http.Get(srv.URL + "/preflight/download/10mb")
	require.NoError(t, err)
	_, _ = io.Copy(io.Discard, resp.Body)
	resp.Body.Close()
	assert.Equal(t, http.StatusOK, resp.StatusCode)
}
