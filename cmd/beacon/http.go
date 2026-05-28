package main

import (
	"crypto/rand"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"
)

const (
	downloadSize   = 10 * 1024 * 1024
	uploadMaxBytes = 5 * 1024 * 1024

	// Bandwidth-test concurrency limit. Download + upload share this
	// semaphore because they contend on the same POP egress. Size 2 lets
	// tests run in pairs without totally serialising during a busy pre-
	// event setup. When full, further requests get 503 with Retry-After.
	// Clients fold the retry wait out of their throughput measurement.
	bandwidthConcurrency = 2
	bandwidthRetryAfter  = 5 // seconds
)

func newHTTPHandler(cfg *config) (http.Handler, error) {
	// Pre-allocate the 10 MB download buffer once at startup — spec §3.1.
	downloadBuf := make([]byte, downloadSize)
	if _, err := rand.Read(downloadBuf); err != nil {
		return nil, err
	}

	bwSem := make(chan struct{}, bandwidthConcurrency)

	mux := http.NewServeMux()

	mux.HandleFunc("GET /preflight/ping", func(w http.ResponseWriter, r *http.Request) {})
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, r *http.Request) {})

	mux.HandleFunc("GET /preflight/mtu", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		_, _ = w.Write(make([]byte, 1400))
	})

	mux.HandleFunc("GET /preflight/echo-ts", func(w http.ResponseWriter, r *http.Request) {
		now := time.Now()
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"iso":          now.UTC().Format(time.RFC3339Nano),
			"monotonic_ns": now.UnixNano(),
		})
	})

	mux.HandleFunc("GET /preflight/download/10mb", withBandwidthSem(bwSem, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Cache-Control", "no-store")
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", "10485760")
		_, _ = w.Write(downloadBuf)
	}))

	mux.HandleFunc("POST /preflight/upload", withBandwidthSem(bwSem, func(w http.ResponseWriter, r *http.Request) {
		if r.ContentLength > uploadMaxBytes {
			http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
			return
		}
		limited := io.LimitReader(r.Body, uploadMaxBytes+1)
		n, _ := io.Copy(io.Discard, limited)
		if n > uploadMaxBytes {
			http.Error(w, "payload too large", http.StatusRequestEntityTooLarge)
			return
		}
	}))

	mux.HandleFunc("GET /preflight/whoami", func(w http.ResponseWriter, r *http.Request) {
		ip := clientIP(r)
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]string{
			"public_ip": ip,
			"pop_id":    cfg.POPID,
			"geo":       cfg.Geo,
		})
	})

	return mux, nil
}

// withBandwidthSem acquires a slot from the shared bandwidth semaphore before
// handing off to the wrapped handler. When full, replies 503 with Retry-After
// so clients can back off. Non-blocking — we never queue; the client is
// expected to retry after the hint.
func withBandwidthSem(sem chan struct{}, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		select {
		case sem <- struct{}{}:
			defer func() { <-sem }()
			next(w, r)
		default:
			w.Header().Set("Retry-After", strconv.Itoa(bandwidthRetryAfter))
			http.Error(w, "pop bandwidth tests busy, retry shortly", http.StatusServiceUnavailable)
		}
	}
}

func clientIP(r *http.Request) string {
	if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
		if idx := strings.Index(xff, ","); idx >= 0 {
			return strings.TrimSpace(xff[:idx])
		}
		return strings.TrimSpace(xff)
	}
	if xri := r.Header.Get("X-Real-IP"); xri != "" {
		return strings.TrimSpace(xri)
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

