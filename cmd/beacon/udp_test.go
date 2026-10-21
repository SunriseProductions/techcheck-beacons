package main

import (
	"bytes"
	"context"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func startUDP(t *testing.T, cfg *config) (*udpEcho, string) {
	t.Helper()
	return startUDPWith(t, cfg, newBandwidthSem(bandwidthConcurrency), nil)
}

func startUDPWith(t *testing.T, cfg *config, sem bandwidthSem, key []byte) (*udpEcho, string) {
	t.Helper()
	u, err := newUDPEcho(cfg, "127.0.0.1:0", sem, newStreamOwners(), key)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	go u.run(ctx)
	return u, u.conn.LocalAddr().String()
}

func TestUDPEchoesMatchingPrefix(t *testing.T) {
	cfg := &config{UDPMagic: "PFLT", UDPRateLimit: 100}
	_, addr := startUDP(t, cfg)

	conn, err := net.Dial("udp", addr)
	require.NoError(t, err)
	defer conn.Close()

	payload := append([]byte("PFLT\x00"), []byte("12345678")...)
	_, err = conn.Write(payload)
	require.NoError(t, err)

	buf := make([]byte, 1500)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	n, err := conn.Read(buf)
	require.NoError(t, err)
	assert.Equal(t, payload, buf[:n])
}

func TestUDPDropsNonMatchingPrefix(t *testing.T) {
	cfg := &config{UDPMagic: "PFLT", UDPRateLimit: 100}
	_, addr := startUDP(t, cfg)

	conn, err := net.Dial("udp", addr)
	require.NoError(t, err)
	defer conn.Close()

	_, err = conn.Write([]byte("HELLO\x00nonsense"))
	require.NoError(t, err)

	buf := make([]byte, 1500)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(200*time.Millisecond)))
	_, err = conn.Read(buf)
	assert.Error(t, err, "expected read timeout — datagram should have been dropped")
}

func TestUDPRateLimitPerSource(t *testing.T) {
	cfg := &config{UDPMagic: "PFLT", UDPRateLimit: 3}
	_, addr := startUDP(t, cfg)

	conn, err := net.Dial("udp", addr)
	require.NoError(t, err)
	defer conn.Close()

	payload := append([]byte("PFLT\x00"), bytes.Repeat([]byte("a"), 8)...)
	for i := 0; i < 20; i++ {
		_, _ = conn.Write(payload)
	}

	replies := 0
	deadline := time.Now().Add(300 * time.Millisecond)
	buf := make([]byte, 1500)
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(50 * time.Millisecond))
		if _, err := conn.Read(buf); err != nil {
			break
		}
		replies++
	}
	assert.LessOrEqual(t, replies, 6, "rate limiter should cap replies near bucket size, got %d", replies)
}
