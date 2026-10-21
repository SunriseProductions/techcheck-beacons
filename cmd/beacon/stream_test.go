package main

import (
	"bytes"
	"context"
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var testKey = bytes.Repeat([]byte{0x42}, 32)

func streamCfg() *config {
	return &config{UDPMagic: "PFLT", UDPRateLimit: 100}
}

// helloBody builds the body shared by H and S: ver, nonce, rate, size, duration.
func helloBody(nonce string, rateKbps uint32, packetBytes uint16, durationMs uint32) []byte {
	b := []byte{1}
	b = append(b, []byte(nonce)...)
	b = binary.BigEndian.AppendUint32(b, rateKbps)
	b = binary.BigEndian.AppendUint16(b, packetBytes)
	b = binary.BigEndian.AppendUint32(b, durationMs)
	return b
}

func helloPkt(body []byte) []byte {
	return append([]byte("PFLTH"), body...)
}

func startPkt(body, cookie []byte) []byte {
	pkt := append([]byte("PFLTS"), body...)
	return append(pkt, cookie...)
}

func dialUDP(t *testing.T, addr string) *net.UDPConn {
	t.Helper()
	ua, err := net.ResolveUDPAddr("udp", addr)
	require.NoError(t, err)
	conn, err := net.DialUDP("udp", nil, ua)
	require.NoError(t, err)
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// requestCookie sends a hello and returns the cookie from the C reply.
func requestCookie(t *testing.T, conn *net.UDPConn, body []byte) []byte {
	t.Helper()
	_, err := conn.Write(helloPkt(body))
	require.NoError(t, err)
	buf := make([]byte, 1500)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	n, err := conn.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 4+1+8+16, n)
	require.Equal(t, "PFLTC", string(buf[:5]))
	require.Equal(t, body[1:9], buf[5:13], "C reply must carry the hello nonce")
	return append([]byte(nil), buf[13:29]...)
}

// expectSilence asserts that nothing arrives on conn within d.
func expectSilence(t *testing.T, conn *net.UDPConn, d time.Duration) {
	t.Helper()
	buf := make([]byte, 1500)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(d)))
	n, err := conn.Read(buf)
	assert.Error(t, err, "expected no reply, got %q", buf[:n])
}

func TestStreamHandshakeAndData(t *testing.T) {
	sem := newBandwidthSem(bandwidthConcurrency)
	_, addr := startUDPWith(t, streamCfg(), sem, testKey)
	conn := dialUDP(t, addr)
	require.NoError(t, conn.SetReadBuffer(1<<20))

	body := helloBody("nonce-01", 2000, 1250, 500)
	cookie := requestCookie(t, conn, body)
	_, err := conn.Write(startPkt(body, cookie))
	require.NoError(t, err)

	buf := make([]byte, 1500)
	var received uint32
	var totalSent uint32
	gotEnd := false
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		require.NoError(t, conn.SetReadDeadline(deadline))
		n, err := conn.Read(buf)
		require.NoError(t, err)
		require.Equal(t, "PFLT", string(buf[:4]))
		require.Equal(t, "nonce-01", string(buf[5:13]))
		if buf[4] == 'E' {
			require.Equal(t, 4+1+8+4, n)
			totalSent = binary.BigEndian.Uint32(buf[13:17])
			gotEnd = true
			break
		}
		require.Equal(t, byte('D'), buf[4])
		require.Equal(t, 1250, n, "every data packet is padded to packet_bytes")
		assert.Equal(t, received, binary.BigEndian.Uint32(buf[13:17]), "seq must be consecutive from 0")
		sentNs := int64(binary.BigEndian.Uint64(buf[17:25]))
		assert.InDelta(t, time.Now().UnixNano(), sentNs, float64(2*time.Second))
		assert.True(t, bytes.Equal(buf[25:n], make([]byte, n-25)), "padding must be zero")
		received++
	}
	require.True(t, gotEnd, "expected an E packet")
	assert.Equal(t, uint32(100), totalSent, "2 Mbps of 1250-byte packets for 500 ms is 100 packets")
	assert.Equal(t, totalSent, received)
	assert.Eventually(t, func() bool { return len(sem) == 0 }, time.Second, 10*time.Millisecond,
		"stream must release its semaphore slot")
}

func TestStreamStartWithBadCookieIgnored(t *testing.T) {
	sem := newBandwidthSem(bandwidthConcurrency)
	_, addr := startUDPWith(t, streamCfg(), sem, testKey)
	conn := dialUDP(t, addr)

	body := helloBody("nonce-02", 2000, 1250, 500)
	_, err := conn.Write(startPkt(body, make([]byte, 16)))
	require.NoError(t, err)

	expectSilence(t, conn, 300*time.Millisecond)
	assert.Equal(t, 0, len(sem), "a bad cookie must not take a slot")
}

func TestStreamCookieBoundToSourcePort(t *testing.T) {
	_, addr := startUDPWith(t, streamCfg(), newBandwidthSem(bandwidthConcurrency), testKey)
	connA := dialUDP(t, addr)
	connB := dialUDP(t, addr)

	body := helloBody("nonce-03", 2000, 1250, 500)
	cookie := requestCookie(t, connA, body)
	_, err := connB.Write(startPkt(body, cookie))
	require.NoError(t, err)

	expectSilence(t, connB, 300*time.Millisecond)
	expectSilence(t, connA, 50*time.Millisecond)
}

func TestStreamCookieRejectsChangedBody(t *testing.T) {
	_, addr := startUDPWith(t, streamCfg(), newBandwidthSem(bandwidthConcurrency), testKey)
	conn := dialUDP(t, addr)

	cookie := requestCookie(t, conn, helloBody("nonce-04", 2000, 1250, 500))
	_, err := conn.Write(startPkt(helloBody("nonce-04", 40000, 1250, 15000), cookie))
	require.NoError(t, err)

	expectSilence(t, conn, 300*time.Millisecond)
}

func TestStreamCookieExpires(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("203.0.113.7"), Port: 50000}
	body := helloBody("nonce-05", 2000, 1250, 500)
	now := time.Now()
	window := cookieWindow(now)

	for _, tc := range []struct {
		window int64
		ok     bool
	}{
		{window, true},
		{window - 1, true},
		{window - 2, false},
		{window + 1, false},
	} {
		c := makeCookie(testKey, src, body, tc.window)
		assert.Equal(t, tc.ok, verifyCookie(testKey, src, body, c[:], now), "window offset %d", tc.window-window)
	}
}

func TestStreamClampsRequest(t *testing.T) {
	p, ok := parseHello(helloBody("nonce-06", 100000, 9000, 60000))
	require.True(t, ok)
	assert.Equal(t, [8]byte([]byte("nonce-06")), p.nonce)
	assert.Equal(t, uint32(40000), p.rateKbps)
	assert.Equal(t, 15*time.Second, p.duration)
	assert.Equal(t, 1400, p.packetBytes)

	p, ok = parseHello(helloBody("nonce-06", 5000, 10, 3000))
	require.True(t, ok)
	assert.Equal(t, 200, p.packetBytes)

	_, ok = parseHello(helloBody("nonce-06", 0, 1250, 3000))
	assert.False(t, ok, "rate 0 gets no stream")
	_, ok = parseHello(helloBody("nonce-06", 5000, 1250, 0))
	assert.False(t, ok, "duration 0 gets no stream")
}

func TestStreamBusyWhenSemFull(t *testing.T) {
	sem := newBandwidthSem(bandwidthConcurrency)
	require.True(t, sem.tryAcquire())
	require.True(t, sem.tryAcquire())
	_, addr := startUDPWith(t, streamCfg(), sem, testKey)
	conn := dialUDP(t, addr)

	body := helloBody("nonce-07", 2000, 1250, 500)
	cookie := requestCookie(t, conn, body)
	_, err := conn.Write(startPkt(body, cookie))
	require.NoError(t, err)

	buf := make([]byte, 1500)
	require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
	n, err := conn.Read(buf)
	require.NoError(t, err)
	require.Equal(t, 4+1+8+2, n)
	assert.Equal(t, "PFLTB", string(buf[:5]))
	assert.Equal(t, "nonce-07", string(buf[5:13]))
	assert.Equal(t, uint16(bandwidthRetryAfter), binary.BigEndian.Uint16(buf[13:15]))
	expectSilence(t, conn, 100*time.Millisecond)
}

func TestStreamReleasesSlotOnShutdown(t *testing.T) {
	sem := newBandwidthSem(bandwidthConcurrency)
	u, err := newUDPEcho(streamCfg(), "127.0.0.1:0", sem, testKey)
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go u.run(ctx)
	conn := dialUDP(t, u.conn.LocalAddr().String())

	body := helloBody("nonce-08", 2000, 1250, 10000)
	cookie := requestCookie(t, conn, body)
	_, err = conn.Write(startPkt(body, cookie))
	require.NoError(t, err)
	require.Eventually(t, func() bool { return len(sem) == 1 }, time.Second, 5*time.Millisecond)

	cancel()
	assert.Eventually(t, func() bool { return len(sem) == 0 }, time.Second, 5*time.Millisecond,
		"shutdown must release the stream's slot")
}

func TestEchoStillWorksOnBothKinds(t *testing.T) {
	_, addr := startUDPWith(t, streamCfg(), newBandwidthSem(bandwidthConcurrency), testKey)
	conn := dialUDP(t, addr)

	buf := make([]byte, 1500)
	for _, payload := range [][]byte{
		append([]byte("PFLT\x00"), []byte("12345678")...),
		[]byte("PFLTlegacy-probe"),
	} {
		_, err := conn.Write(payload)
		require.NoError(t, err)
		require.NoError(t, conn.SetReadDeadline(time.Now().Add(time.Second)))
		n, err := conn.Read(buf)
		require.NoError(t, err)
		assert.Equal(t, payload, buf[:n])
	}
}
