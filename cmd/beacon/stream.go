package main

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"log"
	"net"
	"sync"
	"time"
)

// UDP stream test. A client asks for a stream of sequenced packets at a set
// rate, so it can measure loss, jitter and reordering. Before the beacon
// sends anything it hands out a cookie bound to the source address. Only a
// start that echoes a valid cookie gets a stream, so a spoofed source address
// cannot point a stream at a third party.
//
// Wire format, after the magic and a one-byte type (integers big-endian):
//
//	H hello  c→b  ver u8, nonce [8], rate_kbps u32, packet_bytes u16, duration_ms u32
//	C cookie b→c  nonce [8], cookie [16]
//	S start  c→b  hello body, cookie [16]
//	B busy   b→c  nonce [8], retry_after_s u16
//	D data   b→c  nonce [8], seq u32, send_unix_ns i64, zero padding to packet_bytes
//	E end    b→c  nonce [8], total_sent u32 (sent 3 times, 20 ms apart)

const (
	streamVersion = 1

	helloBodyLen = 1 + 8 + 4 + 2 + 4
	cookieLen    = 16

	maxStreamKbps        = 40000
	maxStreamDuration    = 15 * time.Second
	minStreamPacketBytes = 200
	maxStreamPacketBytes = 1400

	cookieWindowSeconds = 10
	streamTick          = 5 * time.Millisecond
	endRepeats          = 3
	endInterval         = 20 * time.Millisecond
)

// streamOwners holds the source IPs that have a stream running. Every UDP
// listener shares it, so one client gets one stream at a time, whatever its
// source port or the beacon port.
type streamOwners struct {
	mu  sync.Mutex
	ips map[string]bool
}

func newStreamOwners() *streamOwners {
	return &streamOwners{ips: make(map[string]bool)}
}

// claim records ip as streaming. It reports false when ip already is.
func (o *streamOwners) claim(ip string) bool {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.ips[ip] {
		return false
	}
	o.ips[ip] = true
	return true
}

func (o *streamOwners) drop(ip string) {
	o.mu.Lock()
	defer o.mu.Unlock()
	delete(o.ips, ip)
}

type streamParams struct {
	nonce       [8]byte
	rateKbps    uint32
	packetBytes int
	duration    time.Duration
}

// parseHello decodes a hello body and clamps it to the stream caps. It
// reports false for a malformed body or a request for no traffic.
func parseHello(body []byte) (streamParams, bool) {
	if len(body) != helloBodyLen || body[0] != streamVersion {
		return streamParams{}, false
	}
	var p streamParams
	copy(p.nonce[:], body[1:9])
	rate := binary.BigEndian.Uint32(body[9:13])
	size := int(binary.BigEndian.Uint16(body[13:15]))
	durMs := binary.BigEndian.Uint32(body[15:19])
	if rate == 0 || durMs == 0 {
		return streamParams{}, false
	}
	p.rateKbps = min(rate, maxStreamKbps)
	p.packetBytes = max(minStreamPacketBytes, min(size, maxStreamPacketBytes))
	p.duration = min(time.Duration(durMs)*time.Millisecond, maxStreamDuration)
	return p, true
}

func cookieWindow(now time.Time) int64 {
	return now.Unix() / cookieWindowSeconds
}

// makeCookie binds a hello body to the source address and a time window.
func makeCookie(key []byte, src *net.UDPAddr, hello []byte, window int64) [16]byte {
	mac := hmac.New(sha256.New, key)
	mac.Write(src.IP.To16())
	mac.Write(binary.BigEndian.AppendUint16(nil, uint16(src.Port)))
	mac.Write(hello)
	mac.Write(binary.BigEndian.AppendUint64(nil, uint64(window)))
	var c [16]byte
	copy(c[:], mac.Sum(nil))
	return c
}

// verifyCookie accepts a cookie from the current window or the one before.
func verifyCookie(key []byte, src *net.UDPAddr, hello, cookie []byte, now time.Time) bool {
	w := cookieWindow(now)
	for _, window := range []int64{w, w - 1} {
		want := makeCookie(key, src, hello, window)
		if hmac.Equal(want[:], cookie) {
			return true
		}
	}
	return false
}

func (u *udpEcho) packetHeader(typ byte, nonce []byte) []byte {
	b := make([]byte, 0, len(u.magic)+1+len(nonce))
	b = append(b, u.magic...)
	b = append(b, typ)
	return append(b, nonce...)
}

func (u *udpEcho) handleHello(pkt []byte, src *net.UDPAddr) {
	body := pkt[len(u.magic)+1:]
	if _, ok := parseHello(body); !ok {
		return
	}
	cookie := makeCookie(u.key, src, body, cookieWindow(time.Now()))
	reply := append(u.packetHeader('C', body[1:9]), cookie[:]...)
	if _, err := u.conn.WriteToUDP(reply, src); err != nil {
		log.Printf("beacon udp: write %s: %v", src, err)
	}
}

func (u *udpEcho) handleStart(pkt []byte, src *net.UDPAddr) {
	rest := pkt[len(u.magic)+1:]
	if len(rest) != helloBodyLen+cookieLen {
		return
	}
	body, cookie := rest[:helloBodyLen], rest[helloBodyLen:]
	p, ok := parseHello(body)
	if !ok || !verifyCookie(u.key, src, body, cookie, time.Now()) {
		return
	}
	// One stream per source IP, so a client cannot take both slots by
	// using two source ports.
	ip := src.IP.String()
	if !u.streams.claim(ip) {
		u.replyBusy(p, src)
		return
	}
	if !u.sem.tryAcquire() {
		u.streams.drop(ip)
		u.replyBusy(p, src)
		return
	}
	go u.stream(p, src)
}

func (u *udpEcho) replyBusy(p streamParams, src *net.UDPAddr) {
	reply := binary.BigEndian.AppendUint16(u.packetHeader('B', p.nonce[:]), bandwidthRetryAfter)
	if _, err := u.conn.WriteToUDP(reply, src); err != nil {
		log.Printf("beacon udp: write %s: %v", src, err)
	}
}

// stream sends the data packets, then the end marker. It stops early on
// shutdown or a write error. The caller has taken a semaphore slot and
// claimed the source IP; stream gives both back on every path, right after
// the last data packet, so the next stream can start while the end markers
// still go out.
func (u *udpEcho) stream(p streamParams, src *net.UDPAddr) {
	sent, ok := u.sendData(p, src)
	u.sem.release()
	u.streams.drop(src.IP.String())
	if !ok {
		return
	}

	end := binary.BigEndian.AppendUint32(u.packetHeader('E', p.nonce[:]), sent)
	for i := 0; i < endRepeats; i++ {
		if i > 0 {
			time.Sleep(endInterval)
		}
		if _, err := u.conn.WriteToUDP(end, src); err != nil {
			if !errors.Is(err, net.ErrClosed) {
				log.Printf("beacon udp: write %s: %v", src, err)
			}
			return
		}
	}
}

// sendData paces the data packets and returns how many went out. ok is false
// after a write error; shutdown stops the packets but still sends the end.
func (u *udpEcho) sendData(p streamParams, src *net.UDPAddr) (sent uint32, ok bool) {
	pktsPerSec := float64(p.rateKbps) * 1000 / 8 / float64(p.packetBytes)
	total := uint32(pktsPerSec * p.duration.Seconds())

	pkt := make([]byte, p.packetBytes)
	hdr := u.packetHeader('D', p.nonce[:])
	copy(pkt, hdr)
	seqOff := len(hdr)

	start := time.Now()
	ticker := time.NewTicker(streamTick)
	defer ticker.Stop()
	for sent < total {
		select {
		case <-u.ctx.Done():
			return sent, true
		case <-ticker.C:
		}
		due := total
		if elapsed := time.Since(start); elapsed < p.duration {
			due = uint32(pktsPerSec * elapsed.Seconds())
		}
		for ; sent < due; sent++ {
			binary.BigEndian.PutUint32(pkt[seqOff:], sent)
			binary.BigEndian.PutUint64(pkt[seqOff+4:], uint64(time.Now().UnixNano()))
			if _, err := u.conn.WriteToUDP(pkt, src); err != nil {
				if !errors.Is(err, net.ErrClosed) {
					log.Printf("beacon udp: stream to %s stopped after %d packets: %v", src, sent, err)
				}
				return sent, false
			}
		}
	}
	return sent, true
}
