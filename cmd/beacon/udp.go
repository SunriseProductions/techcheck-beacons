package main

import (
	"bytes"
	"context"
	"log"
	"net"
	"sync"
	"time"
)

type udpEcho struct {
	conn      *net.UDPConn
	magic     []byte
	rateLimit int
	sem       bandwidthSem
	streams   *streamOwners
	key       []byte
	// ctx is set by run and stops stream goroutines on shutdown.
	ctx context.Context

	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	tokens   float64
	lastFill time.Time
}

func newUDPEcho(cfg *config, addr string, sem bandwidthSem, streams *streamOwners, key []byte) (*udpEcho, error) {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", ua)
	if err != nil {
		return nil, err
	}
	// Room for bursts from up to two streams at the rate cap.
	if err := conn.SetWriteBuffer(4 << 20); err != nil {
		log.Printf("beacon udp: set write buffer on %s: %v", addr, err)
	}
	return &udpEcho{
		conn:      conn,
		magic:     []byte(cfg.UDPMagic),
		rateLimit: cfg.UDPRateLimit,
		sem:       sem,
		streams:   streams,
		key:       key,
		buckets:   make(map[string]*bucket),
	}, nil
}

func (u *udpEcho) run(ctx context.Context) {
	u.ctx = ctx
	go func() {
		<-ctx.Done()
		_ = u.conn.Close()
	}()
	buf := make([]byte, 1500)
	for {
		n, addr, err := u.conn.ReadFromUDP(buf)
		if err != nil {
			return
		}
		if n < len(u.magic) || !bytes.HasPrefix(buf[:n], u.magic) {
			continue
		}
		if !u.allow(addr.IP.String()) {
			continue
		}
		// The byte after the magic selects a stream message. Anything else
		// is a plain echo, which clients send as magic + 0x00 + nonce.
		if n > len(u.magic) {
			switch buf[len(u.magic)] {
			case 'H':
				u.handleHello(buf[:n], addr)
				continue
			case 'S':
				u.handleStart(buf[:n], addr)
				continue
			}
		}
		// Copy payload — ReadFromUDP reuses buf.
		payload := make([]byte, n)
		copy(payload, buf[:n])
		if _, err := u.conn.WriteToUDP(payload, addr); err != nil {
			log.Printf("beacon udp: write %s: %v", addr, err)
		}
	}
}

func (u *udpEcho) allow(srcIP string) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	now := time.Now()
	b, ok := u.buckets[srcIP]
	if !ok {
		b = &bucket{tokens: float64(u.rateLimit), lastFill: now}
		u.buckets[srcIP] = b
	}
	elapsed := now.Sub(b.lastFill).Seconds()
	b.tokens = min(float64(u.rateLimit), b.tokens+elapsed*float64(u.rateLimit))
	b.lastFill = now
	if b.tokens < 1 {
		return false
	}
	b.tokens--
	return true
}
