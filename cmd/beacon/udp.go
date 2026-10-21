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
	key       []byte

	mu      sync.Mutex
	buckets map[string]*bucket
}

type bucket struct {
	tokens   float64
	lastFill time.Time
}

func newUDPEcho(cfg *config, addr string, sem bandwidthSem, key []byte) (*udpEcho, error) {
	ua, err := net.ResolveUDPAddr("udp", addr)
	if err != nil {
		return nil, err
	}
	conn, err := net.ListenUDP("udp", ua)
	if err != nil {
		return nil, err
	}
	return &udpEcho{
		conn:      conn,
		magic:     []byte(cfg.UDPMagic),
		rateLimit: cfg.UDPRateLimit,
		sem:       sem,
		key:       key,
		buckets:   make(map[string]*bucket),
	}, nil
}

func (u *udpEcho) run(ctx context.Context) {
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
