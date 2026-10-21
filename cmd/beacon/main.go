package main

import (
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func main() {
	configPath := flag.String("config", "/etc/beacon/config.json", "path to JSON config file")
	flag.Parse()

	cfg, err := loadConfig(*configPath)
	if err != nil {
		logJSON("error", "config load failed", map[string]any{"err": err.Error()})
		os.Exit(1)
	}
	logJSON("info", "beacon starting", map[string]any{
		"pop_id":    cfg.POPID,
		"geo":       cfg.Geo,
		"http_addr": cfg.HTTPAddr,
		"udp_addrs": cfg.UDPAddrs,
	})

	sem := newBandwidthSem(bandwidthConcurrency)
	// Reserved for the UDP stream service (signs and verifies tokens).
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		logJSON("error", "key generation failed", map[string]any{"err": err.Error()})
		os.Exit(1)
	}

	handler, err := newHTTPHandler(cfg, sem)
	if err != nil {
		logJSON("error", "http handler init failed", map[string]any{"err": err.Error()})
		os.Exit(1)
	}

	httpSrv := &http.Server{
		Addr:              cfg.HTTPAddr,
		Handler:           handler,
		ReadHeaderTimeout: 5 * time.Second,
	}

	var udps []*udpEcho
	for _, addr := range cfg.UDPAddrs {
		u, err := newUDPEcho(cfg, addr, sem, key)
		if err != nil {
			logJSON("error", "udp listen failed", map[string]any{"addr": addr, "err": err.Error()})
			os.Exit(1)
		}
		udps = append(udps, u)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logJSON("error", "http serve failed", map[string]any{"err": err.Error()})
		}
	}()
	for _, u := range udps {
		go u.run(ctx)
	}

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM, syscall.SIGHUP)
	for {
		s := <-sig
		switch s {
		case syscall.SIGHUP:
			logJSON("info", "SIGHUP received — reserved for future use, ignored", nil)
		default:
			logJSON("info", "shutdown requested", map[string]any{"signal": s.String()})
			shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
			_ = httpSrv.Shutdown(shutdownCtx)
			shutdownCancel()
			cancel() // closes UDP
			logJSON("info", "beacon stopped", nil)
			return
		}
	}
}

func logJSON(level, msg string, fields map[string]any) {
	entry := map[string]any{
		"ts":    time.Now().UTC().Format(time.RFC3339Nano),
		"level": level,
		"msg":   msg,
	}
	for k, v := range fields {
		entry[k] = v
	}
	b, err := json.Marshal(entry)
	if err != nil {
		log.Printf("log marshal: %v", err)
		return
	}
	os.Stdout.Write(append(b, '\n'))
}
