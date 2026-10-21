// beacon serves the /preflight/* HTTP endpoints and the UDP echo sidecar at
// a Sunrise POP. Production evolution of cmd/mockprobe.
package main

import (
	"encoding/json"
	"fmt"
	"os"
)

type config struct {
	POPID    string   `json:"pop_id"`
	Geo      string   `json:"geo"`
	HTTPAddr string   `json:"http_addr"`
	UDPAddrs []string `json:"udp_addrs"`
	// UDPAddr is the legacy single-address key. If set, it replaces the
	// first entry of UDPAddrs.
	UDPAddr      string `json:"udp_addr"`
	UDPMagic     string `json:"udp_magic"`
	UDPRateLimit int    `json:"udp_rate_limit"`
}

func loadConfig(path string) (*config, error) {
	cfg := &config{
		HTTPAddr:     "127.0.0.1:8080",
		UDPAddrs:     []string{"0.0.0.0:4172", "0.0.0.0:8443"},
		UDPMagic:     "PFLT",
		UDPRateLimit: 50,
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config %s: %w", path, err)
	}
	if err := json.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("parse config %s: %w", path, err)
	}
	if cfg.UDPAddr != "" && len(cfg.UDPAddrs) > 0 {
		cfg.UDPAddrs[0] = cfg.UDPAddr
	}
	if cfg.POPID == "" {
		return nil, fmt.Errorf("config: pop_id is required")
	}
	if cfg.Geo == "" {
		return nil, fmt.Errorf("config: geo is required")
	}
	return cfg, nil
}
