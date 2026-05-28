package main

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLoadConfigRequiredFields(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"pop_id": "eu-west-2", "geo": "London"}`), 0o644))

	cfg, err := loadConfig(path)
	require.NoError(t, err)
	assert.Equal(t, "eu-west-2", cfg.POPID)
	assert.Equal(t, "London", cfg.Geo)
	assert.Equal(t, "127.0.0.1:8080", cfg.HTTPAddr)
	assert.Equal(t, "0.0.0.0:4172", cfg.UDPAddr)
	assert.Equal(t, "PFLT", cfg.UDPMagic)
	assert.Equal(t, 50, cfg.UDPRateLimit)
}

func TestLoadConfigMissingPOPID(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"geo": "London"}`), 0o644))

	_, err := loadConfig(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "pop_id")
}

func TestLoadConfigMissingGeo(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"pop_id": "eu-west-2"}`), 0o644))

	_, err := loadConfig(path)
	require.Error(t, err)
	assert.Contains(t, err.Error(), "geo")
}

func TestLoadConfigOverrides(t *testing.T) {
	cfg, err := loadConfig("testdata/config.json")
	require.NoError(t, err)
	assert.Equal(t, "127.0.0.1:19091", cfg.HTTPAddr)
	assert.Equal(t, "127.0.0.1:14172", cfg.UDPAddr)
	assert.Equal(t, "TESTMAGIC", cfg.UDPMagic)
	assert.Equal(t, 5, cfg.UDPRateLimit)
}
