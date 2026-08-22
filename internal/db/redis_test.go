package db

import (
	"context"
	"crypto/tls"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"
)

func dlr_dockerUp(t *testing.T) {
	t.Helper()

	sock := os.Getenv("DOCKER_HOST")
	if sock == "" || !strings.HasPrefix(sock, "unix://") {
		sock = "unix:///var/run/docker.sock"
	}
	path := strings.TrimPrefix(sock, "unix://")

	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", path)
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/_ping", nil)
	if err != nil {
		t.Skipf("docker ping request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Skipf("docker daemon unavailable at %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
}

func TestDlrRedisOptionsMapping(t *testing.T) {
	tests := []struct {
		name       string
		cfg        RedisConfig
		wantAddr   string
		wantPass   string
		wantTLS    bool
		wantServer string
	}{
		{
			name:     "defaults localhost 6379 no tls",
			cfg:      RedisConfig{},
			wantAddr: "localhost:6379",
		},
		{
			name:       "plain host with explicit tls flag",
			cfg:        RedisConfig{Host: "redis.example.com", Port: 6380, Password: "dlp_secret", TLS: true},
			wantAddr:   "redis.example.com:6380",
			wantPass:   "dlp_secret",
			wantTLS:    true,
			wantServer: "redis.example.com",
		},
		{
			name:       "upstash host forces tls without flag",
			cfg:        RedisConfig{Host: "global-lively-sheep-1234.upstash.io", Port: 6379, Password: "upstash-token"},
			wantAddr:   "global-lively-sheep-1234.upstash.io:6379",
			wantPass:   "upstash-token",
			wantTLS:    true,
			wantServer: "global-lively-sheep-1234.upstash.io",
		},
		{
			name:       "upstash substring anywhere in host forces tls",
			cfg:        RedisConfig{Host: "cache.upstash.io.internal", Port: 6379},
			wantAddr:   "cache.upstash.io.internal:6379",
			wantTLS:    true,
			wantServer: "cache.upstash.io.internal",
		},
		{
			name:     "host merely containing upstash without marker stays plaintext",
			cfg:      RedisConfig{Host: "upstashio.local", Port: 6379},
			wantAddr: "upstashio.local:6379",
		},
		{
			name:     "empty password means no auth",
			cfg:      RedisConfig{Host: "127.0.0.1", Port: 6379},
			wantAddr: "127.0.0.1:6379",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opts, err := buildRedisOptions(tt.cfg)
			if err != nil {
				t.Fatalf("buildRedisOptions: %v", err)
			}
			if opts.Addr != tt.wantAddr {
				t.Errorf("Addr = %q, want %q", opts.Addr, tt.wantAddr)
			}
			if opts.Password != tt.wantPass {
				t.Errorf("Password = %q, want %q", opts.Password, tt.wantPass)
			}
			if tt.wantTLS && opts.TLSConfig == nil {
				t.Fatal("TLSConfig is nil, want TLS enabled")
			}
			if !tt.wantTLS && opts.TLSConfig != nil {
				t.Fatalf("TLSConfig = %+v, want nil", opts.TLSConfig)
			}
			if tt.wantTLS && opts.TLSConfig.ServerName != tt.wantServer {
				t.Errorf("TLSConfig.ServerName = %q, want %q", opts.TLSConfig.ServerName, tt.wantServer)
			}
			if tt.wantTLS && opts.TLSConfig.MinVersion < tls.VersionTLS12 {
				t.Errorf("TLSConfig.MinVersion = %#x, want >= TLS 1.2", opts.TLSConfig.MinVersion)
			}
		})
	}
}

func TestDlrRedisOptionsErrors(t *testing.T) {
	tests := []struct {
		name string
		cfg  RedisConfig
	}{
		{name: "negative port", cfg: RedisConfig{Host: "127.0.0.1", Port: -1}},
		{name: "port above range", cfg: RedisConfig{Host: "127.0.0.1", Port: 65536}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := buildRedisOptions(tt.cfg); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestDlrRedisErrorPath(t *testing.T) {
	client, err := NewRedis(RedisConfig{Host: "127.0.0.1", Port: 1})
	if err != nil {
		t.Fatalf("NewRedis must not dial eagerly, got error: %v", err)
	}
	defer func() { _ = client.Close() }()

	if opts := client.Options(); opts.Addr != "127.0.0.1:1" {
		t.Errorf("Options().Addr = %q, want %q", opts.Addr, "127.0.0.1:1")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err == nil {
		t.Fatal("expected Ping error against closed port, got nil")
	}
}

func TestDlrRedisLivePing(t *testing.T) {
	addr := os.Getenv("TEST_REDIS_ADDR")
	if addr == "" {
		t.Skip("TEST_REDIS_ADDR not set; skipping live redis ping")
	}
	dlr_dockerUp(t)

	host, portStr, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatalf("parse TEST_REDIS_ADDR %q: %v", addr, err)
	}
	port, err := strconv.Atoi(portStr)
	if err != nil {
		t.Fatalf("parse TEST_REDIS_PORT %q: %v", portStr, err)
	}

	cfg := RedisConfig{Host: host, Port: port}
	if pass := os.Getenv("TEST_REDIS_PASSWORD"); pass != "" {
		cfg.Password = pass
	}

	client, err := NewRedis(cfg)
	if err != nil {
		t.Fatalf("NewRedis(%s): %v", addr, err)
	}
	defer func() { _ = client.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := client.Ping(ctx).Err(); err != nil {
		t.Fatalf("Ping: %v", err)
	}
}
