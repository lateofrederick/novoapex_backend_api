package db

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

const dlpTestDSN = "postgres://dlp_user:dlp_pass@127.0.0.1:5432/dlp_testdb?sslmode=disable"

func dlp_dockerUp(t *testing.T) {
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

func TestDlpPoolConfigMapping(t *testing.T) {
	tests := []struct {
		name         string
		cfg          PoolConfig
		wantMaxConns int32
		wantIdle     time.Duration
		wantConn     time.Duration
	}{
		{
			name:         "overrides applied",
			cfg:          PoolConfig{MaxConns: 42, IdleTimeout: 30 * time.Second, ConnTimeout: 10 * time.Second},
			wantMaxConns: 42,
			wantIdle:     30 * time.Second,
			wantConn:     10 * time.Second,
		},
		{
			name: "zeros keep DSN defaults",
			cfg:  PoolConfig{},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			baseline, err := pgxpool.ParseConfig(dlpTestDSN)
			if err != nil {
				t.Fatalf("parse baseline config: %v", err)
			}
			got, err := buildPoolConfig(dlpTestDSN, tt.cfg)
			if err != nil {
				t.Fatalf("buildPoolConfig: %v", err)
			}

			wantMax := tt.wantMaxConns
			if wantMax == 0 {
				wantMax = baseline.MaxConns
			}
			if got.MaxConns != wantMax {
				t.Errorf("MaxConns = %d, want %d", got.MaxConns, wantMax)
			}
			wantIdle := tt.wantIdle
			if wantIdle == 0 {
				wantIdle = baseline.MaxConnIdleTime
			}
			if got.MaxConnIdleTime != wantIdle {
				t.Errorf("MaxConnIdleTime = %s, want %s", got.MaxConnIdleTime, wantIdle)
			}
			wantConn := tt.wantConn
			if wantConn == 0 {
				wantConn = baseline.ConnConfig.ConnectTimeout
			}
			if got.ConnConfig.ConnectTimeout != wantConn {
				t.Errorf("ConnectTimeout = %s, want %s", got.ConnConfig.ConnectTimeout, wantConn)
			}

			if got.ConnConfig.Host != "127.0.0.1" || got.ConnConfig.Port != 5432 || got.ConnConfig.Database != "dlp_testdb" {
				t.Errorf("conn params not parsed from DSN: host=%s port=%d db=%s",
					got.ConnConfig.Host, got.ConnConfig.Port, got.ConnConfig.Database)
			}
			if got.ConnConfig.User != "dlp_user" || got.ConnConfig.Password != "dlp_pass" {
				t.Errorf("credentials not parsed from DSN: user=%q pass=%q",
					got.ConnConfig.User, got.ConnConfig.Password)
			}
		})
	}
}

func TestDlpPoolConfigErrors(t *testing.T) {
	tests := []struct {
		name string
		url  string
		cfg  PoolConfig
	}{
		{name: "empty url", url: "", cfg: PoolConfig{}},
		{name: "invalid url", url: "not-a-dsn", cfg: PoolConfig{}},
		{name: "negative max conns", url: dlpTestDSN, cfg: PoolConfig{MaxConns: -1}},
		{name: "negative idle timeout", url: dlpTestDSN, cfg: PoolConfig{IdleTimeout: -time.Second}},
		{name: "negative conn timeout", url: dlpTestDSN, cfg: PoolConfig{ConnTimeout: -time.Second}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if _, err := buildPoolConfig(tt.url, tt.cfg); err == nil {
				t.Fatal("expected error, got nil")
			}
		})
	}
}

func TestDlpPoolNewPoolErrorPath(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := NewPool(ctx, "postgres://dlp_user@127.0.0.1:1/dlp_testdb?sslmode=disable",
		PoolConfig{ConnTimeout: 2 * time.Second})
	if err != nil {
		t.Fatalf("NewPool must not dial eagerly, got error: %v", err)
	}
	defer pool.Close()

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer pingCancel()
	if err := pool.Ping(pingCtx); err == nil {
		t.Fatal("expected Ping error against closed port, got nil")
	}
}

func TestDlpPoolLivePing(t *testing.T) {
	dsn := os.Getenv("TEST_PG_DSN")
	if dsn == "" {
		t.Skip("TEST_PG_DSN not set; skipping live postgres ping")
	}
	dlp_dockerUp(t)

	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	pool, err := NewPool(ctx, dsn, PoolConfig{MaxConns: 4, ConnTimeout: 10 * time.Second})
	if err != nil {
		t.Fatalf("NewPool(%s): %v", dsn, err)
	}
	defer pool.Close()

	if err := pool.Ping(ctx); err != nil {
		t.Fatalf("Ping: %v", err)
	}

	var one int
	if err := pool.QueryRow(ctx, "SELECT 1").Scan(&one); err != nil {
		t.Fatalf("query SELECT 1: %v", err)
	}
	if one != 1 {
		t.Errorf("SELECT 1 returned %d, want 1", one)
	}
}

func TestDlpPoolLazyNoNetworkOnCreate(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	pool, err := NewPool(ctx, fmt.Sprintf("postgres://u:p@%s/db?sslmode=disable", "256.256.256.256:5432"),
		PoolConfig{})
	if err != nil {
		t.Fatalf("NewPool with unresolvable host should still succeed lazily: %v", err)
	}
	defer pool.Close()
}
