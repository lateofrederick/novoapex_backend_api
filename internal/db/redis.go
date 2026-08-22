package db

import (
	"crypto/tls"
	"fmt"
	"net"
	"strconv"
	"strings"

	"github.com/redis/go-redis/v9"
)

const (
	DefaultRedisHost = "localhost"
	DefaultRedisPort = 6379

	upstashHostMarker = "upstash.io"
)

type RedisConfig struct {
	Host     string
	Port     int
	Password string
	TLS      bool
}

func NewRedis(cfg RedisConfig) (*redis.Client, error) {
	opts, err := buildRedisOptions(cfg)
	if err != nil {
		return nil, err
	}
	return redis.NewClient(opts), nil
}

func buildRedisOptions(cfg RedisConfig) (*redis.Options, error) {
	host := cfg.Host
	if host == "" {
		host = DefaultRedisHost
	}
	port := cfg.Port
	if port == 0 {
		port = DefaultRedisPort
	}
	if port < 1 || port > 65535 {
		return nil, fmt.Errorf("db: invalid redis port %d", cfg.Port)
	}

	useTLS := cfg.TLS || strings.Contains(host, upstashHostMarker)

	opts := &redis.Options{
		Addr: net.JoinHostPort(host, strconv.Itoa(port)),
	}
	if cfg.Password != "" {
		opts.Password = cfg.Password
	}
	if useTLS {
		opts.TLSConfig = &tls.Config{
			ServerName: host,
			MinVersion: tls.VersionTLS12,
		}
	}
	return opts, nil
}
