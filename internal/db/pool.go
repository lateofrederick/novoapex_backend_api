package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

type PoolConfig struct {
	MaxConns    int32
	IdleTimeout time.Duration
	ConnTimeout time.Duration
}

func NewPool(ctx context.Context, databaseURL string, cfg PoolConfig) (*pgxpool.Pool, error) {
	poolCfg, err := buildPoolConfig(databaseURL, cfg)
	if err != nil {
		return nil, err
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("db: create pool: %w", err)
	}
	return pool, nil
}

func buildPoolConfig(databaseURL string, cfg PoolConfig) (*pgxpool.Config, error) {
	if databaseURL == "" {
		return nil, errors.New("db: empty database URL")
	}
	if cfg.MaxConns < 0 {
		return nil, fmt.Errorf("db: MaxConns must be positive, got %d", cfg.MaxConns)
	}
	if cfg.IdleTimeout < 0 {
		return nil, fmt.Errorf("db: IdleTimeout must be non-negative, got %s", cfg.IdleTimeout)
	}
	if cfg.ConnTimeout < 0 {
		return nil, fmt.Errorf("db: ConnTimeout must be non-negative, got %s", cfg.ConnTimeout)
	}

	poolCfg, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("db: parse database URL: %w", err)
	}

	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	if cfg.IdleTimeout > 0 {
		poolCfg.MaxConnIdleTime = cfg.IdleTimeout
	}
	if cfg.ConnTimeout > 0 {
		poolCfg.ConnConfig.ConnectTimeout = cfg.ConnTimeout
	}
	return poolCfg, nil
}
