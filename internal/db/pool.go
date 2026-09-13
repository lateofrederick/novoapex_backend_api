package db

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

type PoolConfig struct {
	MaxConns    int32
	IdleTimeout time.Duration
	ConnTimeout time.Duration
	// Tracer, when set, observes every query (Sentry DB spans).
	Tracer pgx.QueryTracer
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
	NormalizeRuntimeParams(poolCfg.ConnConfig.RuntimeParams)

	if cfg.MaxConns > 0 {
		poolCfg.MaxConns = cfg.MaxConns
	}
	if cfg.IdleTimeout > 0 {
		poolCfg.MaxConnIdleTime = cfg.IdleTimeout
	}
	if cfg.ConnTimeout > 0 {
		poolCfg.ConnConfig.ConnectTimeout = cfg.ConnTimeout
	}
	if cfg.Tracer != nil {
		poolCfg.ConnConfig.Tracer = cfg.Tracer
	}
	return poolCfg, nil
}

// NormalizeRuntimeParams adjusts the startup parameters pgx derives from the
// connection URL:
//
//   - "schema" is a Prisma-only URL option (…?schema=public). pgx forwards
//     unknown query keys to Postgres as startup parameters, which rejects them
//     with "unrecognized configuration parameter" — so Prisma-style
//     DATABASE_URLs would fail to connect. public is the default schema.
//   - Every timestamp column is TIMESTAMP(3) WITHOUT TIME ZONE holding UTC
//     wall time. Pinning the session to UTC keeps CURRENT_TIMESTAMP/now() on
//     that clock regardless of the server's TimeZone; an explicit timezone in
//     the URL still wins.
func NormalizeRuntimeParams(params map[string]string) {
	delete(params, "schema")
	if _, ok := params["timezone"]; !ok {
		params["timezone"] = "UTC"
	}
}
