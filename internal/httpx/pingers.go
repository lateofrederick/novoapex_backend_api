package httpx

import (
	"context"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/redis/go-redis/v9"
)

// pgPinger adapts *pgxpool.Pool to Pinger. It mirrors
// PrismaHealthIndicator's lightweight `SELECT 1` probe.
type pgPinger struct{ pool *pgxpool.Pool }

func (p pgPinger) Ping(ctx context.Context) error {
	_, err := p.pool.Exec(ctx, "SELECT 1")
	return err
}

// redisPinger adapts *redis.Client to Pinger, mirroring
// RedisHealthIndicator's connect()+ping().
type redisPinger struct{ client *redis.Client }

func (r redisPinger) Ping(ctx context.Context) error {
	return r.client.Ping(ctx).Err()
}

func NewPGPinger(pool *pgxpool.Pool) Pinger {
	if pool == nil {
		return nil
	}
	return pgPinger{pool: pool}
}

func NewRedisPinger(client *redis.Client) Pinger {
	if client == nil {
		return nil
	}
	return redisPinger{client: client}
}
