package handlers

import (
	"context"
	"errors"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// BusinessIDByOwnerPhone is the auth.BusinessLookup backing JwtStrategy's
// per-request derivation: business.findUnique({where:{ownerPhone}}).
func BusinessIDByOwnerPhone(pool *pgxpool.Pool) auth.BusinessLookup {
	return func(ctx context.Context, phone string) (string, error) {
		var id string
		err := pool.QueryRow(ctx, `SELECT id FROM businesses WHERE owner_phone = $1`, phone).Scan(&id)
		if errors.Is(err, pgx.ErrNoRows) {
			return "", nil
		}
		return id, err
	}
}

// QueueEmbedPublisher is the asynq-backed storage.EmbedPublisher
// (ProductsService.enqueueEmbedding / enqueueImageEmbeddings). Retries and
// backoff come from the embedding queue policy (attempts 3, exponential 2s).
type QueueEmbedPublisher struct {
	Pub queue.Publisher
}

func (p QueueEmbedPublisher) PublishProductEmbed(ctx context.Context, productID string) error {
	return p.Pub.Enqueue(ctx, queue.QEmbedding, queue.TaskEmbedProduct,
		map[string]string{"productId": productID}, nil)
}

func (p QueueEmbedPublisher) PublishImageEmbed(ctx context.Context, imageID string) error {
	return p.Pub.Enqueue(ctx, queue.QEmbedding, queue.TaskEmbedProductImage,
		map[string]string{"productImageId": imageID}, nil)
}
