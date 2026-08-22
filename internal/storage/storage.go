// Package storage ports libs/common/src/storage (T4.8): the provider
// abstraction the vendor write surface depends on, plus the embedding-queue
// seam (T4.1 hook points).
//
// SOURCE shape: StorageProvider.uploadImage(file, folder?) -> UploadedImage
// {url, storageKey} and deleteImage(storageKey) — best-effort by contract
// (implementations must resolve rather than throw when an asset is already
// gone, so cleanup can never fail a request over an orphaned remote asset).
package storage

import (
	"context"
	"log/slog"

	"github.com/novoapex/novoapex-backend-api/internal/config"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/cloudinary"
)

// StorageProvider mirrors libs/common/src/storage/storage-provider.interface.ts.
// The Go signature flattens UploadedImage into (url, publicID): publicID is
// the provider-specific delete key (Cloudinary public_id), called storageKey
// on the Node side and in every product_images.storage_key column.
type StorageProvider interface {
	// ProviderName identifies the backing implementation ('cloudinary', ...).
	ProviderName() string

	// UploadImage uploads raw image bytes under folder (e.g.
	// 'novoapex/products/<businessId>') and returns the public URL plus the
	// key required to delete the asset later. Failures are errors; callers
	// compensate by deleting whatever sibling uploads succeeded.
	UploadImage(ctx context.Context, file []byte, folder string) (url string, publicID string, err error)

	// DeleteImage removes a previously uploaded asset. Best-effort: an
	// already-gone or unreachable asset is logged, not returned as an error,
	// mirroring CloudinaryProvider.deleteImage which swallows everything.
	DeleteImage(ctx context.Context, publicID string) error
}

// NewDefaultProvider ports StorageProviderFactory.getDefaultProvider
// (storage-provider.factory.ts): only cloudinary is registered today.
func NewDefaultProvider(cfg *config.Config) StorageProvider {
	return cloudinary.New(cloudinary.Options{
		CloudName: cfg.CloudinaryCloudName,
		APIKey:    cfg.CloudinaryAPIKey,
		APISecret: cfg.CloudinaryAPISecret,
	})
}

// EmbedPublisher is the T4.1 bridge seam standing in for BullMQ's
// EMBEDDING_QUEUE. The real enqueue decision (asynq vs defer to Stage 6) is
// central planning's call; these are the exact hook points the Node service
// enqueues at:
//
//   - PublishProductEmbed  <- queue.add('embed-product', { productId })
//     after product create AND after every product update (including
//     markOutOfStock, which routes through update).
//   - PublishImageEmbed    <- queue.addBulk([{name:'embed-product-image',
//     data:{productImageId}, ...}]) once per inserted product_images row.
//
// Deletion, duplicate-skip uploads and image remove/reorder enqueue nothing.
type EmbedPublisher interface {
	PublishProductEmbed(ctx context.Context, productID string) error
	PublishImageEmbed(ctx context.Context, imageID string) error
}

// NullEmbedder is the default EmbedPublisher: it logs what WOULD have been
// enqueued (job name + payload) and never fails the mutation — exactly how
// the source treats enqueue failures ("logged but never block").
type NullEmbedder struct{}

func (NullEmbedder) PublishProductEmbed(ctx context.Context, productID string) error {
	slog.WarnContext(ctx, "embedding queue not configured; dropping embed-product job",
		"job", "embed-product", "productId", productID)
	return nil
}

func (NullEmbedder) PublishImageEmbed(ctx context.Context, imageID string) error {
	slog.WarnContext(ctx, "embedding queue not configured; dropping embed-product-image job",
		"job", "embed-product-image", "productImageId", imageID)
	return nil
}
