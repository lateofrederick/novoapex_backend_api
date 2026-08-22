package storage

// storage_test.go — T4.8 factory: the default provider is cloudinary, built
// from the CLOUDINARY_* env contract.

import (
	"testing"

	"github.com/novoapex/novoapex-backend-api/internal/config"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/cloudinary"
)

func TestNewDefaultProviderChoosesCloudinary(t *testing.T) {
	cfg := &config.Config{
		CloudinaryCloudName: "cn",
		CloudinaryAPIKey:    "ck",
		CloudinaryAPISecret: "cs",
	}
	p := NewDefaultProvider(cfg)
	if p == nil {
		t.Fatal("nil provider")
	}
	if p.ProviderName() != "cloudinary" {
		t.Fatalf("provider = %q, want cloudinary", p.ProviderName())
	}
}

// Compile-time assertion across the seam.
var _ StorageProvider = (*cloudinary.Provider)(nil)

func TestNullEmbedderNeverFails(t *testing.T) {
	var e EmbedPublisher = NullEmbedder{}
	if err := e.PublishProductEmbed(t.Context(), "p1"); err != nil {
		t.Fatalf("product embed: %v", err)
	}
	if err := e.PublishImageEmbed(t.Context(), "i1"); err != nil {
		t.Fatalf("image embed: %v", err)
	}
}
