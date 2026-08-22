package handlers

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/money"
)

// Products ports apps/mobile-api/src/products (read paths only).
// Mount: chi r.Mount("/products", handlers.NewProducts(pool)).
//
// The Prisma payload omits the Unsupported vector column entirely (it is not
// part of the generated model types), so no response carries an embedding
// key; images are embedded with orderBy position asc and every product gains
// the legacy imageUrl field derived from its first image.

// NewProducts returns the /products subtree.
func NewProducts(pool *pgxpool.Pool) http.Handler {
	r := chi.NewRouter()
	r.Get("/", productsList(pool))
	r.Get("/metrics", productsMetrics(pool))
	r.Get("/{id}", productsGet(pool))
	return r
}

type productImageJSON struct {
	ID          string  `json:"id"`
	ProductID   string  `json:"productId"`
	URL         string  `json:"url"`
	StorageKey  *string `json:"storageKey"`
	ContentHash *string `json:"contentHash"`
	Position    int32   `json:"position"`
	CreatedAt   isoTime `json:"createdAt"`
}

type productJSON struct {
	ID           string             `json:"id"`
	BusinessID   string             `json:"businessId"`
	Name         string             `json:"name"`
	Description  *string            `json:"description"`
	Category     *string            `json:"category"`
	RequestCount int32              `json:"requestCount"`
	StockNote    *string            `json:"stockNote"`
	DeliveryNote *string            `json:"deliveryNote"`
	Price        money.Number       `json:"price"`
	Stock        int32              `json:"stock"`
	SKU          *string            `json:"sku"`
	IsAvailable  bool               `json:"isAvailable"`
	CreatedAt    isoTime            `json:"createdAt"`
	UpdatedAt    isoTime            `json:"updatedAt"`
	Images       []productImageJSON `json:"images"`
	ImageURL     *string            `json:"imageUrl"`
}

func ep_productImages(rows []gen.ListProductImagesForProductsRow) []productImageJSON {
	out := make([]productImageJSON, 0, len(rows))
	for _, row := range rows {
		out = append(out, productImageJSON{
			ID:          row.ID,
			ProductID:   row.ProductID,
			URL:         row.Url,
			StorageKey:  ep_text(row.StorageKey),
			ContentHash: ep_text(row.ContentHash),
			Position:    row.Position,
			CreatedAt:   epISO(row.CreatedAt.Time),
		})
	}
	return out
}

// productsList ports ProductsService.findAll (products.service.ts:140-164):
// findMany {where businessId, include images(position asc), orderBy createdAt
// desc} + count, each row mapped through withLegacyImageUrl.
func productsList(pool *pgxpool.Pool) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}
		pq, ok := ep_page(w, r)
		if !ok {
			return
		}

		rows, err := q.ListProductsByBusiness(r.Context(), gen.ListProductsByBusinessParams{
			BusinessID: bizID,
			Limit:      int32(pq.Limit),
			Offset:     int32((pq.Page - 1) * pq.Limit),
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		total, err := q.CountProductsByBusiness(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		data := make([]productJSON, 0, len(rows))
		if len(rows) > 0 {
			ids := make([]string, 0, len(rows))
			for _, row := range rows {
				ids = append(ids, row.ID)
			}
			imgRows, err := q.ListProductImagesForProducts(r.Context(), ids)
			if err != nil {
				httpx.WriteError(w, r, err)
				return
			}
			byProduct := make(map[string][]gen.ListProductImagesForProductsRow, len(rows))
			for _, img := range imgRows {
				byProduct[img.ProductID] = append(byProduct[img.ProductID], img)
			}

			for _, row := range rows {
				pj := productJSON{
					ID:           row.ID,
					BusinessID:   row.BusinessID,
					Name:         row.Name,
					Description:  ep_text(row.Description),
					Category:     ep_text(row.Category),
					RequestCount: row.RequestCount,
					StockNote:    ep_text(row.StockNote),
					DeliveryNote: ep_text(row.DeliveryNote),
					Price:        ep_num(row.Price),
					Stock:        row.Stock,
					SKU:          ep_text(row.Sku),
					IsAvailable:  row.IsAvailable,
					CreatedAt:    epISO(row.CreatedAt.Time),
					UpdatedAt:    epISO(row.UpdatedAt.Time),
				}
				pj.Images = ep_productImages(byProduct[row.ID])
				if len(pj.Images) > 0 {
					url := pj.Images[0].URL
					pj.ImageURL = &url
				}
				data = append(data, pj)
			}
		}
		_ = httpx.WritePaginated(w, http.StatusOK, data, int(total), pq.Page, pq.Limit)
	}
}

// productsGet ports ProductsService.findOne (products.service.ts:166-177).
func productsGet(pool *pgxpool.Pool) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		row, err := q.GetProductByIDAndBusiness(r.Context(), gen.GetProductByIDAndBusinessParams{
			ID:         chi.URLParam(r, "id"),
			BusinessID: bizID,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusNotFound, "Product not found"))
			return
		}
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		imgRows, err := q.ListProductImagesForProducts(r.Context(), []string{row.ID})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		pj := productJSON{
			ID:           row.ID,
			BusinessID:   row.BusinessID,
			Name:         row.Name,
			Description:  ep_text(row.Description),
			Category:     ep_text(row.Category),
			RequestCount: row.RequestCount,
			StockNote:    ep_text(row.StockNote),
			DeliveryNote: ep_text(row.DeliveryNote),
			Price:        ep_num(row.Price),
			Stock:        row.Stock,
			SKU:          ep_text(row.Sku),
			IsAvailable:  row.IsAvailable,
			CreatedAt:    epISO(row.CreatedAt.Time),
			UpdatedAt:    epISO(row.UpdatedAt.Time),
		}
		pj.Images = ep_productImages(imgRows)
		if len(pj.Images) > 0 {
			url := pj.Images[0].URL
			pj.ImageURL = &url
		}
		_ = httpx.WriteJSON(w, http.StatusOK, pj)
	}
}

// productsMetrics ports ProductsService.getMetrics (products.service.ts:449-476):
// total count, stock<=0 count, summed stock units, and the raw-SQL
// SUM(price*stock) inventory value (Number(...) || 0).
func productsMetrics(pool *pgxpool.Pool) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		total, err := q.CountProductsByBusiness(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		outOfStock, err := q.CountProductsOutOfStock(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		units, err := q.SumProductStockUnits(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		value, err := q.SumInventoryValue(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		_ = httpx.WriteJSON(w, http.StatusOK, struct {
			TotalProducts       int          `json:"totalProducts"`
			OutOfStock          int          `json:"outOfStock"`
			TotalInventoryItems int64        `json:"totalInventoryItems"`
			TotalInventoryValue money.Number `json:"totalInventoryValue"`
		}{
			TotalProducts:       int(total),
			OutOfStock:          int(outOfStock),
			TotalInventoryItems: units,
			TotalInventoryValue: ep_num(value),
		})
	}
}
