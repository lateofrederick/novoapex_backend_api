package handlers

import (
	"errors"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/shopspring/decimal"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/httpx"
	"github.com/novoapex/novoapex-backend-api/internal/money"
)

// Orders ports apps/mobile-api/src/orders (read paths only).
// Mount: chi r.Mount("/orders", handlers.NewOrders(pool)).

// NewOrders returns the /orders subtree.
func NewOrders(pool *pgxpool.Pool) http.Handler {
	r := chi.NewRouter()
	r.Get("/", ordersList(pool))
	r.Get("/summary", ordersSummary(pool))
	r.Get("/{id}", ordersGet(pool))
	return r
}

type orderBaseJSON struct {
	ID             string       `json:"id"`
	BusinessID     string       `json:"businessId"`
	CustomerID     string       `json:"customerId"`
	ConversationID *string      `json:"conversationId"`
	IdempotencyKey *string      `json:"idempotencyKey"`
	Status         string       `json:"status"`
	TotalAmount    money.Number `json:"totalAmount"`
	Currency       string       `json:"currency"`
	CreatedAt      isoTime      `json:"createdAt"`
	UpdatedAt      isoTime      `json:"updatedAt"`
}

type orderListJSON struct {
	orderBaseJSON
	Customer *customerJSON `json:"customer"`
}

type orderItemJSON struct {
	ID          string           `json:"id"`
	OrderID     string           `json:"orderId"`
	ProductID   string           `json:"productId"`
	ProductName string           `json:"productName"`
	Quantity    int32            `json:"quantity"`
	UnitPrice   money.Number     `json:"unitPrice"`
	Product     productPlainJSON `json:"product"`
}

// productPlainJSON is the bare Prisma Product payload (no images include,
// no legacy imageUrl) — the shape nested under order items.
type productPlainJSON struct {
	ID           string       `json:"id"`
	BusinessID   string       `json:"businessId"`
	Name         string       `json:"name"`
	Description  *string      `json:"description"`
	Category     *string      `json:"category"`
	RequestCount int32        `json:"requestCount"`
	StockNote    *string      `json:"stockNote"`
	DeliveryNote *string      `json:"deliveryNote"`
	Price        money.Number `json:"price"`
	Stock        int32        `json:"stock"`
	SKU          *string      `json:"sku"`
	IsAvailable  bool         `json:"isAvailable"`
	CreatedAt    isoTime      `json:"createdAt"`
	UpdatedAt    isoTime      `json:"updatedAt"`
}

type orderDetailJSON struct {
	orderBaseJSON
	Customer *customerJSON   `json:"customer"`
	Items    []orderItemJSON `json:"items"`
}

func ep_customerJoined(cID, cBusinessID pgtype.Text, phone, name, channel pgtype.Text, first, last, created, updated pgtype.Timestamp) *customerJSON {
	if !cID.Valid {
		return nil
	}
	c := customerJSON{
		ID:                 cID.String,
		BusinessID:         cBusinessID.String,
		Phone:              phone.String,
		Name:               ep_text(name),
		AcquisitionChannel: ep_text(channel),
		FirstContactAt:     epISO(first.Time),
		LastContactAt:      epISO(last.Time),
		CreatedAt:          epISO(created.Time),
		UpdatedAt:          epISO(updated.Time),
	}
	return &c
}

// ordersList ports OrdersService.findAll (orders.service.ts:11-35):
// findMany {where businessId, include customer, orderBy createdAt desc} +
// count.
func ordersList(pool *pgxpool.Pool) http.HandlerFunc {
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

		rows, err := q.ListOrdersWithCustomer(r.Context(), gen.ListOrdersWithCustomerParams{
			BusinessID: bizID,
			Limit:      int32(pq.Limit),
			Offset:     int32((pq.Page - 1) * pq.Limit),
		})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		total, err := q.CountOrdersByBusiness(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		data := make([]orderListJSON, 0, len(rows))
		for _, row := range rows {
			data = append(data, orderListJSON{
				orderBaseJSON: ep_orderBase(row.ID, row.BusinessID, row.CustomerID, row.ConversationID, row.IdempotencyKey, row.Status, row.TotalAmount, row.Currency, row.CreatedAt, row.UpdatedAt),
				Customer: ep_customerJoined(row.CID, row.CBusinessID, row.CPhone, row.CName, row.CAcquisitionChannel,
					row.CFirstContactAt, row.CLastContactAt, row.CCreatedAt, row.CUpdatedAt),
			})
		}
		_ = httpx.WritePaginated(w, http.StatusOK, data, int(total), pq.Page, pq.Limit)
	}
}

func ep_orderBase(id, businessID, customerID string, conversationID, idempotencyKey pgtype.Text, status gen.OrderStatus, totalAmount decimal.Decimal, currency string, createdAt, updatedAt pgtype.Timestamp) orderBaseJSON {
	return orderBaseJSON{
		ID:             id,
		BusinessID:     businessID,
		CustomerID:     customerID,
		ConversationID: ep_text(conversationID),
		IdempotencyKey: ep_text(idempotencyKey),
		Status:         string(status),
		TotalAmount:    ep_num(totalAmount),
		Currency:       currency,
		CreatedAt:      epISO(createdAt.Time),
		UpdatedAt:      epISO(updatedAt.Time),
	}
}

// ordersSummary ports OrdersService.getSummary (orders.service.ts:57-74).
func ordersSummary(pool *pgxpool.Pool) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		total, err := q.CountOrdersByBusiness(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		pending, err := q.CountOrdersByStatus(r.Context(), gen.CountOrdersByStatusParams{BusinessID: bizID, Status: gen.OrderStatusPENDING})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		paid, err := q.CountOrdersByStatus(r.Context(), gen.CountOrdersByStatusParams{BusinessID: bizID, Status: gen.OrderStatusPAID})
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		revenue, err := q.SumOrderTotalsWherePaid(r.Context(), bizID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		_ = httpx.WriteJSON(w, http.StatusOK, struct {
			TotalOrders   int          `json:"totalOrders"`
			PendingOrders int          `json:"pendingOrders"`
			PaidOrders    int          `json:"paidOrders"`
			TotalRevenue  money.Number `json:"totalRevenue"`
		}{
			TotalOrders:   int(total),
			PendingOrders: int(pending),
			PaidOrders:    int(paid),
			TotalRevenue:  ep_num(revenue),
		})
	}
}

// ordersGet ports OrdersService.findOne (orders.service.ts:37-55): findFirst
// {id, businessId} include customer + items.product; a miss throws
// NotFoundException('Order not found').
func ordersGet(pool *pgxpool.Pool) http.HandlerFunc {
	q := gen.New(pool)
	return func(w http.ResponseWriter, r *http.Request) {
		bizID, ok := ep_businessID(w, r)
		if !ok {
			return
		}

		row, err := q.GetOrderByIDAndBusiness(r.Context(), gen.GetOrderByIDAndBusinessParams{
			ID:         chi.URLParam(r, "id"),
			BusinessID: bizID,
		})
		if errors.Is(err, pgx.ErrNoRows) {
			httpx.WriteError(w, r, httpx.NewHTTPException(http.StatusNotFound, "Order not found"))
			return
		}
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}

		itemRows, err := q.ListOrderItemsWithProduct(r.Context(), row.ID)
		if err != nil {
			httpx.WriteError(w, r, err)
			return
		}
		items := make([]orderItemJSON, 0, len(itemRows))
		for _, item := range itemRows {
			items = append(items, orderItemJSON{
				ID:          item.ID,
				OrderID:     item.OrderID,
				ProductID:   item.ProductID,
				ProductName: item.ProductName,
				Quantity:    item.Quantity,
				UnitPrice:   ep_num(item.UnitPrice),
				Product: productPlainJSON{
					ID:           item.ProductID,
					BusinessID:   item.PBusinessID,
					Name:         item.PName,
					Description:  ep_text(item.PDescription),
					Category:     ep_text(item.PCategory),
					RequestCount: item.PRequestCount,
					StockNote:    ep_text(item.PStockNote),
					DeliveryNote: ep_text(item.PDeliveryNote),
					Price:        ep_num(item.PPrice),
					Stock:        item.PStock,
					SKU:          ep_text(item.PSku),
					IsAvailable:  item.PIsAvailable,
					CreatedAt:    epISO(item.PCreatedAt.Time),
					UpdatedAt:    epISO(item.PUpdatedAt.Time),
				},
			})
		}

		cust, err := q.GetCustomerByIDAndBusiness(r.Context(), gen.GetCustomerByIDAndBusinessParams{
			ID:         row.CustomerID,
			BusinessID: bizID,
		})
		if err != nil && !errors.Is(err, pgx.ErrNoRows) {
			httpx.WriteError(w, r, err)
			return
		}
		var customer *customerJSON
		if err == nil {
			cj := ep_customer(cust)
			customer = &cj
		}

		detail := orderDetailJSON{
			orderBaseJSON: orderBaseJSON{
				ID:             row.ID,
				BusinessID:     row.BusinessID,
				CustomerID:     row.CustomerID,
				ConversationID: ep_text(row.ConversationID),
				IdempotencyKey: ep_text(row.IdempotencyKey),
				Status:         string(row.Status),
				TotalAmount:    ep_num(row.TotalAmount),
				Currency:       row.Currency,
				CreatedAt:      epISO(row.CreatedAt.Time),
				UpdatedAt:      epISO(row.UpdatedAt.Time),
			},
			Customer: customer,
			Items:    items,
		}
		_ = httpx.WriteJSON(w, http.StatusOK, detail)
	}
}
