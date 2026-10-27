package workers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/novoapex/novoapex-backend-api/internal/db/gen"
	"github.com/novoapex/novoapex-backend-api/internal/events"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/expo"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// ExpoClient describes the push-sending surface needed by the push worker.
type ExpoClient interface {
	SendPushNotifications(ctx context.Context, messages []expo.PushMessage) ([]expo.PushTicket, []string, error)
}

// PushDeps carries dependencies for push notification consumers.
type PushDeps struct {
	Pool *pgxpool.Pool
	Expo ExpoClient
}

// PushJobPayload is the wire format enqueued on QPushNotifications (TaskPushSend).
type PushJobPayload struct {
	BusinessID string         `json:"businessId"`
	Title      string         `json:"title"`
	Subtitle   string         `json:"subtitle,omitempty"`
	Body       string         `json:"body"`
	Sound      string         `json:"sound,omitempty"`
	Badge      int            `json:"badge,omitempty"`
	ChannelID  string         `json:"channelId,omitempty"`
	Priority   string         `json:"priority,omitempty"`
	Data       map[string]any `json:"data,omitempty"`
}

// EnqueuePush is a helper to queue a push notification job.
func EnqueuePush(ctx context.Context, pub queue.Publisher, payload PushJobPayload) error {
	if pub == nil {
		return nil
	}
	return pub.Enqueue(ctx, queue.QPushNotifications, queue.TaskPushSend, payload, nil)
}

// RegisterPush registers all push notification consumers on r.
func RegisterPush(r queue.Registrar, deps PushDeps) {
	r.Register(queue.TaskPushSend, PushSendHandler(deps))
	r.Register(queue.TaskPushOrderCreated, PushOrderCreatedHandler(deps))
	r.Register(queue.TaskPushOrderCancelled, PushOrderCancelledHandler(deps))
}

// PushSendHandler processes TaskPushSend jobs.
func PushSendHandler(deps PushDeps) queue.Handler {
	return func(ctx context.Context, raw []byte) error {
		if deps.Pool == nil {
			return errors.New("push-send: nil pool")
		}
		if deps.Expo == nil {
			return errors.New("push-send: nil expo client")
		}

		var payload PushJobPayload
		if err := json.Unmarshal(raw, &payload); err != nil {
			return fmt.Errorf("push-send: unmarshal payload: %w", err)
		}
		if payload.BusinessID == "" {
			return errors.New("push-send: missing businessId")
		}

		return dispatchToBusiness(ctx, deps, payload)
	}
}

// PushOrderCreatedHandler processes order.created domain events forwarded to push-notifications.
func PushOrderCreatedHandler(deps PushDeps) queue.Handler {
	return func(ctx context.Context, raw []byte) error {
		var evt events.OrderCreated
		if err := json.Unmarshal(raw, &evt); err != nil {
			return fmt.Errorf("push-order-created: unmarshal: %w", err)
		}
		if evt.BusinessID == "" || evt.OrderID == "" {
			return errors.New("push-order-created: missing businessId or orderId")
		}

		body := fmt.Sprintf("New order placed for %s %s", evt.Currency, evt.TotalAmount)

		payload := PushJobPayload{
			BusinessID: evt.BusinessID,
			Title:      "New Order Placed! 🛒",
			Body:       body,
			Sound:      "default",
			ChannelID:  "orders",
			Priority:   "high",
			Data: map[string]any{
				"type":     "ORDER_CREATED",
				"version":  "1.0",
				"entityId": evt.OrderID,
				"url":      "/orders/" + evt.OrderID,
				"metadata": map[string]any{
					"orderId":     evt.OrderID,
					"totalAmount": evt.TotalAmount,
					"currency":    evt.Currency,
				},
			},
		}

		if err := dispatchToBusiness(ctx, deps, payload); err != nil {
			return err
		}

		// Check if any product in this order dropped to low stock (<= 3 units)
		lowStockRows, err := deps.Pool.Query(ctx, `
			SELECT p.id, p.name, p.stock
			FROM order_items oi
			JOIN products p ON p.id = oi.product_id
			WHERE oi.order_id = $1 AND p.stock <= 3
		`, evt.OrderID)
		if err == nil {
			defer lowStockRows.Close()
			for lowStockRows.Next() {
				var pID, pName string
				var pStock int32
				if err := lowStockRows.Scan(&pID, &pName, &pStock); err == nil {
					_ = dispatchToBusiness(ctx, deps, PushJobPayload{
						BusinessID: evt.BusinessID,
						Title:      "Low Stock Warning 📉",
						Body:       fmt.Sprintf("'%s' is down to %d units.", pName, pStock),
						Sound:      "default",
						ChannelID:  "alerts",
						Priority:   "default",
						Data: map[string]any{
							"type":     "INVENTORY_LOW_STOCK",
							"version":  "1.0",
							"entityId": pID,
							"url":      "/product/" + pID,
							"metadata": map[string]any{
								"productId":    pID,
								"productName":  pName,
								"currentStock": pStock,
								"threshold":    3,
							},
						},
					})
				}
			}
		}

		return nil
	}
}

// PushOrderCancelledHandler processes order.cancelled domain events forwarded to push-notifications.
func PushOrderCancelledHandler(deps PushDeps) queue.Handler {
	return func(ctx context.Context, raw []byte) error {
		var evt events.OrderCancelled
		if err := json.Unmarshal(raw, &evt); err != nil {
			return fmt.Errorf("push-order-cancelled: unmarshal: %w", err)
		}
		if evt.OrderID == "" {
			return errors.New("push-order-cancelled: missing orderId")
		}

		var bizID string
		err := deps.Pool.QueryRow(ctx, `SELECT business_id FROM orders WHERE id = $1`, evt.OrderID).Scan(&bizID)
		if err != nil {
			return fmt.Errorf("push-order-cancelled: find order business: %w", err)
		}

		payload := PushJobPayload{
			BusinessID: bizID,
			Title:      "Order Cancelled ⚠️",
			Body:       "An order was cancelled and inventory has been restored.",
			Sound:      "default",
			ChannelID:  "orders",
			Priority:   "default",
			Data: map[string]any{
				"type":     "ORDER_CANCELLED",
				"version":  "1.0",
				"entityId": evt.OrderID,
				"url":      "/orders/" + evt.OrderID,
				"metadata": map[string]any{
					"orderId": evt.OrderID,
					"reason":  evt.Reason,
				},
			},
		}

		return dispatchToBusiness(ctx, deps, payload)
	}
}

func dispatchToBusiness(ctx context.Context, deps PushDeps, payload PushJobPayload) error {
	q := gen.New(deps.Pool)
	tokens, err := q.ListDeviceTokensByBusiness(ctx, payload.BusinessID)
	if err != nil {
		return fmt.Errorf("push: list device tokens: %w", err)
	}
	if len(tokens) == 0 {
		slog.Debug("push: no registered devices for business", slog.String("businessId", payload.BusinessID))
		return nil
	}

	messages := make([]expo.PushMessage, 0, len(tokens))
	for _, dt := range tokens {
		sound := payload.Sound
		if sound == "" {
			sound = "default"
		}
		priority := payload.Priority
		if priority == "" {
			priority = "high"
		}
		channelID := payload.ChannelID
		if channelID == "" {
			channelID = "orders"
		}

		messages = append(messages, expo.PushMessage{
			To:        dt.ExpoPushToken,
			Title:     payload.Title,
			Subtitle:  payload.Subtitle,
			Body:      payload.Body,
			Data:      payload.Data,
			Sound:     sound,
			Badge:     payload.Badge,
			ChannelID: channelID,
			Priority:  priority,
		})
	}

	tickets, deadTokens, err := deps.Expo.SendPushNotifications(ctx, messages)
	if err != nil {
		return fmt.Errorf("push: expo api error: %w", err)
	}

	// Cull tokens that Expo reported as DeviceNotRegistered
	if len(deadTokens) > 0 {
		slog.Info("push: culling dead device tokens",
			slog.Int("count", len(deadTokens)),
			slog.String("businessId", payload.BusinessID))
		if _, err := q.DeleteDeviceTokensByTokens(ctx, deadTokens); err != nil {
			slog.Error("push: failed to delete dead tokens", slog.Any("error", err))
		}
	}

	// For active tokens, touch last_used_at
	for i, ticket := range tickets {
		if ticket.Status == "ok" && i < len(tokens) {
			_, _ = q.TouchDeviceToken(ctx, tokens[i].ExpoPushToken)
		}
	}

	return nil
}
