package workers_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/novoapex/novoapex-backend-api/internal/events"
	harness "github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/expo"
	"github.com/novoapex/novoapex-backend-api/internal/workers"
)

type expoFake struct {
	mu          sync.Mutex
	messages    []expo.PushMessage
	deadTokens  []string
	returnError error
}

func (f *expoFake) SendPushNotifications(_ context.Context, messages []expo.PushMessage) ([]expo.PushTicket, []string, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.returnError != nil {
		return nil, nil, f.returnError
	}
	f.messages = append(f.messages, messages...)
	tickets := make([]expo.PushTicket, len(messages))
	for i := range messages {
		tickets[i] = expo.PushTicket{Status: "ok", ID: "ticket-123"}
	}
	return tickets, f.deadTokens, nil
}

func (f *expoFake) getMessages() []expo.PushMessage {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]expo.PushMessage, len(f.messages))
	copy(out, f.messages)
	return out
}

func TestPushSendHandler(t *testing.T) {
	s7b := s7bStartDB(t)
	ctx := t.Context()

	biz := s7b.factory.Business()

	// Insert 2 device tokens for the business
	_, err := s7b.pool.Exec(ctx, `
		INSERT INTO device_tokens (id, business_id, owner_phone, expo_push_token, platform, device_name)
		VALUES
			('dev-1', $1, $2, 'ExponentPushToken[active-1]', 'ios', 'iPhone 15'),
			('dev-2', $1, $2, 'ExponentPushToken[dead-2]', 'android', 'Samsung Galaxy')
	`, biz.ID, biz.OwnerPhone)
	if err != nil {
		t.Fatalf("insert tokens: %v", err)
	}

	fake := &expoFake{deadTokens: []string{"ExponentPushToken[dead-2]"}}
	h := workers.PushSendHandler(workers.PushDeps{Pool: s7b.pool, Expo: fake})

	payload, _ := json.Marshal(workers.PushJobPayload{
		BusinessID: biz.ID,
		Title:      "Test Alert",
		Body:       "Hello Vendor",
		ChannelID:  "alerts",
	})

	if err := h(ctx, payload); err != nil {
		t.Fatalf("handler failed: %v", err)
	}

	msgs := fake.getMessages()
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages sent, got %d", len(msgs))
	}
	if msgs[0].Title != "Test Alert" || msgs[0].Body != "Hello Vendor" {
		t.Errorf("unexpected message content: %+v", msgs[0])
	}

	// Verify dead token was culled from DB
	var remainingCount int
	if err := s7b.pool.QueryRow(ctx, `SELECT COUNT(*) FROM device_tokens WHERE business_id = $1`, biz.ID).Scan(&remainingCount); err != nil {
		t.Fatalf("count remaining: %v", err)
	}
	if remainingCount != 1 {
		t.Errorf("expected 1 remaining token (dead token culled), got %d", remainingCount)
	}
}

func TestPushOrderCreatedHandler(t *testing.T) {
	s7b := s7bStartDB(t)
	ctx := t.Context()

	biz := s7b.factory.Business()
	cust := s7b.factory.Customer(biz.ID)
	orderID := "ord-push-test-1"

	// Insert device token
	_, err := s7b.pool.Exec(ctx, `
		INSERT INTO device_tokens (id, business_id, owner_phone, expo_push_token, platform)
		VALUES ('dev-1', $1, $2, 'ExponentPushToken[vendor-token]', 'ios')
	`, biz.ID, biz.OwnerPhone)
	if err != nil {
		t.Fatalf("insert token: %v", err)
	}

	// Insert order and order item with low stock (2 units left)
	prod := s7b.factory.Product(biz.ID, harness.WithStock(2), harness.WithPrice("100.00"))
	_, err = s7b.pool.Exec(ctx, `
		INSERT INTO orders (id, business_id, customer_id, total_amount, currency, status, updated_at)
		VALUES ($1, $2, $3, 200.0, 'GHS', 'CONFIRMED', CURRENT_TIMESTAMP)
	`, orderID, biz.ID, cust.ID)
	if err != nil {
		t.Fatalf("insert order: %v", err)
	}

	_, err = s7b.pool.Exec(ctx, `
		INSERT INTO order_items (id, order_id, product_id, product_name, quantity, unit_price)
		VALUES ('oi-1', $1, $2, 'Sneakers', 2, 100.0)
	`, orderID, prod.ID)
	if err != nil {
		t.Fatalf("insert order item: %v", err)
	}

	fake := &expoFake{}
	h := workers.PushOrderCreatedHandler(workers.PushDeps{Pool: s7b.pool, Expo: fake})

	evtBytes, _ := json.Marshal(events.OrderCreated{
		OrderID:        orderID,
		BusinessID:     biz.ID,
		CustomerID:     cust.ID,
		ConversationID: "conv-1",
		TotalAmount:    "200.00",
		Currency:       "GHS",
	})

	if err := h(ctx, evtBytes); err != nil {
		t.Fatalf("handler failed: %v", err)
	}

	msgs := fake.getMessages()
	// Should have sent 2 push notifications: ORDER_CREATED + INVENTORY_LOW_STOCK!
	if len(msgs) != 2 {
		t.Fatalf("expected 2 messages (order created + low stock alert), got %d", len(msgs))
	}

	if msgs[0].Title != "New Order Placed! 🛒" {
		t.Errorf("expected order title, got %s", msgs[0].Title)
	}
	if msgs[1].Title != "Low Stock Warning 📉" {
		t.Errorf("expected low stock title, got %s", msgs[1].Title)
	}
}

func TestPushOrderCancelledHandler(t *testing.T) {
	s7b := s7bStartDB(t)
	ctx := t.Context()

	biz := s7b.factory.Business()
	cust := s7b.factory.Customer(biz.ID)
	orderID := "ord-push-cancel-1"

	_, err := s7b.pool.Exec(ctx, `
		INSERT INTO device_tokens (id, business_id, owner_phone, expo_push_token, platform)
		VALUES ('dev-1', $1, $2, 'ExponentPushToken[vendor-token]', 'ios')
	`, biz.ID, biz.OwnerPhone)
	if err != nil {
		t.Fatalf("insert token: %v", err)
	}

	_, err = s7b.pool.Exec(ctx, `
		INSERT INTO orders (id, business_id, customer_id, total_amount, currency, status, updated_at)
		VALUES ($1, $2, $3, 200.0, 'GHS', 'CANCELLED', CURRENT_TIMESTAMP)
	`, orderID, biz.ID, cust.ID)
	if err != nil {
		t.Fatalf("insert order: %v", err)
	}

	fake := &expoFake{}
	h := workers.PushOrderCancelledHandler(workers.PushDeps{Pool: s7b.pool, Expo: fake})

	evtBytes, _ := json.Marshal(events.OrderCancelled{
		OrderID: orderID,
		Reason:  "payment_failed",
	})

	if err := h(ctx, evtBytes); err != nil {
		t.Fatalf("handler failed: %v", err)
	}

	msgs := fake.getMessages()
	if len(msgs) != 1 {
		t.Fatalf("expected 1 message, got %d", len(msgs))
	}
	if msgs[0].Title != "Order Cancelled ⚠️" {
		t.Errorf("expected cancel title, got %s", msgs[0].Title)
	}
}
