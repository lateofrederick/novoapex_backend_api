package workers_test

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/novoapex/novoapex-backend-api/internal/events"
	harness "github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
	"github.com/novoapex/novoapex-backend-api/internal/workers"
)

type paystackFake struct {
	mu    sync.Mutex
	calls int
}

func (f *paystackFake) InitiatePayment(_ context.Context, req workers.PaymentRequest) (workers.PaymentLink, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	return workers.PaymentLink{
		Status:            "initiated",
		ProviderReference: "ref-" + req.Reference,
		PaymentURL:        "https://checkout.paystack.test/pay/" + req.Reference,
	}, nil
}

func (f *paystackFake) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

// s7bReadOrderCreatedEvent loads the order.created event checkout persisted.
func s7bReadOrderCreatedEvent(t *testing.T, orderID string) events.OrderCreated {
	t.Helper()
	var raw string
	if err := env.dbw.db.QueryRow(
		`SELECT payload::text FROM domain_events WHERE aggregate_id = $1 AND event_type = 'order.created'`,
		orderID).Scan(&raw); err != nil {
		t.Fatalf("read order.created event: %v", err)
	}
	var evt events.OrderCreated
	if err := json.Unmarshal([]byte(raw), &evt); err != nil {
		t.Fatalf("decode order.created event: %v", err)
	}
	return evt
}

func s7bCheckoutOrder(t *testing.T, bizID, custID, convID, phone, sourceMsg string, shortID string) string {
	t.Helper()
	s7bRunCheckout(t, s7bCheckoutJob(bizID, custID, convID, phone, sourceMsg,
		[]workers.DetectedItem{{ProductID: shortID, Quantity: 2}}))
	return s7bScalarString(t, `SELECT id FROM orders WHERE conversation_id = $1`, convID)
}

func TestS7b_PaymentInitConsumerIdempotent(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	s7bExec(t, `UPDATE businesses SET payment_callback_url = 'https://shop.example.com/success' WHERE id = $1`, biz.ID)
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("25.50"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))

	orderID := s7bCheckoutOrder(t, biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.pay-1", prod.ID[:8])
	evt := s7bReadOrderCreatedEvent(t, orderID)

	paystack := &paystackFake{}
	deps := workers.PaymentInitDeps{Pool: env.dbw.pool, Publisher: env.publisher, Paystack: paystack}

	if err := workers.HandlePaymentInit(ctx(), deps, evt); err != nil {
		t.Fatalf("HandlePaymentInit: %v", err)
	}
	if paystack.count() != 1 {
		t.Fatalf("paystack calls = %d, want 1", paystack.count())
	}
	if url := s7bScalarString(t, `SELECT COALESCE(payment_url,'') FROM orders WHERE id = $1`, orderID); url == "" {
		t.Error("payment_url not persisted")
	}
	if n := s7bScalarInt(t, `SELECT COUNT(*) FROM outbound_messages WHERE conversation_id = $1`, conv.ID); n != 1 {
		t.Errorf("outbound messages = %d, want 1 (invoice)", n)
	}

	// Redelivery must not call Paystack again.
	if err := workers.HandlePaymentInit(ctx(), deps, evt); err != nil {
		t.Fatalf("HandlePaymentInit redelivery: %v", err)
	}
	if paystack.count() != 1 {
		t.Errorf("paystack calls after redelivery = %d, want still 1", paystack.count())
	}
}

func TestS7b_ProfileStatsConsumerIdempotent(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("25.50"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))

	orderID := s7bCheckoutOrder(t, biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.stats-1", prod.ID[:8])
	evt := s7bReadOrderCreatedEvent(t, orderID)

	deps := workers.ProfileStatsDeps{Pool: env.dbw.pool}
	if err := workers.HandleProfileStats(ctx(), deps, evt); err != nil {
		t.Fatalf("HandleProfileStats: %v", err)
	}
	if got := s7bScalarInt(t, `SELECT total_orders FROM customer_profiles WHERE customer_id = $1`, cust.ID); got != 1 {
		t.Fatalf("total_orders = %d, want 1", got)
	}
	if got := s7bScalarString(t, `SELECT total_spent::text FROM customer_profiles WHERE customer_id = $1`, cust.ID); got != "51.00" {
		t.Errorf("total_spent = %s, want 51.00", got)
	}

	// Redelivery must not double-increment.
	if err := workers.HandleProfileStats(ctx(), deps, evt); err != nil {
		t.Fatalf("HandleProfileStats redelivery: %v", err)
	}
	if got := s7bScalarInt(t, `SELECT total_orders FROM customer_profiles WHERE customer_id = $1`, cust.ID); got != 1 {
		t.Errorf("total_orders after redelivery = %d, want still 1", got)
	}
}

func TestS7b_FollowUpScheduleConsumerIdempotent(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("25.50"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))

	orderID := s7bCheckoutOrder(t, biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.fu-1", prod.ID[:8])
	evt := s7bReadOrderCreatedEvent(t, orderID)

	deps := workers.FollowUpScheduleDeps{Pool: env.dbw.pool}
	if err := workers.HandleFollowUpSchedule(ctx(), deps, evt); err != nil {
		t.Fatalf("HandleFollowUpSchedule: %v", err)
	}
	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM scheduled_follow_ups WHERE order_id = $1`, orderID); got != 3 {
		t.Fatalf("follow-ups = %d, want 3", got)
	}

	// Redelivery must not double-schedule.
	if err := workers.HandleFollowUpSchedule(ctx(), deps, evt); err != nil {
		t.Fatalf("HandleFollowUpSchedule redelivery: %v", err)
	}
	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM scheduled_follow_ups WHERE order_id = $1`, orderID); got != 3 {
		t.Errorf("follow-ups after redelivery = %d, want still 3", got)
	}
}

// TestS7b_CheckoutEventEndToEnd drives the whole pipeline synchronously:
// checkout emits order.created; the dispatcher fans it out; each consumer runs
// and produces its durable effect (payment link, stats, follow-ups).
func TestS7b_CheckoutEventEndToEnd(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	s7bExec(t, `UPDATE businesses SET payment_callback_url = 'https://shop.example.com/success' WHERE id = $1`, biz.ID)
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("25.50"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))

	s7bRunCheckout(t, s7bCheckoutJob(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.e2e-1",
		[]workers.DetectedItem{{ProductID: prod.ID[:8], Quantity: 2}}))
	orderID := s7bScalarString(t, `SELECT id FROM orders WHERE conversation_id = $1`, conv.ID)

	// Dispatch the pending event.
	dispatcher := &events.Dispatcher{
		Pool:      env.dbw.pool,
		Publisher: env.publisher,
		Subscriptions: map[string][]events.Subscription{
			events.TypeOrderCreated: {
				{Queue: queue.QPaymentInit, TaskType: queue.TaskPaymentInit},
				{Queue: queue.QCRMMaterialiser, TaskType: queue.TaskProfileStats},
				{Queue: queue.QFollowUp, TaskType: queue.TaskFollowUpSchedule},
			},
		},
	}
	if _, err := dispatcher.Drain(ctx()); err != nil {
		t.Fatalf("dispatcher.Drain: %v", err)
	}

	// Drain the three enqueued consumer jobs.
	paystack := &paystackFake{}
	run := func(taskType string, handler func(payload []byte) error) {
		t.Helper()
		for _, j := range env.publisher.snapshot() {
			if j.TaskType != taskType {
				continue
			}
			b, err := json.Marshal(j.Payload)
			if err != nil {
				t.Fatalf("marshal %s payload: %v", taskType, err)
			}
			if err := handler(b); err != nil {
				t.Fatalf("%s: %v", taskType, err)
			}
		}
	}
	run(queue.TaskPaymentInit, func(p []byte) error {
		var e events.OrderCreated
		if err := json.Unmarshal(p, &e); err != nil {
			return err
		}
		return workers.HandlePaymentInit(ctx(), workers.PaymentInitDeps{Pool: env.dbw.pool, Publisher: env.publisher, Paystack: paystack}, e)
	})
	run(queue.TaskProfileStats, func(p []byte) error {
		var e events.OrderCreated
		if err := json.Unmarshal(p, &e); err != nil {
			return err
		}
		return workers.HandleProfileStats(ctx(), workers.ProfileStatsDeps{Pool: env.dbw.pool}, e)
	})
	run(queue.TaskFollowUpSchedule, func(p []byte) error {
		var e events.OrderCreated
		if err := json.Unmarshal(p, &e); err != nil {
			return err
		}
		return workers.HandleFollowUpSchedule(ctx(), workers.FollowUpScheduleDeps{Pool: env.dbw.pool}, e)
	})

	// All three effects landed.
	if paystack.count() != 1 {
		t.Errorf("paystack calls = %d, want 1", paystack.count())
	}
	if url := s7bScalarString(t, `SELECT COALESCE(payment_url,'') FROM orders WHERE id = $1`, orderID); url == "" {
		t.Error("payment_url not persisted end-to-end")
	}
	if got := s7bScalarInt(t, `SELECT total_orders FROM customer_profiles WHERE customer_id = $1`, cust.ID); got != 1 {
		t.Errorf("total_orders = %d, want 1", got)
	}
	if got := s7bScalarInt(t, `SELECT COUNT(*) FROM scheduled_follow_ups WHERE order_id = $1`, orderID); got != 3 {
		t.Errorf("follow-ups = %d, want 3", got)
	}
}
