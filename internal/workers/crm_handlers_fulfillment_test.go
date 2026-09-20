package workers_test

// crm_handlers_fulfillment_test.go covers checkout's fulfillment resolution
// (this session's Node work: OrderLedgerHandler.resolveFulfillment /
// resolvePickupLocation), now exercised through workers.HandleCheckout.

import (
	"database/sql"
	"testing"

	"github.com/google/uuid"

	"github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/workers"
)

// s7bSeedLocation inserts a business_locations row directly (no factory
// method exists for it yet) and returns its id.
func s7bSeedLocation(t *testing.T, bizID string, offersPickup, isActive bool) string {
	t.Helper()
	id := uuid.NewString()
	if _, err := env.dbw.db.Exec(`
		INSERT INTO business_locations
			(id, business_id, name, address, opening_time, closing_time,
			 offers_delivery, offers_pickup, is_active, updated_at)
		VALUES ($1, $2, 'Main Street Shop', '12 Main Street', '08:00', '18:00',
		        true, $3, $4, NOW())`,
		id, bizID, offersPickup, isActive); err != nil {
		t.Fatalf("seed location: %v", err)
	}
	return id
}

// s7bFulfilmentCheckout builds a single-item checkout job, optionally choosing
// a pickup location.
func s7bFulfilmentCheckout(bizID, custID, convID, phone, sourceMsg, productID string, choice, locationID *string) workers.CheckoutJob {
	job := s7bCheckoutJob(bizID, custID, convID, phone, sourceMsg,
		[]workers.DetectedItem{{ProductID: productID, Quantity: 1}})
	job.FulfillmentChoice = choice
	job.PickupLocationID = locationID
	return job
}

func TestS7b_Fulfillment_DefaultsToDeliveryWhenUnset(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("10.00"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone,
		harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))

	s7bRunCheckout(t, s7bFulfilmentCheckout(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.fulfil-1", prod.ID[:8], nil, nil))

	orderID := s7bScalarString(t, `SELECT id FROM orders WHERE conversation_id = $1`, conv.ID)
	if ft := s7bScalarString(t, `SELECT fulfillment_type::text FROM orders WHERE id = $1`, orderID); ft != "DELIVERY" {
		t.Errorf("fulfillment_type = %s, want DELIVERY", ft)
	}
	var locationID sql.NullString
	if err := env.dbw.db.QueryRow(`SELECT location_id FROM orders WHERE id = $1`, orderID).Scan(&locationID); err != nil {
		t.Fatalf("read location_id: %v", err)
	}
	if locationID.Valid {
		t.Errorf("location_id = %q, want NULL", locationID.String)
	}
}

func TestS7b_Fulfillment_PickupResolvesLocation(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("10.00"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone,
		harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))
	locID := s7bSeedLocation(t, biz.ID, true, true)
	shortLocID := locID[:8]

	s7bRunCheckout(t, s7bFulfilmentCheckout(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.fulfil-2", prod.ID[:8], s7bPtr("pickup"), s7bPtr(shortLocID)))

	orderID := s7bScalarString(t, `SELECT id FROM orders WHERE conversation_id = $1`, conv.ID)
	if ft := s7bScalarString(t, `SELECT fulfillment_type::text FROM orders WHERE id = $1`, orderID); ft != "PICKUP" {
		t.Errorf("fulfillment_type = %s, want PICKUP", ft)
	}
	if got := s7bScalarString(t, `SELECT location_id FROM orders WHERE id = $1`, orderID); got != locID {
		t.Errorf("location_id = %s, want %s", got, locID)
	}
}

func TestS7b_Fulfillment_PickupUnresolvableLocationStillCreatesOrder(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("10.00"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone,
		harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))

	s7bRunCheckout(t, s7bFulfilmentCheckout(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.fulfil-3", prod.ID[:8], s7bPtr("pickup"), s7bPtr("deadbeef")))

	orderID := s7bScalarString(t, `SELECT id FROM orders WHERE conversation_id = $1`, conv.ID)
	if ft := s7bScalarString(t, `SELECT fulfillment_type::text FROM orders WHERE id = $1`, orderID); ft != "PICKUP" {
		t.Errorf("fulfillment_type = %s, want PICKUP", ft)
	}
	var locationID sql.NullString
	if err := env.dbw.db.QueryRow(`SELECT location_id FROM orders WHERE id = $1`, orderID).Scan(&locationID); err != nil {
		t.Fatalf("read location_id: %v", err)
	}
	if locationID.Valid {
		t.Errorf("location_id = %q, want NULL (unresolvable, non-blocking)", locationID.String)
	}
}

func TestS7b_Fulfillment_PickupOnlyResolvesActivePickupEnabledLocations(t *testing.T) {
	env = s7bNewEnv(t)

	biz := env.dbw.factory.Business()
	prod := env.dbw.factory.Product(biz.ID, harness.WithStock(5), harness.WithPrice("10.00"))
	cust := env.dbw.factory.Customer(biz.ID)
	conv := env.dbw.factory.Conversation(biz.ID, cust.Phone,
		harness.WithLinkedCustomer(cust.ID), harness.WithState("CHECKOUT"))
	// Location exists but does not offer pickup — must not resolve.
	locID := s7bSeedLocation(t, biz.ID, false, true)

	s7bRunCheckout(t, s7bFulfilmentCheckout(biz.ID, cust.ID, conv.ID, cust.Phone, "wamid.fulfil-4", prod.ID[:8], s7bPtr("pickup"), s7bPtr(locID[:8])))

	orderID := s7bScalarString(t, `SELECT id FROM orders WHERE conversation_id = $1`, conv.ID)
	var locationID sql.NullString
	if err := env.dbw.db.QueryRow(`SELECT location_id FROM orders WHERE id = $1`, orderID).Scan(&locationID); err != nil {
		t.Fatalf("read location_id: %v", err)
	}
	if locationID.Valid {
		t.Errorf("location_id = %q, want NULL (offers_pickup=false must not resolve)", locationID.String)
	}
}
