package workers

// new_arrivals_test.go covers SweepNewArrivals (this session's Node work:
// NewArrivalsScannerProcessor), mirroring the s7a_* harness shape already
// used by crons_test.go's SweepRetention tests.

import (
	"database/sql"
	"testing"
	"time"

	"github.com/google/uuid"

	"github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

// s7a_seedProduct inserts a minimal available product with the given
// category and created_at, matching the columns SweepNewArrivals reads.
func s7a_seedProduct(t *testing.T, db *sql.DB, bizID, category string, createdAt time.Time) string {
	t.Helper()
	id := uuid.NewString()
	if _, err := db.Exec(`
		INSERT INTO products (id, business_id, name, price, stock, is_available, category, created_at, updated_at)
		VALUES ($1, $2, 'New Item', 10.00, 5, true, $3, $4, NOW())`,
		id, bizID, category, createdAt.UTC()); err != nil {
		t.Fatalf("seed product: %v", err)
	}
	return id
}

func s7a_optIn(t *testing.T, db *sql.DB, customerID string) {
	t.Helper()
	if _, err := db.Exec(`UPDATE customers SET marketing_opt_in = true WHERE id = $1`, customerID); err != nil {
		t.Fatalf("opt in %s: %v", customerID, err)
	}
}

func s7a_setPreferences(t *testing.T, db *sql.DB, customerID string, prefsJSON string) {
	t.Helper()
	if _, err := db.Exec(`UPDATE customer_profiles SET preferences = $2::jsonb WHERE customer_id = $1`, customerID, prefsJSON); err != nil {
		t.Fatalf("set preferences %s: %v", customerID, err)
	}
}

func TestS7A_NewArrivals_SkipsBusinessesWithoutTemplate(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	factory.Business() // no new_arrivals_template_name configured (default NULL)

	if err := SweepNewArrivals(t.Context(), s7a_deps(s, pub)); err != nil {
		t.Fatalf("SweepNewArrivals: %v", err)
	}
	if len(pub.recorded()) != 0 {
		t.Errorf("enqueues = %d, want 0 (business has no approved template)", len(pub.recorded()))
	}
}

func TestS7A_NewArrivals_CadenceGating(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	biz := factory.Business()
	if _, err := s.db.Exec(`UPDATE businesses SET new_arrivals_template_name = 'tmpl_v1' WHERE id = $1`, biz.ID); err != nil {
		t.Fatalf("set template: %v", err)
	}
	cust := factory.Customer(biz.ID)
	s7a_optIn(t, s.db, cust.ID)
	factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID))
	s7a_seedProduct(t, s.db, biz.ID, "hair", time.Now().UTC())

	t.Run("recently notified business is skipped", func(t *testing.T) {
		recently := time.Now().UTC().Add(-2 * 24 * time.Hour) // within default 14-day interval
		if _, err := s.db.Exec(`UPDATE businesses SET last_new_arrivals_notified_at = $2 WHERE id = $1`, biz.ID, recently); err != nil {
			t.Fatalf("stamp notified: %v", err)
		}
		if err := SweepNewArrivals(t.Context(), s7a_deps(s, pub)); err != nil {
			t.Fatalf("SweepNewArrivals: %v", err)
		}
		if len(pub.recorded()) != 0 {
			t.Errorf("enqueues = %d, want 0 (not due yet)", len(pub.recorded()))
		}
	})

	t.Run("never-notified business with a new product is due", func(t *testing.T) {
		if _, err := s.db.Exec(`UPDATE businesses SET last_new_arrivals_notified_at = NULL WHERE id = $1`, biz.ID); err != nil {
			t.Fatalf("clear notified: %v", err)
		}
		if err := SweepNewArrivals(t.Context(), s7a_deps(s, pub)); err != nil {
			t.Fatalf("SweepNewArrivals: %v", err)
		}
		if len(pub.recorded()) != 1 {
			t.Fatalf("enqueues = %d, want 1", len(pub.recorded()))
		}
		call := pub.recorded()[0]
		if call.Queue != queue.QOutbound || call.TaskType != queue.TaskOutboundSend {
			t.Errorf("enqueue target = %s/%s, want %s/%s", call.Queue, call.TaskType, queue.QOutbound, queue.TaskOutboundSend)
		}

		var msgType, templateName string
		if err := s.db.QueryRow(
			`SELECT message_type, template_name FROM outbound_messages WHERE business_id = $1 ORDER BY created_at DESC LIMIT 1`,
			biz.ID).Scan(&msgType, &templateName); err != nil {
			t.Fatalf("read outbound row: %v", err)
		}
		if msgType != "template" || templateName != "tmpl_v1" {
			t.Errorf("message_type/template_name = %s/%s, want template/tmpl_v1", msgType, templateName)
		}

		var stamped bool
		if err := s.db.QueryRow(`SELECT last_new_arrivals_notified_at IS NOT NULL FROM businesses WHERE id = $1`, biz.ID).Scan(&stamped); err != nil {
			t.Fatalf("check stamped: %v", err)
		}
		if !stamped {
			t.Error("last_new_arrivals_notified_at not stamped after send")
		}
	})
}

func TestS7A_NewArrivals_NoNewProductsSkipsSilently(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	biz := factory.Business()
	if _, err := s.db.Exec(`UPDATE businesses SET new_arrivals_template_name = 'tmpl_v1' WHERE id = $1`, biz.ID); err != nil {
		t.Fatalf("set template: %v", err)
	}
	cust := factory.Customer(biz.ID)
	s7a_optIn(t, s.db, cust.ID)
	factory.Conversation(biz.ID, cust.Phone, harness.WithLinkedCustomer(cust.ID))
	// No products seeded at all.

	if err := SweepNewArrivals(t.Context(), s7a_deps(s, pub)); err != nil {
		t.Fatalf("SweepNewArrivals: %v", err)
	}
	if len(pub.recorded()) != 0 {
		t.Errorf("enqueues = %d, want 0 (no new products)", len(pub.recorded()))
	}
}

func TestS7A_NewArrivals_PreferenceTargeting(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	biz := factory.Business()
	if _, err := s.db.Exec(`UPDATE businesses SET new_arrivals_template_name = 'tmpl_v1' WHERE id = $1`, biz.ID); err != nil {
		t.Fatalf("set template: %v", err)
	}
	s7a_seedProduct(t, s.db, biz.ID, "hair", time.Now().UTC())

	matching := factory.Customer(biz.ID)
	mismatched := factory.Customer(biz.ID)
	noPrefs := factory.Customer(biz.ID)
	for _, c := range []harness.Customer{matching, mismatched, noPrefs} {
		s7a_optIn(t, s.db, c.ID)
		factory.Conversation(biz.ID, c.Phone, harness.WithLinkedCustomer(c.ID))
	}
	s7a_setPreferences(t, s.db, matching.ID, `["hair"]`)
	s7a_setPreferences(t, s.db, mismatched.ID, `["electronics"]`)
	// noPrefs keeps the default empty preferences array — fallback: sent anyway.

	if err := SweepNewArrivals(t.Context(), s7a_deps(s, pub)); err != nil {
		t.Fatalf("SweepNewArrivals: %v", err)
	}

	sentTo := map[string]bool{}
	rows, err := s.db.Query(`SELECT recipient_phone FROM outbound_messages WHERE business_id = $1`, biz.ID)
	if err != nil {
		t.Fatalf("query sent phones: %v", err)
	}
	defer func() { _ = rows.Close() }()
	for rows.Next() {
		var phone string
		if err := rows.Scan(&phone); err != nil {
			t.Fatalf("scan phone: %v", err)
		}
		sentTo[phone] = true
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate sent phones: %v", err)
	}

	if !sentTo[matching.Phone] {
		t.Error("matching-preference customer did not receive the digest")
	}
	if sentTo[mismatched.Phone] {
		t.Error("mismatched-preference customer should NOT receive the digest")
	}
	if !sentTo[noPrefs.Phone] {
		t.Error("no-preference customer should receive the digest (fallback: no data to filter on)")
	}
}
