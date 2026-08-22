package harness

import (
	"database/sql"
	"fmt"
	"testing"

	"github.com/pgvector/pgvector-go"
)

func newTestFactory(t *testing.T) (*Factory, *sql.DB) {
	t.Helper()
	h := startHarness(t)
	repoDir := harnessRepoDir(t)
	applyMigrations(t, h.PostgresDSN, repoDir)

	db, err := sql.Open("pgx", h.PostgresDSN)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })
	return NewFactory(t, db), db
}

func TestBusinessFixtureInserts(t *testing.T) {
	f, db := newTestFactory(t)

	b1 := f.Business()
	b2 := f.Business()

	for _, b := range []Business{b1, b2} {
		var name string
		if err := db.QueryRow(`SELECT name FROM businesses WHERE id = $1`, b.ID).Scan(&name); err != nil {
			t.Fatalf("business %s not persisted: %v", b.ID, err)
		}
	}
	if b1.OwnerPhone == b2.OwnerPhone {
		t.Error("owner_phone must be unique across fixtures")
	}
	if b1.WhatsAppPhoneNumberID == b2.WhatsAppPhoneNumberID {
		t.Error("whatsapp_phone_number_id must be unique across fixtures")
	}
}

func TestCustomerAndProfileFixtures(t *testing.T) {
	f, db := newTestFactory(t)
	b := f.Business()

	c := f.Customer(b.ID)

	var phone string
	var prefs string
	var totalOrders int
	err := db.QueryRow(
		`SELECT cu.phone FROM customers cu WHERE cu.id = $1`, c.ID).Scan(&phone)
	if err != nil {
		t.Fatalf("customer not persisted: %v", err)
	}
	if phone != c.Phone {
		t.Errorf("customer phone = %q, want %q", phone, c.Phone)
	}
	if err := db.QueryRow(
		`SELECT preferences::text, total_orders FROM customer_profiles WHERE id = $1`, c.ProfileID,
	).Scan(&prefs, &totalOrders); err != nil {
		t.Fatalf("profile not persisted: %v", err)
	}
	if prefs != "[]" {
		t.Errorf("default preferences = %q, want []", prefs)
	}
	if totalOrders != 0 {
		t.Errorf("default total_orders = %d, want 0", totalOrders)
	}
}

func TestConversationFixtureDefaultsAndUniqueness(t *testing.T) {
	f, db := newTestFactory(t)
	b := f.Business()
	c := f.Customer(b.ID)

	cv := f.Conversation(b.ID, c.Phone)
	var state, language string
	var escalated bool
	if err := db.QueryRow(
		`SELECT state::text, language, is_escalated_to_human FROM conversations WHERE id = $1`, cv.ID,
	).Scan(&state, &language, &escalated); err != nil {
		t.Fatalf("conversation not persisted: %v", err)
	}
	if state != "LEAD" || language != "en" || escalated {
		t.Errorf("defaults = (%q,%q,%v), want (LEAD,en,false)", state, language, escalated)
	}

	escalatedConv := f.Conversation(b.ID, f.nextPhone(), WithState("ESCALATED"))
	if escalatedConv.State != "ESCALATED" {
		t.Errorf("WithState ignored: %q", escalatedConv.State)
	}

	_, err := db.Exec(
		`INSERT INTO conversations (id, business_id, customer_phone, updated_at)
		 VALUES ('dup_conv', $1, $2, NOW())`,
		b.ID, c.Phone)
	if err == nil {
		t.Error("duplicate (business_id, customer_phone) must violate unique constraint")
	}
}

func TestProductFixtureWithImagesAndEmbeddings(t *testing.T) {
	f, db := newTestFactory(t)
	b := f.Business()

	p := f.ProductWithImages(b.ID, 3)

	var price string
	var emb pgvector.Vector
	if err := db.QueryRow(
		`SELECT price::text, embedding FROM products WHERE id = $1`, p.ID,
	).Scan(&price, &emb); err != nil {
		t.Fatalf("product not persisted: %v", err)
	}
	if price != "25.50" {
		t.Errorf("price = %s, want 25.50", price)
	}
	if len(emb.Slice()) != 768 {
		t.Fatalf("product embedding dims = %d, want 768", len(emb.Slice()))
	}

	rows, err := db.Query(
		`SELECT position, content_hash, embedding FROM product_images WHERE product_id = $1 ORDER BY position`, p.ID)
	if err != nil {
		t.Fatalf("query images: %v", err)
	}
	defer func() { _ = rows.Close() }()

	count := 0
	for rows.Next() {
		var pos int
		var hash *string
		var imgEmb pgvector.Vector
		if err := rows.Scan(&pos, &hash, &imgEmb); err != nil {
			t.Fatalf("scan image: %v", err)
		}
		if pos != count {
			t.Errorf("position = %d, want dense %d", pos, count)
		}
		if hash == nil || *hash == "" {
			t.Errorf("image at position %d missing content_hash", pos)
		}
		if len(imgEmb.Slice()) != 768 {
			t.Errorf("image embedding dims = %d, want 768", len(imgEmb.Slice()))
		}
		count++
	}
	if count != 3 {
		t.Errorf("images created = %d, want 3", count)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate images: %v", err)
	}

	existingHash := HashString(fmt.Sprintf("%s:%d", p.ID, 0))
	_, err = db.Exec(
		`INSERT INTO product_images (id, product_id, url, position, content_hash)
		 VALUES ('dup_img', $1, 'https://x.example/dup.jpg', 9, $2)`,
		p.ID, existingHash)
	if err == nil {
		t.Error("duplicate content_hash on same product must violate unique constraint")
	}
}

func TestFAQFixture(t *testing.T) {
	f, db := newTestFactory(t)
	b := f.Business()

	q := f.FAQ(b.ID)
	var emb pgvector.Vector
	err := db.QueryRow(
		`SELECT question, answer, policy_tag, embedding FROM faqs WHERE id = $1`,
		q.ID).Scan(&q.Question, &q.Answer, &q.PolicyTag, &emb)
	if err != nil {
		t.Fatalf("faq not persisted: %v", err)
	}
	if len(emb.Slice()) != 768 {
		t.Errorf("faq embedding dims = %d, want 768", len(emb.Slice()))
	}
}

func TestEmbeddingsDeterministicPerSeed(t *testing.T) {
	a1 := Embedding("seed-a", 64)
	a2 := Embedding("seed-a", 64)
	b := Embedding("seed-b", 64)
	for i := range a1 {
		if a1[i] != a2[i] {
			t.Fatal("same seed produced different embeddings")
		}
	}
	if fmt.Sprint(a1[:4]) == fmt.Sprint(b[:4]) {
		t.Fatal("different seeds produced identical prefix")
	}
}
