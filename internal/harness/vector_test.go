package harness

import (
	"database/sql"
	"testing"
	"time"

	"github.com/pgvector/pgvector-go"
)

func exactFloat32(t *testing.T, got, want []float32) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("length = %d, want %d", len(got), len(want))
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("precision loss at index %d: got %v, want %v", i, got[i], want[i])
		}
	}
}

func seedBusinessAndProduct(t *testing.T, db *sql.DB, productID string, embedding pgvector.Vector) {
	t.Helper()
	bizID := "biz_" + productID
	now := time.Now().UTC()
	_, err := db.Exec(
		`INSERT INTO businesses (id, name, whatsapp_phone_number_id, owner_phone, updated_at)
		 VALUES ($1, $2, $3, $4, $5)`,
		bizID, "Harness Biz", "wni_"+productID, "+233000000"+productID[len(productID)-1:], now,
	)
	if err != nil {
		t.Fatalf("insert business: %v", err)
	}
	_, err = db.Exec(
		`INSERT INTO products (id, business_id, name, price, updated_at, embedding)
		 VALUES ($1, $2, $3, $4, $5, $6)`,
		productID, bizID, "Round Trip Product", "12.34", now, embedding,
	)
	if err != nil {
		t.Fatalf("insert product: %v", err)
	}
}

func TestVectorRoundTripPreservesFullPrecision(t *testing.T) {
	h := startHarness(t)
	applyBaseline(t, h.PostgresDSN)

	db, err := sql.Open("pgx", h.PostgresDSN)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	const dim = 768
	want := make([]float32, dim)
	for i := range want {
		want[i] = float32(i%97)*0.031 - 1.5
	}

	seedBusinessAndProduct(t, db, "prod_rt_1", pgvector.NewVector(want))

	var got pgvector.Vector
	if err := db.QueryRow(`SELECT embedding FROM products WHERE id = $1`, "prod_rt_1").Scan(&got); err != nil {
		t.Fatalf("scan embedding: %v", err)
	}
	exactFloat32(t, got.Slice(), want)
}

func TestVectorDimensionIsEnforced(t *testing.T) {
	h := startHarness(t)
	applyBaseline(t, h.PostgresDSN)

	db, err := sql.Open("pgx", h.PostgresDSN)
	if err != nil {
		t.Fatalf("open db: %v", err)
	}
	defer func() { _ = db.Close() }()

	wrong := make([]float32, 767)
	if _, err := db.Exec(
		`INSERT INTO businesses (id, name, whatsapp_phone_number_id, owner_phone, updated_at)
		 VALUES ('b_dim', 'Dim Biz', 'wni_dim', '+2330000001', $1)`,
		time.Now().UTC(),
	); err != nil {
		t.Fatalf("insert business: %v", err)
	}
	_, err = db.Exec(
		`INSERT INTO products (id, business_id, name, price, updated_at, embedding)
		 VALUES ('p_bad', 'b_dim', 'Bad Dim', 1.00, $1, $2)`,
		time.Now().UTC(), pgvector.NewVector(wrong),
	)
	if err == nil {
		t.Fatal("inserting a 767-dim vector into vector(768) must fail")
	}
}
