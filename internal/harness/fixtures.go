package harness

import (
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"fmt"
	"hash/fnv"
	"testing"
	"time"

	"github.com/pgvector/pgvector-go"
)

type Factory struct {
	DB  *sql.DB
	t   *testing.T
	seq int
}

type Business struct {
	ID                    string
	Name                  string
	OwnerPhone            string
	WhatsAppPhoneNumberID string
	Currency              string
}

type Customer struct {
	ID         string
	BusinessID string
	Phone      string
	Name       string
	ProfileID  string
}

type Profile struct {
	ID         string
	CustomerID string
}

type Conversation struct {
	ID            string
	BusinessID    string
	CustomerPhone string
	State         string
}

type Product struct {
	ID         string
	BusinessID string
	Name       string
	Price      string
	Stock      int
	Embedding  []float32
}

type ProductImage struct {
	ID          string
	ProductID   string
	URL         string
	Position    int
	ContentHash string
}

type FAQ struct {
	ID         string
	BusinessID string
	Question   string
	Answer     string
	PolicyTag  string
}

type conversationOptions struct {
	State      string
	CustomerID string
}

type ConversationOption func(*conversationOptions)

func WithState(state string) ConversationOption {
	return func(o *conversationOptions) { o.State = state }
}

func WithLinkedCustomer(customerID string) ConversationOption {
	return func(o *conversationOptions) { o.CustomerID = customerID }
}

func NewFactory(t *testing.T, db *sql.DB) *Factory {
	return &Factory{DB: db, t: t}
}

func (f *Factory) nextID(kind string) string {
	f.seq++
	return fmt.Sprintf("%08x%04x", uint64(0xEC000000)+uint64(f.seq), f.seq)
}

func (f *Factory) nextPhone() string {
	f.seq++
	return fmt.Sprintf("+2332000%05d", f.seq)
}

func (f *Factory) exec(label, query string, args ...any) {
	f.t.Helper()
	if _, err := f.DB.Exec(query, args...); err != nil {
		f.t.Fatalf("fixture %s: %v", label, err)
	}
}

func (f *Factory) Business() Business {
	f.t.Helper()
	b := Business{
		ID:                    f.nextID("biz"),
		Name:                  fmt.Sprintf("Fixture Business %d", f.seq+1),
		OwnerPhone:            f.nextPhone(),
		WhatsAppPhoneNumberID: f.nextID("wni"),
		Currency:              "GHS",
	}
	f.exec("business", `INSERT INTO businesses
		(id, name, whatsapp_phone_number_id, owner_phone, currency, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		b.ID, b.Name, b.WhatsAppPhoneNumberID, b.OwnerPhone, b.Currency, time.Now().UTC())
	return b
}

func (f *Factory) Customer(businessID string) Customer {
	f.t.Helper()
	c := Customer{
		ID:         f.nextID("cust"),
		BusinessID: businessID,
		Phone:      f.nextPhone(),
		Name:       fmt.Sprintf("Fixture Customer %d", f.seq+1),
	}
	now := time.Now().UTC()
	f.exec("customer", `INSERT INTO customers
		(id, business_id, phone, name, acquisition_channel, first_contact_at, last_contact_at, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8)`,
		c.ID, c.BusinessID, c.Phone, c.Name, "whatsapp", now, now, now)
	p := f.Profile(c.ID)
	c.ProfileID = p.ID
	return c
}

func (f *Factory) Profile(customerID string) Profile {
	f.t.Helper()
	p := Profile{ID: f.nextID("prof"), CustomerID: customerID}
	f.exec("profile", `INSERT INTO customer_profiles
		(id, customer_id, updated_at) VALUES ($1, $2, $3)`,
		p.ID, p.CustomerID, time.Now().UTC())
	return p
}

func (f *Factory) Conversation(businessID, customerPhone string, opts ...ConversationOption) Conversation {
	f.t.Helper()
	o := conversationOptions{State: "LEAD"}
	for _, opt := range opts {
		opt(&o)
	}
	c := Conversation{
		ID:            f.nextID("conv"),
		BusinessID:    businessID,
		CustomerPhone: customerPhone,
		State:         o.State,
	}
	f.exec("conversation", `INSERT INTO conversations
		(id, business_id, customer_id, customer_phone, state, updated_at)
		VALUES ($1, $2, NULLIF($3, ''), $4, $5, $6)`,
		c.ID, c.BusinessID, o.CustomerID, c.CustomerPhone, c.State, time.Now().UTC())
	return c
}

type productOptions struct {
	Price string
	Stock int
}

type ProductOption func(*productOptions)

func WithPrice(price string) ProductOption {
	return func(o *productOptions) { o.Price = price }
}

func WithStock(stock int) ProductOption {
	return func(o *productOptions) { o.Stock = stock }
}

func (f *Factory) Product(businessID string, opts ...ProductOption) Product {
	return f.ProductWithImages(businessID, 0, opts...)
}

func (f *Factory) ProductWithImages(businessID string, imageCount int, opts ...ProductOption) Product {
	f.t.Helper()
	o := productOptions{Price: "25.50", Stock: 0}
	for _, opt := range opts {
		opt(&o)
	}
	p := Product{
		ID:         f.nextID("prod"),
		BusinessID: businessID,
		Name:       fmt.Sprintf("Fixture Product %d", f.seq+1),
		Price:      o.Price,
		Stock:      o.Stock,
		Embedding:  Embedding("product:"+businessID+fmt.Sprint(f.seq+1), 768),
	}
	f.exec("product", `INSERT INTO products
		(id, business_id, name, price, stock, embedding, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		p.ID, p.BusinessID, p.Name, p.Price, p.Stock, pgvector.NewVector(p.Embedding), time.Now().UTC())
	for i := 0; i < imageCount; i++ {
		f.ProductImage(p.ID, i)
	}
	return p
}

func (f *Factory) ProductImage(productID string, position int) ProductImage {
	f.t.Helper()
	img := ProductImage{
		ID:          f.nextID("img"),
		ProductID:   productID,
		URL:         fmt.Sprintf("https://res.example.com/%s_%d.jpg", productID, position),
		Position:    position,
		ContentHash: HashString(fmt.Sprintf("%s:%d", productID, position)),
	}
	f.exec("product_image", `INSERT INTO product_images
		(id, product_id, url, position, embedding, content_hash)
		VALUES ($1, $2, $3, $4, $5, $6)`,
		img.ID, img.ProductID, img.URL, img.Position,
		pgvector.NewVector(Embedding(img.URL, 768)), img.ContentHash)
	return img
}

func (f *Factory) FAQ(businessID string) FAQ {
	f.t.Helper()
	q := FAQ{
		ID:         f.nextID("faq"),
		BusinessID: businessID,
		Question:   fmt.Sprintf("Fixture question %d?", f.seq+1),
		Answer:     fmt.Sprintf("Fixture answer %d.", f.seq+1),
		PolicyTag:  "delivery",
	}
	f.exec("faq", `INSERT INTO faqs
		(id, business_id, question, answer, policy_tag, embedding, updated_at)
		VALUES ($1, $2, $3, $4, $5, $6, $7)`,
		q.ID, q.BusinessID, q.Question, q.Answer, q.PolicyTag,
		pgvector.NewVector(Embedding(q.Question, 768)), time.Now().UTC())
	return q
}

func Embedding(seed string, dim int) []float32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(seed))
	x := h.Sum32() | 1
	vals := make([]float32, dim)
	for i := range vals {
		x = x*1664525 + 1013904223
		vals[i] = (float32(x>>8)/float32(1<<24))*2 - 1
	}
	return vals
}

func HashString(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])
}
