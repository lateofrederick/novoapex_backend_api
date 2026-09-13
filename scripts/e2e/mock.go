package main

import (
	"bufio"
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"fmt"
	"image"
	"image/color"
	"image/jpeg"
	"io"
	"math"
	"net"
	"net/http"
	"regexp"
	"strings"
	"sync"
	"time"
)

// providerMock impersonates every external provider the Go services call:
// Meta Graph (messages + media), OpenAI (responses, embeddings, whisper),
// Gemini embeddings, Paystack and Cloudinary. It records every call so the
// scenario can assert on exactly what left the system.
type providerMock struct {
	URL string

	mu    sync.Mutex
	sent  []waMessage // WhatsApp messages delivered, in order
	llm   []llmCall
	calls map[string]int

	sentry      []sentryItem // envelope items received by the Sentry ingest
	metaTraceID []string     // trace ids propagated on Meta sends (sentry-trace)
}

// sentryItem is one envelope item: an error "event", a "transaction", its
// "profile", or a "log" batch.
type sentryItem struct {
	EventID string
	Type    string
	Payload map[string]any
}

type waMessage struct {
	PhoneNumberID string
	To            string
	Type          string
	Text          string
	ImageLink     string
	Caption       string
	Template      string
}

type llmCall struct {
	LastUser string
	System   string
	HasImage bool
}

func startMock() (*providerMock, error) {
	m := &providerMock{calls: map[string]int{}}
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, err
	}
	m.URL = "http://" + ln.Addr().String()
	mux := http.NewServeMux()
	mux.HandleFunc("/graph/", m.graph)
	mux.HandleFunc("/media-bytes/", m.mediaBytes)
	mux.HandleFunc("/openai/responses", m.responses)
	mux.HandleFunc("/openai/embeddings", m.embeddings)
	mux.HandleFunc("/openai/audio/transcriptions", m.transcriptions)
	mux.HandleFunc("/google/", m.geminiEmbed)
	mux.HandleFunc("/paystack/", m.paystack)
	mux.HandleFunc("/cloudinary/", m.cloudinary)
	mux.HandleFunc("/img/", m.imageFile)
	mux.HandleFunc("/sentry/api/1/envelope/", m.sentryEnvelope)
	go func() { _ = http.Serve(ln, mux) }()
	return m, nil
}

// SentryDSN points the services' Sentry SDK at this mock.
func (m *providerMock) SentryDSN() string {
	return strings.Replace(m.URL, "http://", "http://e2epublickey@", 1) + "/sentry/1"
}

// --- Sentry ingest ------------------------------------------------------------------

func (m *providerMock) sentryEnvelope(w http.ResponseWriter, r *http.Request) {
	if !strings.Contains(r.Header.Get("X-Sentry-Auth"), "sentry_key=e2epublickey") {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"detail": "missing auth"})
		return
	}
	body, _ := io.ReadAll(r.Body)
	rd := bufio.NewReader(bytes.NewReader(body))
	headerLine, _ := rd.ReadBytes('\n')
	var header struct {
		EventID string `json:"event_id"`
	}
	_ = json.Unmarshal(headerLine, &header)
	var items []sentryItem
	for {
		line, err := rd.ReadBytes('\n')
		if len(bytes.TrimSpace(line)) == 0 {
			if err != nil {
				break
			}
			continue
		}
		var ih struct {
			Type   string `json:"type"`
			Length *int   `json:"length"`
		}
		if json.Unmarshal(line, &ih) != nil {
			break
		}
		var payload []byte
		if ih.Length != nil {
			payload = make([]byte, *ih.Length)
			if _, err := io.ReadFull(rd, payload); err != nil {
				break
			}
			_, _ = rd.ReadByte()
		} else {
			payload, _ = rd.ReadBytes('\n')
		}
		var decoded map[string]any
		_ = json.Unmarshal(payload, &decoded)
		items = append(items, sentryItem{EventID: header.EventID, Type: ih.Type, Payload: decoded})
	}
	m.mu.Lock()
	m.sentry = append(m.sentry, items...)
	m.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{"id": header.EventID})
}

func (m *providerMock) sentryItems(itemType string) []sentryItem {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []sentryItem
	for _, it := range m.sentry {
		if it.Type == itemType {
			out = append(out, it)
		}
	}
	return out
}

func (m *providerMock) propagatedTraceIDs() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]string(nil), m.metaTraceID...)
}

func (m *providerMock) count(kind string) {
	m.mu.Lock()
	m.calls[kind]++
	m.mu.Unlock()
}

func (m *providerMock) callCount(kind string) int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.calls[kind]
}

func (m *providerMock) messagesTo(to string) []waMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	var out []waMessage
	for _, s := range m.sent {
		if s.To == to {
			out = append(out, s)
		}
	}
	return out
}

func (m *providerMock) llmCalls() []llmCall {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]llmCall(nil), m.llm...)
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// --- Meta Graph ---------------------------------------------------------------

func (m *providerMock) graph(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer wa-token-e2e" {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"error": map[string]any{"message": "bad token"}})
		return
	}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/graph/"), "/")
	// GET /graph/{version}/{mediaId} → media metadata
	if r.Method == http.MethodGet && len(parts) == 2 {
		m.count("media-metadata")
		writeJSON(w, http.StatusOK, map[string]any{"url": m.URL + "/media-bytes/" + parts[1], "mime_type": "audio/ogg"})
		return
	}
	// POST /graph/{version}/{phoneNumberId}/messages
	if r.Method != http.MethodPost || len(parts) != 3 || parts[2] != "messages" {
		http.NotFound(w, r)
		return
	}
	var body struct {
		Product string `json:"messaging_product"`
		To      string `json:"to"`
		Type    string `json:"type"`
		Text    struct {
			Body string `json:"body"`
		} `json:"text"`
		Image struct {
			Link    string `json:"link"`
			Caption string `json:"caption"`
		} `json:"image"`
		Template struct {
			Name string `json:"name"`
		} `json:"template"`
	}
	if err := json.NewDecoder(r.Body).Decode(&body); err != nil || body.Product != "whatsapp" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"message": "bad payload"}})
		return
	}
	if strings.Contains(body.Text.Body, "meta-reject") {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"message": "(#131047) Re-engagement message"}})
		return
	}
	m.mu.Lock()
	if traceID, _, ok := strings.Cut(r.Header.Get("Sentry-Trace"), "-"); ok {
		m.metaTraceID = append(m.metaTraceID, traceID)
	}
	m.sent = append(m.sent, waMessage{
		PhoneNumberID: parts[1], To: body.To, Type: body.Type, Text: body.Text.Body,
		ImageLink: body.Image.Link, Caption: body.Image.Caption, Template: body.Template.Name,
	})
	n := len(m.sent)
	m.mu.Unlock()
	writeJSON(w, http.StatusOK, map[string]any{
		"messaging_product": "whatsapp",
		"contacts":          []any{map[string]any{"input": body.To, "wa_id": body.To}},
		"messages":          []any{map[string]any{"id": fmt.Sprintf("wamid.e2e.%d", n)}},
	})
}

func (m *providerMock) mediaBytes(w http.ResponseWriter, r *http.Request) {
	m.count("media-download")
	w.Header().Set("Content-Type", "audio/ogg")
	_, _ = w.Write([]byte("OggS-fake-voice-note"))
}

// --- OpenAI ---------------------------------------------------------------------

var catalogIDRe = regexp.MustCompile(`\[ID: ([0-9a-f]{8})\] Shea Butter`)

func (m *providerMock) responses(w http.ResponseWriter, r *http.Request) {
	m.count("llm")
	// Model latency: long enough for the orchestrator run to be profiled.
	time.Sleep(80 * time.Millisecond)
	var req struct {
		Input []struct {
			Role    string `json:"role"`
			Content []struct {
				Type     string `json:"type"`
				Text     string `json:"text"`
				ImageURL string `json:"image_url"`
			} `json:"content"`
		} `json:"input"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"message": err.Error()}})
		return
	}
	var system, lastUser string
	hasImage := false
	for _, in := range req.Input {
		for _, c := range in.Content {
			switch {
			case in.Role == "system" && c.Type == "input_text":
				system = c.Text
			case in.Role == "user" && c.Type == "input_text":
				lastUser = c.Text
			case in.Role == "user" && c.Type == "input_image":
				hasImage = true
			}
		}
	}
	m.mu.Lock()
	m.llm = append(m.llm, llmCall{LastUser: lastUser, System: system, HasImage: hasImage})
	m.mu.Unlock()

	text := strings.ToLower(lastUser)
	if strings.Contains(text, "llm-error") {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": map[string]any{"message": "upstream exploded"}})
		return
	}
	shea := ""
	if match := catalogIDRe.FindStringSubmatch(system); match != nil {
		shea = match[1]
	}

	crm := map[string]any{
		"order_confirmed": false, "detected_items": []any{}, "delivery_area": nil,
		"fulfillment_choice": nil, "pickup_location_id": nil, "customer_name": nil,
		"detected_preferences": []string{}, "wants_updates": nil, "sentiment": "neutral",
	}
	out := map[string]any{
		"reply_text": "Hello! How can I help you today?", "internal_confidence": 0.95,
		"intent": "greeting", "customer_requested_images": false,
		"send_product_image_ids": []string{}, "referenced_product_ids": []string{},
		"crm_signals": crm,
	}
	switch {
	case strings.Contains(text, "photo"):
		out["intent"] = "product_inquiry"
		out["reply_text"] = "Here is our Shea Butter [ID: " + shea + "]"
		out["customer_requested_images"] = true
		out["send_product_image_ids"] = []string{shea}
		out["referenced_product_ids"] = []string{shea}
		crm["detected_preferences"] = []string{"skin care"}
	case strings.Contains(text, "confirm"):
		out["intent"] = "checkout_request"
		out["reply_text"] = "Your order is confirmed! A secure payment link is on its way."
		out["referenced_product_ids"] = []string{shea}
		crm["order_confirmed"] = true
		crm["detected_items"] = []any{map[string]any{"product_id": shea, "quantity": 2}}
		crm["fulfillment_choice"] = "delivery"
		crm["delivery_area"] = "East Legon"
		crm["customer_name"] = "Ama Mensah"
		crm["wants_updates"] = true
		crm["sentiment"] = "positive"
	case strings.Contains(text, "not sure"):
		out["internal_confidence"] = 0.3
	case strings.Contains(text, "how much"):
		out["intent"] = "product_inquiry"
		out["reply_text"] = "Shea Butter is GH₵45."
	}
	body, _ := json.Marshal(out)
	writeJSON(w, http.StatusOK, map[string]any{
		"id": "resp_e2e", "status": "completed",
		"output": []any{map[string]any{"type": "message", "role": "assistant",
			"content": []any{map[string]any{"type": "output_text", "text": string(body)}}}},
		"usage": map[string]any{"input_tokens": 100, "output_tokens": 50, "total_tokens": 150},
	})
}

// fakeVector derives a stable unit vector from text so identical inputs embed
// identically (lets vector search rank the product we asked about).
func fakeVector(seed string) []float32 {
	v := make([]float32, 768)
	var norm float64
	for i := range v {
		h := sha256.Sum256([]byte(fmt.Sprintf("%s#%d", seed, i/32)))
		x := float64(binary.BigEndian.Uint32(h[:4]))/float64(math.MaxUint32) - 0.5
		v[i] = float32(x)
		norm += x * x
	}
	norm = math.Sqrt(norm)
	for i := range v {
		v[i] = float32(float64(v[i]) / norm)
	}
	return v
}

func (m *providerMock) embeddings(w http.ResponseWriter, r *http.Request) {
	m.count("embedding")
	var req struct {
		Input      []string `json:"input"`
		Dimensions int      `json:"dimensions"`
		Model      string   `json:"model"`
	}
	_ = json.NewDecoder(r.Body).Decode(&req)
	if req.Dimensions != 768 || req.Model != "text-embedding-3-small" || len(req.Input) != 1 {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"message": "unexpected embedding request"}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"data": []any{map[string]any{"index": 0, "embedding": fakeVector("text")}}})
}

func (m *providerMock) transcriptions(w http.ResponseWriter, r *http.Request) {
	m.count("transcription")
	if err := r.ParseMultipartForm(1 << 20); err != nil || r.FormValue("model") != "whisper-1" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"message": "bad multipart"}})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"text": "voice note: how much is the shea butter?"})
}

func (m *providerMock) geminiEmbed(w http.ResponseWriter, r *http.Request) {
	m.count("gemini")
	if !strings.HasSuffix(r.URL.Path, "/models/gemini-embedding-001:embedContent") {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"embedding": map[string]any{"values": fakeVector("image")}})
}

// --- Paystack ---------------------------------------------------------------------

func (m *providerMock) paystack(w http.ResponseWriter, r *http.Request) {
	if r.Header.Get("Authorization") != "Bearer sk_test_e2e" {
		writeJSON(w, http.StatusUnauthorized, map[string]any{"status": false, "message": "Invalid key"})
		return
	}
	var body map[string]any
	_ = json.NewDecoder(r.Body).Decode(&body)
	switch strings.TrimPrefix(r.URL.Path, "/paystack") {
	case "/transaction/initialize":
		m.count("paystack-initialize")
		ref, _ := body["reference"].(string)
		writeJSON(w, http.StatusOK, map[string]any{"status": true, "message": "Authorization URL created",
			"data": map[string]any{"authorization_url": "https://checkout.paystack.mock/" + ref, "access_code": "ac_e2e", "reference": ref}})
	case "/transferrecipient":
		m.count("paystack-recipient")
		writeJSON(w, http.StatusOK, map[string]any{"status": true, "data": map[string]any{"recipient_code": "RCP_e2e"}})
	case "/transfer":
		m.count("paystack-transfer")
		writeJSON(w, http.StatusOK, map[string]any{"status": true, "data": map[string]any{"status": "success", "transfer_code": "TRF_e2e"}})
	default:
		http.NotFound(w, r)
	}
}

// --- Cloudinary -----------------------------------------------------------------

func (m *providerMock) cloudinary(w http.ResponseWriter, r *http.Request) {
	switch {
	case strings.HasSuffix(r.URL.Path, "/image/upload"):
		m.count("cloudinary-upload")
		if err := r.ParseMultipartForm(10 << 20); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": map[string]any{"message": err.Error()}})
			return
		}
		n := m.callCount("cloudinary-upload")
		id := fmt.Sprintf("novoapex/products/e2e-%d", n)
		writeJSON(w, http.StatusOK, map[string]any{"secure_url": fmt.Sprintf("%s/img/e2e-%d.jpg", m.URL, n), "public_id": id})
	case strings.HasSuffix(r.URL.Path, "/image/destroy"):
		m.count("cloudinary-destroy")
		writeJSON(w, http.StatusOK, map[string]any{"result": "ok"})
	default:
		http.NotFound(w, r)
	}
}

func (m *providerMock) imageFile(w http.ResponseWriter, r *http.Request) {
	m.count("image-fetch")
	w.Header().Set("Content-Type", "image/jpeg")
	_, _ = w.Write(testJPEG(color.RGBA{R: 200, G: 160, B: 90, A: 255}))
}

func testJPEG(c color.Color) []byte {
	img := image.NewRGBA(image.Rect(0, 0, 16, 16))
	for x := 0; x < 16; x++ {
		for y := 0; y < 16; y++ {
			img.Set(x, y, c)
		}
	}
	var buf bytes.Buffer
	_ = jpeg.Encode(&buf, img, nil)
	return buf.Bytes()
}
