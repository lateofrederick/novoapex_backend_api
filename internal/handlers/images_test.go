package handlers

// images_test.go — image-pipeline coverage (T4.10–T4.15, T4.23a): multipart
// contract (field names, 5-file cap, 5MB, magic-number types), SHA-256 dedup
// with NULL-hash legacy coexistence, max-5 enforcement, delete re-densification,
// two-phase reorder under the unique index incl. concurrent reorders, and the
// real Cloudinary provider against a local stub asserting the signed request.

import (
	"bytes"
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/novoapex/novoapex-backend-api/internal/auth"
	"github.com/novoapex/novoapex-backend-api/internal/integrations/cloudinary"
	"github.com/novoapex/novoapex-backend-api/internal/storage"
)

// ---- multipart builders ------------------------------------------------------

type s4pFilePart struct {
	Field    string
	Filename string
	Mimetype string
	Content  []byte
}

func s4p_buildMultipart(t *testing.T, fields map[string]string, files []s4pFilePart) (string, []byte) {
	t.Helper()
	buf := &bytes.Buffer{}
	boundary := "----s4pboundary9271"
	for _, f := range files {
		fmt.Fprintf(buf, "--%s\r\nContent-Disposition: form-data; name=%q; filename=%q\r\n", boundary, f.Field, f.Filename)
		if f.Mimetype != "" {
			fmt.Fprintf(buf, "Content-Type: %s\r\n", f.Mimetype)
		}
		buf.WriteString("\r\n")
		buf.Write(f.Content)
		buf.WriteString("\r\n")
	}
	for k, v := range fields {
		fmt.Fprintf(buf, "--%s\r\nContent-Disposition: form-data; name=%q\r\n\r\n%s\r\n", boundary, k, v)
	}
	fmt.Fprintf(buf, "--%s--\r\n", boundary)
	return "multipart/form-data; boundary=" + boundary, buf.Bytes()
}

func s4p_uploadReq(t *testing.T, h http.Handler, target string, claims *auth.Claims, multi bool, contents ...[]byte) *httptest.ResponseRecorder {
	t.Helper()
	field := "files"
	if !multi {
		field = "file"
	}
	parts := make([]s4pFilePart, len(contents))
	for i, c := range contents {
		mt := "image/png"
		if len(c) > 3 && c[0] == 0xFF {
			mt = "image/jpeg"
		}
		parts[i] = s4pFilePart{Field: field, Filename: fmt.Sprintf("img%d.png", i), Mimetype: mt, Content: c}
	}
	ct, body := s4p_buildMultipart(t, nil, parts)
	return s4p_do(t, h, http.MethodPost, target, claims, ct, body)
}

func s4p_png(tag string) []byte { return s4p_pngBytes(tag) }

func TestS4pImagesUploadPipeline(t *testing.T) {
	e := s4p_start(t)
	biz := e.F.Business()
	claims := auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID}
	stub := &s4p_stubStorage{}
	emb := &s4p_recordingEmbedder{}
	h := s4p_productsHandler(e, stub, emb)

	prod := e.F.Product(biz.ID)

	// Single alias upload -> position 0, url/public_id from the provider.
	rec := s4p_uploadReq(t, h, "/products/"+prod.ID+"/image", &claims, false, s4p_png("A"))
	s4p_status(t, rec, http.StatusCreated)
	first := s4p_decode(t, rec)
	imgs, _ := first["images"].([]any)
	if len(imgs) != 1 {
		t.Fatalf("want 1 image, got %s", rec.Body.String())
	}
	img0 := imgs[0].(map[string]any)
	if img0["position"] != json.Number("0") || s4p_str(img0, "url") != "https://res.example.com/stub/1.png" ||
		s4p_str(img0, "storageKey") != "stub/1" {
		t.Fatalf("image0 wrong: %v", img0)
	}
	var storedHash string
	if err := e.DB.QueryRow(`SELECT content_hash FROM product_images WHERE id = $1`,
		s4p_str(img0, "id")).Scan(&storedHash); err != nil {
		t.Fatalf("read hash: %v", err)
	}

	// Multi upload preserves client order; positions append densely.
	bB, bC := s4p_png("B"), s4p_png("C")
	rec = s4p_uploadReq(t, h, "/products/"+prod.ID+"/images", &claims, true, bB, bC)
	s4p_status(t, rec, http.StatusCreated)
	second := s4p_decode(t, rec)
	imgs2, _ := second["images"].([]any)
	if len(imgs2) != 3 {
		t.Fatalf("want 3 images, got %d (%s)", len(imgs2), rec.Body.String())
	}
	if imgs2[1].(map[string]any)["position"] != json.Number("1") ||
		imgs2[2].(map[string]any)["position"] != json.Number("2") {
		t.Fatalf("positions not dense/appended: %v", imgs2)
	}
	if second["imageUrl"] != imgs2[0].(map[string]any)["url"] {
		t.Fatalf("legacy imageUrl not position-0 url")
	}
	if u, d, folders := stub.snapshot(); u != 3 || d != 0 || len(folders) != 3 || folders[0] != "novoapex/products/"+biz.ID {
		t.Fatalf("storage calls uploads=%d deletes=%d folders=%v", u, d, folders)
	}
	emb.mu.Lock()
	if len(emb.imageIDs) != 3 {
		t.Fatalf("image embeds = %d, want 3", len(emb.imageIDs))
	}
	emb.mu.Unlock()

	// Hashes are sha256 of raw bytes.
	wantHash := s4p_sha(bB)
	var gotHash string
	if err := e.DB.QueryRow(`SELECT content_hash FROM product_images WHERE id = $1`,
		s4p_str(imgs2[1].(map[string]any), "id")).Scan(&gotHash); err != nil || gotHash != wantHash {
		t.Fatalf("content hash mismatch got=%q want=%q err=%v (first=%q)", gotHash, wantHash, err, storedHash)
	}

	// MAX-5: product holds 3; adding 3 more is rejected AFTER upload+compensation.
	e.F.ProductImage(prod.ID, 3) // factory row at position 3
	rec = s4p_uploadReq(t, h, "/products/"+prod.ID+"/images", &claims, true,
		s4p_png("D"), s4p_png("E"), s4p_png("F"))
	s4p_status(t, rec, http.StatusBadRequest)
	if msg := s4p_message(t, rec); msg != "A product can have at most 5 images (has 4, tried to add 3)." {
		t.Fatalf("max-5 message = %q", msg)
	}
	if n := s4p_imageCount(t, e, prod.ID); n != 4 {
		t.Fatalf("rejected upload changed rows: %d", n)
	}
	if _, d, _ := stub.snapshot(); d != 3 { // compensating deletes for the orphans
		t.Fatalf("compensating deletes = %d, want 3", d)
	}

	// MULTIPART CONTRACT — six files in one request exceed FilesInterceptor maxCount.
	ct, body := s4p_buildMultipart(t, nil, []s4pFilePart{
		{Field: "files", Filename: "x.png", Mimetype: "image/png", Content: s4p_png("G")},
		{Field: "files", Filename: "y.png", Mimetype: "image/png", Content: s4p_png("H")},
		{Field: "files", Filename: "z.png", Mimetype: "image/png", Content: s4p_png("I")},
		{Field: "files", Filename: "w.png", Mimetype: "image/png", Content: s4p_png("J")},
		{Field: "files", Filename: "v.png", Mimetype: "image/png", Content: s4p_png("K")},
		{Field: "files", Filename: "u.png", Mimetype: "image/png", Content: s4p_png("L")},
	})
	rec = s4p_do(t, h, http.MethodPost, "/products/"+prod.ID+"/images", &claims, ct, body)
	s4p_status(t, rec, http.StatusBadRequest)
	if msg := s4p_message(t, rec); msg != "Unexpected field - files" {
		t.Fatalf("max-count message = %q", msg)
	}

	// No file parts at all -> ParseFilePipe's 'File is required'.
	emptyCT, emptyBody := s4p_buildMultipart(t, map[string]string{"note": "none"}, nil)
	rec = s4p_do(t, h, http.MethodPost, "/products/"+prod.ID+"/images", &claims, emptyCT, emptyBody)
	s4p_status(t, rec, http.StatusBadRequest)
	if msg := s4p_message(t, rec); msg != "File is required" {
		t.Fatalf("missing-file message = %q", msg)
	}
	rec = s4p_do(t, h, http.MethodPost, "/products/"+prod.ID+"/image", &claims,
		"application/json", []byte("{}"))
	s4p_status(t, rec, http.StatusBadRequest)
	if msg := s4p_message(t, rec); msg != "File is required" {
		t.Fatalf("single missing-file message = %q", msg)
	}

	// Oversize file (5MB + 1 byte).
	big := append(s4p_png("BIG"), make([]byte, 5*1024*1024)...)
	rec = s4p_uploadReq(t, h, "/products/"+prod.ID+"/image", &claims, false, big)
	s4p_status(t, rec, http.StatusBadRequest)
	if msg := s4p_message(t, rec); msg != fmt.Sprintf(
		"Validation failed (current file size is %d, expected size is less than %d)",
		len(big), 5*1024*1024) {
		t.Fatalf("size message = %q", msg)
	}

	// Wrong type: text bytes wearing a png mimetype fail magic-number sniffing.
	rec = s4p_uploadReq(t, h, "/products/"+prod.ID+"/image", &claims, false,
		[]byte("definitely not an image, just text pretending"))
	s4p_status(t, rec, http.StatusBadRequest)
	if msg := s4p_message(t, rec); msg !=
		"Validation failed (current file type is image/png, expected type is .(png|jpeg|jpg))" {
		t.Fatalf("type message = %q", msg)
	}
}

func TestS4pImagesDedupAndLegacyNullHashRows(t *testing.T) {
	e := s4p_start(t)
	biz := e.F.Business()
	claims := auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID}
	stub := &s4p_stubStorage{}
	h := s4p_productsHandler(e, stub, &s4p_recordingEmbedder{})

	prod := e.F.Product(biz.ID)
	x := s4p_png("X")

	// First upload lands one row.
	s4p_status(t, s4p_uploadReq(t, h, "/products/"+prod.ID+"/images", &claims, true, x),
		http.StatusCreated)
	if u, _, _ := stub.snapshot(); u != 1 {
		t.Fatalf("uploads after first = %d", u)
	}

	// Idempotent retry — same bytes, NO second Cloudinary call, no error.
	rec := s4p_uploadReq(t, h, "/products/"+prod.ID+"/images", &claims, true, x)
	s4p_status(t, rec, http.StatusCreated)
	if u, _, _ := stub.snapshot(); u != 1 {
		t.Fatalf("duplicate upload reached cloudinary: uploads=%d", u)
	}
	if n := s4p_imageCount(t, e, prod.ID); n != 1 {
		t.Fatalf("rows after duplicate = %d", n)
	}

	// Mixed batch: X already stored (pre-upload skip), Y fresh.
	y := s4p_png("Y")
	s4p_status(t, s4p_uploadReq(t, h, "/products/"+prod.ID+"/images", &claims, true, x, y),
		http.StatusCreated)
	if u, _, _ := stub.snapshot(); u != 2 {
		t.Fatalf("mixed batch uploads = %d, want 2 (X skipped)", u)
	}
	if n := s4p_imageCount(t, e, prod.ID); n != 2 {
		t.Fatalf("rows after mixed = %d", n)
	}

	// Within-request duplicates collapse to one upload.
	z := s4p_png("Z")
	s4p_status(t, s4p_uploadReq(t, h, "/products/"+prod.ID+"/images", &claims, true, z, z),
		http.StatusCreated)
	if u, _, _ := stub.snapshot(); u != 3 {
		t.Fatalf("intra-request dup uploads = %d, want 3 cumulative", u)
	}
	if n := s4p_imageCount(t, e, prod.ID); n != 3 {
		t.Fatalf("rows after intra-dup = %d", n)
	}

	// Legacy rows with NULL hashes coexist (NULLs distinct) and never match
	// the pre-check.
	legacyProd := e.F.Product(biz.ID)
	s4p_seedImage(t, e, "legacy-1", legacyProd.ID, "https://old.example/1.jpg", "", "", 0)
	s4p_seedImage(t, e, "legacy-2", legacyProd.ID, "https://old.example/2.jpg", "", "", 1)
	w := s4p_png("W")
	s4p_status(t, s4p_uploadReq(t, h, "/products/"+legacyProd.ID+"/images", &claims, true, w),
		http.StatusCreated)
	if n := s4p_imageCount(t, e, legacyProd.ID); n != 3 {
		t.Fatalf("null-hash coexistence broken: %d != 3", n)
	}

	// RACE BACKSTOP — concurrent identical uploads: unique
	// (product_id, content_hash) lets exactly one insert win; losers take the
	// skip-success path. Rows stay deduped and positions stay dense.
	raceProd := e.F.Product(biz.ID)
	r := s4p_png("RACE")
	const racers = 6
	var wg sync.WaitGroup
	codes := make([]int, racers)
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			codes[i] = s4p_uploadReq(t, h, "/products/"+raceProd.ID+"/images",
				&claims, true, r).Code
		}(i)
	}
	wg.Wait()
	for i, c := range codes {
		if c >= 500 || c == 400 || c == 409 {
			t.Fatalf("racer %d got status %d", i, c)
		}
	}
	if n := s4p_imageCount(t, e, raceProd.ID); n != 1 {
		t.Fatalf("race produced %d rows, want 1", n)
	}
	s4p_assertDensePositions(t, e, raceProd.ID)
}

func TestS4pImagesRemoveRedensifyAndReorder(t *testing.T) {
	e := s4p_start(t)
	biz := e.F.Business()
	claims := auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID}
	stub := &s4p_stubStorage{}
	h := s4p_productsHandler(e, stub, &s4p_recordingEmbedder{})

	prod := e.F.Product(biz.ID)
	ids := make([]string, 3)
	for i := range ids {
		id := s4p_newUUID() // must be a real UUID: reorder bodies are zod-validated
		s4p_seedImage(t, e, id, prod.ID,
			fmt.Sprintf("https://res.example/%d.png", i),
			fmt.Sprintf("key/%d", i), s4p_sha([]byte{byte(i)}), i)
		ids[i] = id
	}

	// REMOVE middle -> positions re-densify ascending; asset cleanup fires once.
	rec := s4p_do(t, h, http.MethodDelete, "/products/"+prod.ID+"/images/"+ids[1], &claims, "", nil)
	s4p_status(t, rec, http.StatusOK)
	if ps := s4p_positions(t, e, prod.ID); len(ps) != 2 || ps[0] != 0 || ps[1] != 1 {
		t.Fatalf("redensify failed: %v", ps)
	}
	if _, d, _ := stub.snapshot(); d != 1 {
		t.Fatalf("asset deletes = %d, want 1", d)
	}

	// Scope guard: another product's image id must 404, not delete.
	other := e.F.Product(biz.ID)
	otherImgID := s4p_newUUID()
	s4p_seedImage(t, e, otherImgID, other.ID, "https://x/y.png", "other/key",
		s4p_sha([]byte("other")), 0)
	rec = s4p_do(t, h, http.MethodDelete, "/products/"+prod.ID+"/images/"+otherImgID, &claims, "", nil)
	s4p_status(t, rec, http.StatusNotFound)
	if msg := s4p_message(t, rec); msg != "Product image not found" {
		t.Fatalf("remove scope message = %q", msg)
	}
	rec = s4p_do(t, h, http.MethodDelete, "/products/prod-nope/images/x", &claims, "", nil)
	s4p_status(t, rec, http.StatusNotFound)
	if msg := s4p_message(t, rec); msg != "Product not found" {
		t.Fatalf("remove product-scope message = %q", msg)
	}

	// REORDER full set — explicit order wins, positions rewritten densely.
	remaining := []string{ids[2], ids[0]}
	reorder := func(order []string) int {
		body, _ := json.Marshal(map[string]any{"imageIds": order})
		return s4p_json(t, h, http.MethodPut, "/products/"+prod.ID+"/images/reorder", &claims,
			json.RawMessage(body)).Code
	}
	if code := reorder(remaining); code != http.StatusOK {
		t.Fatalf("reorder status = %d", code)
	}
	rows, err := e.DB.Query(`SELECT id FROM product_images WHERE product_id=$1 ORDER BY position`, prod.ID)
	if err != nil {
		t.Fatalf("query order: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var gotOrder []string
	for rows.Next() {
		var id string
		_ = rows.Scan(&id)
		gotOrder = append(gotOrder, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	if len(gotOrder) != 2 || gotOrder[0] != ids[2] || gotOrder[1] != ids[0] {
		t.Fatalf("reorder not applied: %v", gotOrder)
	}

	// Partial list rejected with the exact service message.
	if code := reorder([]string{ids[2]}); code != http.StatusBadRequest {
		t.Fatalf("partial reorder status = %d", code)
	}
	rec = s4p_json(t, h, http.MethodPut, "/products/"+prod.ID+"/images/reorder", &claims,
		map[string]any{"imageIds": []string{ids[2]}})
	if msg := s4p_message(t, rec); msg != "imageIds must list every image of this product exactly once (product has 2, received 1)." {
		t.Fatalf("partial reorder message = %q", msg)
	}

	// ZOD shapes on the reorder body.
	zodCases := []struct {
		name string
		body map[string]any
		code string
		path any
		msg  string
	}{
		{"duplicate ids", map[string]any{"imageIds": []string{ids[0], ids[0]}},
			"custom", "imageIds", "imageIds must not contain duplicates"},
		{"bad uuid", map[string]any{"imageIds": []string{"nope"}},
			"invalid_format", []any{"imageIds", "0"}, "Invalid UUID"},
		{"empty list", map[string]any{"imageIds": []string{}},
			"too_small", "imageIds", "Too small: expected array to have >=1 items"},
	}
	for _, tc := range zodCases {
		t.Run(tc.name, func(t *testing.T) {
			// pad to a full set where needed so we reach the intended issue
			payload := tc.body
			raw, _ := json.Marshal(payload)
			rec := s4p_json(t, h, http.MethodPut, "/products/"+prod.ID+"/images/reorder", &claims,
				json.RawMessage(raw))
			s4p_status(t, rec, http.StatusBadRequest)
			issues := s4p_issues(t, rec)
			if len(issues) != 1 {
				t.Fatalf("want 1 issue, got %s", rec.Body.String())
			}
			is := issues[0]
			if s4p_str(is, "code") != tc.code || s4p_str(is, "message") != tc.msg {
				t.Fatalf("issue mismatch: %s", rec.Body.String())
			}
			switch want := tc.path.(type) {
			case string:
				p, _ := is["path"].([]any)
				if len(p) != 1 || p[0] != want {
					t.Fatalf("path mismatch: %s", rec.Body.String())
				}
			case []any:
				p, _ := is["path"].([]any)
				if fmt.Sprint(p) != fmt.Sprint(want) {
					t.Fatalf("path mismatch: %s (%v vs %v)", rec.Body.String(), p, want)
				}
			}
		})
	}

	// T4.23a — concurrent reorders: both writers park/write inside one tx;
	// afterwards positions are dense 0..n-1 over the same id set.
	raceProd := e.F.Product(biz.ID)
	raceIDs := make([]string, 5)
	for i := range raceIDs {
		id := s4p_newUUID()
		s4p_seedImage(t, e, id, raceProd.ID,
			fmt.Sprintf("https://r/%d", i), "", s4p_sha([]byte(fmt.Sprint(i))), i)
		raceIDs[i] = id
	}
	fwd := append([]string(nil), raceIDs...)
	rev := []string{raceIDs[4], raceIDs[3], raceIDs[2], raceIDs[1], raceIDs[0]}
	var wg sync.WaitGroup
	statuses := make(chan int, 40)
	for round := 0; round < 20; round++ {
		wg.Add(2)
		go func(order []string) {
			defer wg.Done()
			body, _ := json.Marshal(map[string]any{"imageIds": order})
			statuses <- s4p_json(t, h, http.MethodPut,
				"/products/"+raceProd.ID+"/images/reorder", &claims, json.RawMessage(body)).Code
		}(fwd)
		go func(order []string) {
			defer wg.Done()
			body, _ := json.Marshal(map[string]any{"imageIds": order})
			statuses <- s4p_json(t, h, http.MethodPut,
				"/products/"+raceProd.ID+"/images/reorder", &claims, json.RawMessage(body)).Code
		}(rev)
	}
	wg.Wait()
	close(statuses)
	for code := range statuses {
		if code != http.StatusOK {
			t.Fatalf("concurrent reorder returned %d", code)
		}
	}
	ps := s4p_positions(t, e, raceProd.ID)
	if len(ps) != 5 {
		t.Fatalf("post-race count = %d", len(ps))
	}
	for i, p := range ps {
		if p != i {
			t.Fatalf("post-race positions not dense: %v", ps)
		}
	}
	var finalOrder []string
	rows2, err := e.DB.Query(`SELECT id FROM product_images WHERE product_id=$1 ORDER BY position`, raceProd.ID)
	if err != nil {
		t.Fatalf("query final order: %v", err)
	}
	defer func() { _ = rows2.Close() }()
	for rows2.Next() {
		var id string
		_ = rows2.Scan(&id)
		finalOrder = append(finalOrder, id)
	}
	if err := rows2.Err(); err != nil {
		t.Fatalf("rows2: %v", err)
	}
	setOK := func(a, b []string) bool {
		if len(a) != len(b) {
			return false
		}
		counts := map[string]int{}
		for _, s := range a {
			counts[s]++
		}
		for _, s := range b {
			counts[s]--
		}
		for _, v := range counts {
			if v != 0 {
				return false
			}
		}
		return true
	}
	if !setOK(finalOrder, raceIDs) {
		t.Fatalf("post-race set drifted: %v", finalOrder)
	}
}

// TestS4pCloudinarySignedRequestsEndToEnd wires the REAL provider against a
// local stub server and asserts the signed request shape + folder contract.
func TestS4pCloudinarySignedRequestsEndToEnd(t *testing.T) {
	e := s4p_start(t)
	biz := e.F.Business()
	claims := auth.Claims{Phone: biz.OwnerPhone, BusinessID: biz.ID}

	const (
		cloud   = "test-cloud"
		apiKey  = "k-test"
		secret  = "s-test-secret"
		fixedTS = int64(1755850000)
	)
	wantSigUpload := func(folder string) string {
		// SDK signature = sha1(sortedParams + apiSecret), NOT an HMAC.
		sum := sha1.Sum([]byte("folder=" + folder + "&timestamp=" + strconv.FormatInt(fixedTS, 10) + secret))
		return hex.EncodeToString(sum[:])
	}

	var mu sync.Mutex
	var uploads, destroys int
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		switch r.URL.Path {
		case "/" + cloud + "/image/upload":
			uploads++
			if err := r.ParseMultipartForm(10 << 20); err != nil {
				http.Error(w, `{"error":{"message":"parse"}}`, http.StatusBadRequest)
				return
			}
			form := r.MultipartForm.Value // multipart fields land here
			get := func(k string) string {
				if v, ok := form[k]; ok && len(v) > 0 {
					return v[0]
				}
				return ""
			}
			if get("api_key") != apiKey || get("folder") != "novoapex/products/"+biz.ID ||
				get("timestamp") != strconv.FormatInt(fixedTS, 10) ||
				get("signature") != wantSigUpload("novoapex/products/"+biz.ID) {
				http.Error(w, `{"error":{"message":"signature mismatch"}}`, http.StatusBadRequest)
				return
			}
			_, _ = fmt.Fprintf(w, `{"secure_url":"https://res.example/up/%d.png","public_id":"up/%d"}`, uploads, uploads)
		case "/" + cloud + "/image/destroy":
			destroys++
			publicID := r.PostFormValue("public_id")
			sum := sha1.Sum([]byte("public_id=" + publicID + "&timestamp=" + strconv.FormatInt(fixedTS, 10) + secret))
			if r.PostFormValue("signature") != hex.EncodeToString(sum[:]) {
				http.Error(w, `{"error":{"message":"sig"}}`, http.StatusBadRequest)
				return
			}
			_, _ = fmt.Fprint(w, `{"result":"ok"}`)
		default:
			http.NotFound(w, r)
		}
	}))
	t.Cleanup(ts.Close)

	var provider storage.StorageProvider = cloudinary.New(cloudinary.Options{
		CloudName: cloud, APIKey: apiKey, APISecret: secret,
		BaseURL: ts.URL,
		Now:     func() time.Time { return time.Unix(fixedTS, 0) },
	})

	emb := &s4p_recordingEmbedder{}
	h := s4p_productsHandler(e, provider, emb)
	prod := e.F.Product(biz.ID)

	rec := s4p_uploadReq(t, h, "/products/"+prod.ID+"/images", &claims, true, s4p_png("REAL"))
	s4p_status(t, rec, http.StatusCreated)
	body := s4p_decode(t, rec)
	imgs, _ := body["images"].([]any)
	if len(imgs) != 1 || s4p_str(imgs[0].(map[string]any), "url") != "https://res.example/up/1.png" ||
		s4p_str(imgs[0].(map[string]any), "storageKey") != "up/1" {
		t.Fatalf("cloudinary-backed upload payload wrong: %s", rec.Body.String())
	}
	mu.Lock()
	if uploads != 1 || destroys != 0 {
		mu.Unlock()
		t.Fatalf("stub calls uploads=%d destroys=%d", uploads, destroys)
	}
	mu.Unlock()

	// Product delete triggers best-effort destroy with a valid signature.
	s4p_status(t, s4p_json(t, h, http.MethodDelete, "/products/"+prod.ID, &claims, nil), http.StatusOK)
	mu.Lock()
	defer mu.Unlock()
	if destroys != 1 {
		t.Fatalf("destroy calls = %d, want 1", destroys)
	}
}
