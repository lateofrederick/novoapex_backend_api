package harness

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	"github.com/novoapex/novoapex-backend-api/internal/orchestrator"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
	"github.com/novoapex/novoapex-backend-api/internal/workers"
)

func trnNodeDistAvailable(t *testing.T) bool {
	t.Helper()
	repoDir, err := NovoApexRepoDir()
	if err != nil {
		return false
	}
	for _, bin := range []string{
		filepath.Join(repoDir, "dist/apps/api/apps/api/src/main.js"),
		filepath.Join(repoDir, "dist/apps/worker/apps/worker/src/main.js"),
	} {
		if _, err := os.Stat(bin); err != nil {
			return false
		}
	}
	return true
}

func trnHappyPathScenario() Scenario {
	return Scenario{
		Name:     "trnhappy",
		Products: []ProductSpec{{Name: "Golden Gadget", Price: "25.50", ImageCount: 1, Stock: 5}},
		Turns: []Turn{
			{
				InboundText: "Do you have the Golden Gadget in stock?",
				LLM: LLMStep{
					ReplyText:         "Yes! We have that in stock.",
					Confidence:        0.95,
					Intent:            "product_inquiry",
					ReferencedIndexes: []int{0},
				},
				WantState: "BROWSING",
			},
			{
				InboundText: "Yes please confirm my order: 2 of the Golden Gadget",
				LLM: LLMStep{
					ReplyText:      "Great! Your order is confirmed. Total: GHS 51.00.",
					Confidence:     0.95,
					Intent:         "product_inquiry",
					OrderConfirmed: true,
					Items:          []Item{{ProductIndex: 0, Quantity: 2}},
					DeliveryArea:   "Osu",
					Sentiment:      "positive",
				},
				WantState:  "INVOICING",
				WantOrders: 1,
			},
		},
	}
}

// trnScriptedLLM replays the scenario's decision script against the Go
// pipeline with the provisioned short IDs.
type trnScriptedLLM struct {
	steps    []Turn
	shortIDs []string
	call     int
}

func (l *trnScriptedLLM) GenerateObject(context.Context, orchestrator.GenerateRequest) (orchestrator.LlmResponse, error) {
	step := l.steps[l.call].LLM
	l.call++
	resp := orchestrator.LlmResponse{
		ReplyText:               step.ReplyText,
		InternalConfidence:      step.Confidence,
		Intent:                  step.Intent,
		CustomerRequestedImages: step.CustomerRequestedImages,
		SendProductImageIDs:     []string{},
		ReferencedProductIDs:    []string{},
		CrmSignals: orchestrator.CrmSignals{
			DetectedItems:       []orchestrator.DetectedItem{},
			DetectedPreferences: []string{},
			Sentiment:           cmpOr(step.Sentiment, "neutral"),
		},
	}
	for _, idx := range step.SendImageIndexes {
		resp.SendProductImageIDs = append(resp.SendProductImageIDs, l.shortIDs[idx])
	}
	for _, idx := range step.ReferencedIndexes {
		resp.ReferencedProductIDs = append(resp.ReferencedProductIDs, l.shortIDs[idx])
	}
	for _, it := range step.Items {
		resp.CrmSignals.DetectedItems = append(resp.CrmSignals.DetectedItems,
			orchestrator.DetectedItem{ProductID: l.shortIDs[it.ProductIndex], Quantity: it.Quantity})
	}
	resp.CrmSignals.OrderConfirmed = step.OrderConfirmed
	if step.DeliveryArea != "" {
		area := step.DeliveryArea
		resp.CrmSignals.DeliveryArea = &area
	}
	return resp, nil
}

type trnNullPublisher struct{}

func (trnNullPublisher) Enqueue(context.Context, string, string, any, *queue.EnqueueOpts) error {
	return nil // transcript driver captures/drains CRM internally
}

type trnPaystack struct{}

func (trnPaystack) InitiatePayment(context.Context, workers.PaymentRequest) (workers.PaymentLink, error) {
	return workers.PaymentLink{
		Status:            "initiated",
		ProviderReference: "ref-trn-fixed",
		PaymentURL:        "https://checkout.paystack.test/pay/ref-trn-fixed",
	}, nil
}

// trnGoDriverFactory wires workers.TranscriptWebhookDriver once the Go side's
// business has been provisioned (the scripted LLM needs its product short IDs).
func trnGoDriverFactory(dsn string, sc Scenario) func(GoStackInfo) (func(ctx context.Context, payload []byte) error, error) {
	return func(info GoStackInfo) (func(ctx context.Context, payload []byte) error, error) {
		pool, err := pgxpool.New(context.Background(), dsn)
		if err != nil {
			return nil, fmt.Errorf("open pool for go driver: %w", err)
		}
		orchDeps := workers.OrchestratorDeps{
			Pool:      pool,
			Publisher: trnNullPublisher{},
			LLM:       &trnScriptedLLM{steps: sc.Turns, shortIDs: info.ShortIDs},
		}
		crmDeps := workers.CRMDeps{Pool: pool, Publisher: trnNullPublisher{}, Paystack: trnPaystack{}}
		drive := workers.TranscriptWebhookDriver(orchDeps, crmDeps)
		// Pool intentionally outlives all turns of this stack; it dies with
		// the test process (containers are terminated by t.Cleanup anyway).
		return drive, nil
	}
}

// TestTrnGoldenHappyPathZeroDiffs replays the two-turn order happy path and
// requires ZERO decision diffs — against the real Node stack when its dist
// build exists, otherwise as a Go-driver self round-trip (run twice, diffed).
func TestTrnGoldenHappyPathZeroDiffs(t *testing.T) {
	h := startHarness(t)
	repoDir := harnessRepoDir(t)
	applyMigrations(t, h.PostgresDSN, repoDir)

	sc := trnHappyPathScenario()
	opts := TranscriptRunnerOpts{GoDriveFactory: trnGoDriverFactory(h.PostgresDSN, sc)}

	nodeRan := trnNodeDistAvailable(t)
	if nodeRan {
		opts.Node = StartNodeStack(t, h)
	} else {
		t.Log("node dist missing — running Go self round-trip only")
	}

	nodeVecs, goVecs, diffs := RunGoldenTranscript(t, h, sc, opts)
	if len(diffs) > 0 {
		t.Fatalf("golden transcript decision diffs (%d):\n%s", len(diffs), fmt.Sprint(diffs))
	}
	if len(goVecs) != 2 {
		t.Fatalf("go vectors = %d turns, want 2", len(goVecs))
	}

	// explicit spot-checks of the decision surface
	if goVecs[0].Intent != "product_inquiry" || goVecs[0].StateTransition != "LEAD->BROWSING" ||
		goVecs[0].Escalated || len(goVecs[0].ImageIDsSent) != 0 ||
		goVecs[0].CRMSignals != "sentiment=neutral" || goVecs[0].ReplyLenBucket != "s" {
		t.Errorf("turn1 vector: %+v", goVecs[0])
	}
	if goVecs[1].StateTransition != "BROWSING->INVOICING" || goVecs[1].Escalated ||
		goVecs[1].CRMSignals == "" || goVecs[1].CRMSignals == "-" {
		t.Errorf("turn2 vector: %+v", goVecs[1])
	}
	if !contains(goVecs[1].CRMSignals, "orders=1") || !contains(goVecs[1].CRMSignals, "total=51.00") {
		t.Errorf("turn2 crm vector missing order: %+v", goVecs[1].CRMSignals)
	}
	if nodeRan && len(nodeVecs) != 2 {
		t.Errorf("node vectors = %d turns, want 2", len(nodeVecs))
	}
}

func cmpOr(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}

func contains(haystack, needle string) bool {
	return len(haystack) >= len(needle) && (haystack == needle ||
		len(needle) == 0 ||
		indexOf(haystack, needle) >= 0)
}

func indexOf(haystack, needle string) int {
	for i := 0; i+len(needle) <= len(haystack); i++ {
		if haystack[i:i+len(needle)] == needle {
			return i
		}
	}
	return -1
}
