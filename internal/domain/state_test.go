package domain

import (
	"context"
	"database/sql"
	"fmt"
	"net"
	"net/http"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/jackc/pgx/v5/stdlib"

	harness "github.com/novoapex/novoapex-backend-api/internal/harness"
)

// s7bExpectedTransitions is the literal VALID_TRANSITIONS table from
// libs/orchestrator/src/conversation-state.service.ts:23-30 — the oracle this
// port must reproduce exactly.
var s7bExpectedTransitions = map[ConversationState][]ConversationState{
	StateLead:      {StateBrowsing, StateCheckout, StateSupport, StateEscalated},
	StateBrowsing:  {StateCheckout, StateSupport, StateInvoicing, StateEscalated},
	StateCheckout:  {StateInvoicing, StateSupport, StateEscalated},
	StateInvoicing: {StateSupport, StateEscalated},
	StateSupport:   {StateBrowsing, StateLead, StateEscalated},
	StateEscalated: {}, // terminal
}

var allStates = []ConversationState{
	StateLead, StateBrowsing, StateCheckout, StateInvoicing, StateSupport, StateEscalated,
}

// TestS7b_TransitionTableMatchesSource pins the exported table against the
// verbatim TS map (order-sensitive) and ValidateTransition over every pair.
func TestS7b_TransitionTableMatchesSource(t *testing.T) {
	if len(ValidTransitions) != len(s7bExpectedTransitions) {
		t.Fatalf("ValidTransitions has %d keys, want %d", len(ValidTransitions), len(s7bExpectedTransitions))
	}
	for from, want := range s7bExpectedTransitions {
		got, ok := ValidTransitions[from]
		if !ok {
			t.Fatalf("ValidTransitions missing key %q", from)
		}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("ValidTransitions[%q] = %v, want %v", from, got, want)
		}
	}

	for _, from := range allStates {
		for _, to := range allStates {
			wantLegality := false
			for _, allowed := range s7bExpectedTransitions[from] {
				if allowed == to {
					wantLegality = true
					break
				}
			}
			if got := ValidateTransition(from, to); got != wantLegality {
				t.Errorf("ValidateTransition(%s, %s) = %v, want %v", from, to, got, wantLegality)
			}
		}
	}

	if ValidateTransition("NOT_A_STATE", StateLead) {
		t.Error(`ValidateTransition("NOT_A_STATE", LEAD) must reject unknown states`)
	}
}

// s7bSTMRow mirrors one row of the 17-row characterization table
// (internal/harness/characterization_statemachine_test.go TestT012): the state
// a conversation starts in, what the pipeline attempts, and where reality says
// it must end up.
type s7bSTMRow struct {
	from    ConversationState
	attempt ConversationState // transition attempted by the pipeline; "" means none (guard blocked / flag only)
	toWant  ConversationState
	class   string
	success bool // expected Transition() result when attempt != ""
}

func s7bStatemachineRows() []s7bSTMRow {
	return []s7bSTMRow{
		{from: StateLead, attempt: StateBrowsing, toWant: StateBrowsing, class: "legal-edge", success: true},
		{from: StateLead, attempt: StateCheckout, toWant: StateCheckout, class: "legal-edge", success: true},
		{from: StateLead, attempt: StateSupport, toWant: StateSupport, class: "legal-edge", success: true},
		{from: StateBrowsing, attempt: StateCheckout, toWant: StateCheckout, class: "legal-edge", success: true},
		{from: StateBrowsing, attempt: StateSupport, toWant: StateSupport, class: "legal-edge", success: true},
		{from: StateBrowsing, attempt: StateInvoicing, toWant: StateInvoicing, class: "legal-edge", success: true},
		{from: StateCheckout, attempt: StateInvoicing, toWant: StateInvoicing, class: "legal-edge", success: true},
		// Table rejects: pipeline wants INVOICING but LEAD/SUPPORT have no edge to it.
		{from: StateLead, attempt: StateInvoicing, toWant: StateLead, class: "table-rejects"},
		{from: StateSupport, attempt: StateInvoicing, toWant: StateSupport, class: "table-rejects"},
		// Orchestrator guard blocks the LLM flow before any transition attempt.
		{from: StateCheckout, toWant: StateCheckout, class: "guard-blocked"},
		{from: StateInvoicing, toWant: StateInvoicing, class: "guard-blocked"},
		{from: StateInvoicing, toWant: StateInvoicing, class: "guard-blocked"},
		{from: StateSupport, toWant: StateSupport, class: "guard-blocked"},
		// ESCALATED is terminal: any attempted move is rejected.
		{from: StateEscalated, attempt: StateBrowsing, toWant: StateEscalated, class: "terminal-guard-blocked"},
		{from: StateEscalated, attempt: StateInvoicing, toWant: StateEscalated, class: "terminal-guard-blocked"},
		// Low-confidence escalation sets a flag only; never a transition.
		{from: StateLead, toWant: StateLead, class: "escalation-flag-only"},
		{from: StateCheckout, toWant: StateCheckout, class: "escalation-flag-only"},
	}
}

func s7bDockerAlive(t *testing.T) {
	t.Helper()
	sock := os.Getenv("DOCKER_HOST")
	if sock == "" || !strings.HasPrefix(sock, "unix://") {
		sock = "unix:///var/run/docker.sock"
	}
	path := strings.TrimPrefix(sock, "unix://")
	client := &http.Client{
		Transport: &http.Transport{
			DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
				var d net.Dialer
				return d.DialContext(ctx, "unix", path)
			},
		},
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://localhost/_ping", nil)
	if err != nil {
		t.Skipf("docker ping request: %v", err)
	}
	resp, err := client.Do(req)
	if err != nil {
		t.Skipf("docker daemon unavailable at %s: %v", path, err)
	}
	defer func() { _ = resp.Body.Close() }()
}

type s7bDB struct {
	pool    *pgxpool.Pool
	db      *sql.DB
	factory *harness.Factory
}

func s7bStartDB(t *testing.T) *s7bDB {
	t.Helper()
	s7bDockerAlive(t)
	ctx := t.Context()

	env, err := harness.Start(ctx)
	if err != nil {
		t.Fatalf("start harness postgres: %v", err)
	}
	t.Cleanup(func() { env.Terminate(context.Background()) })

	if err := harness.ApplyBaselineSchema(ctx, env.PostgresDSN); err != nil {
		t.Fatalf("apply schema: %v", err)
	}

	pool, err := pgxpool.New(ctx, env.PostgresDSN)
	if err != nil {
		t.Fatalf("open pgx pool: %v", err)
	}
	t.Cleanup(pool.Close)

	db, err := sql.Open("pgx", env.PostgresDSN)
	if err != nil {
		t.Fatalf("open stdlib db: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	return &s7bDB{pool: pool, db: db, factory: harness.NewFactory(t, db)}
}

// TestS7b_TransitionCASAgainstDatabase runs every characterized row through
// Transition() against a real conversations row, asserting both the boolean
// outcome and the final persisted state.
func TestS7b_TransitionCASAgainstDatabase(t *testing.T) {
	env := s7bStartDB(t)
	biz := env.factory.Business()

	for i, row := range s7bStatemachineRows() {
		t.Run(fmt.Sprintf("%02d_%s_to_%s_%s", i+1, row.from, row.toWant, row.class), func(t *testing.T) {
			conv := env.factory.Conversation(biz.ID, fmt.Sprintf("+2337%09d", i), harness.WithState(string(row.from)))

			if row.attempt == "" {
				// Guard/flag rows: nothing transitions, state must hold.
				var state string
				if err := env.db.QueryRow(`SELECT state::text FROM conversations WHERE id = $1`, conv.ID).Scan(&state); err != nil {
					t.Fatalf("read state: %v", err)
				}
				if state != string(row.from) {
					t.Fatalf("state drifted to %q without an attempt, want %q", state, row.from)
				}
				return
			}

			ok, err := Transition(t.Context(), env.pool, conv.ID, row.from, row.attempt)
			if err != nil {
				t.Fatalf("Transition(%s -> %s): %v", row.from, row.attempt, err)
			}

			if row.class == "legal-edge" {
				if !ok {
					t.Fatalf("legal-edge transition %s -> %s was rejected", row.from, row.attempt)
				}
			} else if ok {
				t.Fatalf("%s: transition %s -> %s must be rejected (stale-write/table guard)", row.class, row.from, row.attempt)
			}

			var state string
			if err := env.db.QueryRow(`SELECT state::text FROM conversations WHERE id = $1`, conv.ID).Scan(&state); err != nil {
				t.Fatalf("read state: %v", err)
			}
			if state != string(row.toWant) {
				t.Errorf("final state = %q, want characterized %q", state, row.toWant)
			}
		})
	}
}

// TestS7b_ConcurrentSameFromExactlyOneWinner is T0.12c finally validated here:
// two racing Transitions with the same `from` on the same conversation — CAS
// must admit exactly one.
func TestS7b_ConcurrentSameFromExactlyOneWinner(t *testing.T) {
	env := s7bStartDB(t)
	biz := env.factory.Business()
	conv := env.factory.Conversation(biz.ID, "+233770000001", harness.WithState(string(StateCheckout)))

	const racers = 2
	results := make(chan bool, racers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < racers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			ok, err := Transition(context.Background(), env.pool, conv.ID, StateCheckout, StateInvoicing)
			if err != nil {
				t.Errorf("racer Transition error: %v", err)
				return
			}
			results <- ok
		}()
	}
	close(start)
	wg.Wait()
	close(results)

	wins := 0
	for ok := range results {
		if ok {
			wins++
		}
	}
	if wins != 1 {
		t.Fatalf("concurrent same-from Transitions produced %d winners, want exactly 1", wins)
	}

	var state string
	if err := env.db.QueryRow(`SELECT state::text FROM conversations WHERE id = $1`, conv.ID).Scan(&state); err != nil {
		t.Fatalf("read final state: %v", err)
	}
	if state != string(StateInvoicing) {
		t.Errorf("final state = %q, want INVOICING", state)
	}
}
