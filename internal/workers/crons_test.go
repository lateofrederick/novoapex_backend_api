package workers

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
)

func TestS7A_CronSpecs_ThreeSingletonCrons(t *testing.T) {
	specs := CronSpecs()
	want := []struct{ spec, cron string }{
		{"follow-up-scanner", "*/1 * * * *"},
		{"outbox-sweep", "*/2 * * * *"},
		{"retention-scanner", "0 9 * * *"},
	}
	if len(specs) != len(want) {
		t.Fatalf("specs = %d, want %d", len(specs), len(want))
	}
	taskTypes := map[string]bool{}
	for i, sp := range specs {
		if sp.Spec != want[i].spec || sp.Cron != want[i].cron {
			t.Errorf("spec[%d] = {%s %s}, want {%s %s}", i, sp.Spec, sp.Cron, want[i].spec, want[i].cron)
		}
		if sp.TaskType == "" {
			t.Errorf("spec[%d] TaskType empty", i)
		}
		if taskTypes[sp.TaskType] {
			t.Errorf("duplicate TaskType %q", sp.TaskType)
		}
		taskTypes[sp.TaskType] = true
	}
}

func TestS7A_SweepFollowUps_ClaimsDueRowsOnly(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	biz := factory.Business()
	cust := factory.Customer(biz.ID)
	orderID := s7a_seedOrder(t, s.db, biz.ID, cust.ID, "", "CONFIRMED", "10.00")

	dueA := s7a_seedFollowUp(t, s.db, orderID, biz.ID, cust.ID, "abandoned-cart", time.Now().Add(-1*time.Hour))
	dueB := s7a_seedFollowUp(t, s.db, orderID, biz.ID, cust.ID, "unpaid-invoice-first", time.Now().Add(-2*time.Hour))
	future := s7a_seedFollowUp(t, s.db, orderID, biz.ID, cust.ID, "abandoned-cart", time.Now().Add(1*time.Hour))
	cancelled := s7a_seedFollowUp(t, s.db, orderID, biz.ID, cust.ID, "abandoned-cart", time.Now().Add(-1*time.Hour))
	executed := s7a_seedFollowUp(t, s.db, orderID, biz.ID, cust.ID, "abandoned-cart", time.Now().Add(-1*time.Hour))
	now := time.Now().UTC()
	for _, pair := range []struct {
		id   string
		col  string
		when time.Time
	}{{cancelled, "cancelled_at", now}, {executed, "executed_at", now}} {
		if _, err := s.db.Exec(fmt.Sprintf(`UPDATE scheduled_follow_ups SET %s = $2 WHERE id = $1`, pair.col), pair.id, pair.when); err != nil {
			t.Fatalf("seed %s: %v", pair.col, err)
		}
	}

	if err := SweepFollowUps(t.Context(), s7a_deps(s, pub)); err != nil {
		t.Fatalf("SweepFollowUps: %v", err)
	}

	calls := pub.recorded()
	if len(calls) != 2 {
		t.Fatalf("enqueues = %d, want 2 (due rows only)", len(calls))
	}
	gotIDs := map[string]bool{}
	for _, c := range calls {
		if c.Queue != queue.QFollowUp || c.TaskType != queue.TaskFollowUpRun {
			t.Errorf("enqueue target = %s/%s, want %s/%s", c.Queue, c.TaskType, queue.QFollowUp, queue.TaskFollowUpRun)
		}
		var fp FollowUpPayload
		if err := json.Unmarshal(c.Payload, &fp); err != nil {
			t.Fatalf("decode follow-up payload: %v", err)
		}
		if fp.OrderID != orderID || fp.BusinessID != biz.ID || fp.CustomerID != cust.ID {
			t.Errorf("payload = %+v, want seeded ids", fp)
		}
		if fp.JobType == "" {
			t.Error("payload job_type empty")
		}
		if c.Opts == nil || !strings.HasPrefix(c.Opts.TaskID, "scheduled-follow-up:sfu_") {
			t.Errorf("TaskID missing or malformed: %+v", c.Opts)
		}
		gotIDs[c.Opts.TaskID] = true
	}
	if len(gotIDs) != 2 {
		t.Errorf("distinct task ids = %d, want 2", len(gotIDs))
	}

	for _, id := range []string{dueA, dueB} {
		if exec := s7a_scalar(t, s.db, `SELECT COALESCE(executed_at::text,'') FROM scheduled_follow_ups WHERE id = $1`, id); exec == "" {
			t.Errorf("due row %s not claimed (executed_at null)", id)
		}
	}
	if exec := s7a_scalar(t, s.db, `SELECT COALESCE(executed_at::text,'') FROM scheduled_follow_ups WHERE id = $1`, future); exec != "" {
		t.Errorf("future row %s wrongly claimed", future)
	}
	if exec := s7a_scalar(t, s.db, `SELECT COALESCE(executed_at::text,'') FROM scheduled_follow_ups WHERE id = $1`, cancelled); exec != "" {
		t.Errorf("cancelled row %s wrongly claimed", cancelled)
	}
	if exec := s7a_scalar(t, s.db, `SELECT COALESCE(executed_at::text,'') FROM scheduled_follow_ups WHERE id = $1`, executed); exec == "" {
		t.Errorf("pre-executed row %s lost its original executed_at", executed)
	}
	if n := s7a_count(t, s.db, `SELECT COUNT(*) FROM scheduled_follow_ups WHERE executed_at IS NOT NULL`); n != 3 {
		t.Errorf("claimed rows = %d, want 3 (pre-executed + two due)", n)
	}
}

func TestS7A_SweepFollowUps_SecondRunClaimsNothing(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	biz := factory.Business()
	cust := factory.Customer(biz.ID)
	orderID := s7a_seedOrder(t, s.db, biz.ID, cust.ID, "", "CONFIRMED", "10.00")
	for i := 0; i < 3; i++ {
		s7a_seedFollowUp(t, s.db, orderID, biz.ID, cust.ID, "abandoned-cart", time.Now().Add(-1*time.Hour))
	}

	if err := SweepFollowUps(t.Context(), s7a_deps(s, pub)); err != nil {
		t.Fatalf("first sweep: %v", err)
	}
	first := len(pub.recorded())
	if first != 3 {
		t.Fatalf("first sweep enqueues = %d, want 3", first)
	}
	pub.reset()
	if err := SweepFollowUps(t.Context(), s7a_deps(s, pub)); err != nil {
		t.Fatalf("second sweep: %v", err)
	}
	if got := len(pub.recorded()); got != 0 {
		t.Errorf("second sweep enqueues = %d, want 0 (CAS claim prevents double-send)", got)
	}
}

func TestS7A_SweepFollowUps_TakeFiftyCap(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	biz := factory.Business()
	cust := factory.Customer(biz.ID)
	orderID := s7a_seedOrder(t, s.db, biz.ID, cust.ID, "", "CONFIRMED", "10.00")

	for i := 0; i < 55; i++ {
		s7a_seedFollowUp(t, s.db, orderID, biz.ID, cust.ID, "abandoned-cart",
			time.Now().Add(-1*time.Hour).Add(time.Duration(i)*time.Second))
	}

	if err := SweepFollowUps(t.Context(), s7a_deps(s, pub)); err != nil {
		t.Fatalf("SweepFollowUps: %v", err)
	}
	if got := len(pub.recorded()); got != 50 {
		t.Errorf("enqueued = %d, want 50 (take:50 cap)", got)
	}
	if remaining := s7a_count(t, s.db, `SELECT COUNT(*) FROM scheduled_follow_ups WHERE executed_at IS NULL AND cancelled_at IS NULL`); remaining != 5 {
		t.Errorf("unclaimed rows = %d, want 5 (deferred to next sweep)", remaining)
	}
}

func TestS7A_SweepFollowUps_PublisherErrorStillClaimsRows(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{Fail: fmt.Errorf("redis down")}

	biz := factory.Business()
	cust := factory.Customer(biz.ID)
	orderID := s7a_seedOrder(t, s.db, biz.ID, cust.ID, "", "CONFIRMED", "10.00")
	sfu := s7a_seedFollowUp(t, s.db, orderID, biz.ID, cust.ID, "abandoned-cart", time.Now().Add(-1*time.Hour))

	if err := SweepFollowUps(t.Context(), s7a_deps(s, pub)); err != nil {
		t.Fatalf("SweepFollowUps with failing publisher = %v, want nil (log+continue)", err)
	}
	if exec := s7a_scalar(t, s.db, `SELECT COALESCE(executed_at::text,'') FROM scheduled_follow_ups WHERE id = $1`, sfu); exec == "" {
		t.Error("claimed row must keep executed_at even when enqueue failed (will not retry)")
	}
	if got := len(pub.recorded()); got != 0 {
		t.Errorf("recorded calls = %d, want 0", got)
	}
}

func TestS7A_OutboxSweep_ReenqueuesStrandedOnly(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	biz := factory.Business()
	cust := factory.Customer(biz.ID)
	conv := factory.Conversation(biz.ID, cust.Phone)

	stranded := s7a_seedOutbound(t, s.db, biz.ID, conv.ID, cust.Phone, "pending", time.Now().Add(-5*time.Minute))
	recent := s7a_seedOutbound(t, s.db, biz.ID, conv.ID, cust.Phone, "pending", time.Now().Add(-30*time.Second))
	sent := s7a_seedOutbound(t, s.db, biz.ID, conv.ID, cust.Phone, "sent", time.Now().Add(-9*time.Minute))

	if err := SweepOutbox(t.Context(), s7a_deps(s, pub)); err != nil {
		t.Fatalf("SweepOutbox: %v", err)
	}

	calls := pub.recorded()
	if len(calls) != 1 {
		t.Fatalf("re-enqueues = %d, want 1", len(calls))
	}
	c := calls[0]
	if c.Queue != queue.QOutbound || c.TaskType != queue.TaskOutboundSend {
		t.Errorf("enqueue target = %s/%s, want %s/%s", c.Queue, c.TaskType, queue.QOutbound, queue.TaskOutboundSend)
	}
	var job outboundSendJob
	if err := json.Unmarshal(c.Payload, &job); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if job.OutboundMessageID != stranded {
		t.Errorf("re-enqueued id = %s, want stranded %s", job.OutboundMessageID, stranded)
	}
	if c.Opts == nil || c.Opts.TaskID != "outbox-sweep:"+stranded {
		t.Errorf("TaskID = %+v, want outbox-sweep:<id>", c.Opts)
	}
	for _, id := range []string{recent, sent} {
		if status := s7a_scalar(t, s.db, `SELECT status FROM outbound_messages WHERE id = $1`, id); status != "pending" && status != "sent" {
			t.Errorf("row %s status changed to %s unexpectedly", id, status)
		}
	}
}

func TestS7A_OutboxSweep_MarksFailedStaleWhenNewerSentExists(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	bizA := factory.Business()
	custA := factory.Customer(bizA.ID)
	convA := factory.Conversation(bizA.ID, custA.Phone)

	bizB := factory.Business()
	custB := factory.Customer(bizB.ID)
	convB := factory.Conversation(bizB.ID, custB.Phone)

	staleStranded := s7a_seedOutbound(t, s.db, bizA.ID, convA.ID, custA.Phone, "pending", time.Now().Add(-5*time.Minute))
	s7a_seedOutbound(t, s.db, bizA.ID, convA.ID, custA.Phone, "sent", time.Now().Add(-1*time.Minute))
	freshStranded := s7a_seedOutbound(t, s.db, bizB.ID, convB.ID, custB.Phone, "pending", time.Now().Add(-4*time.Minute))

	if err := SweepOutbox(t.Context(), s7a_deps(s, pub)); err != nil {
		t.Fatalf("SweepOutbox: %v", err)
	}

	if status := s7a_scalar(t, s.db, `SELECT status FROM outbound_messages WHERE id = $1`, staleStranded); status != "failed_stale" {
		t.Errorf("stale row status = %s, want failed_stale (newer sent exists)", status)
	}
	if status := s7a_scalar(t, s.db, `SELECT status FROM outbound_messages WHERE id = $1`, freshStranded); status != "pending" {
		t.Errorf("other-conversation stranded status = %s, want pending (still queued)", status)
	}
	calls := pub.recorded()
	if len(calls) != 1 || func() bool {
		var j outboundSendJob
		_ = json.Unmarshal(calls[0].Payload, &j)
		return j.OutboundMessageID != freshStranded
	}() {
		t.Errorf("re-enqueues = %+v, want only the non-stale row %s", calls, freshStranded)
	}
}

func TestS7A_OutboxSweep_TakeHundredCap(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	biz := factory.Business()
	cust := factory.Customer(biz.ID)
	conv := factory.Conversation(biz.ID, cust.Phone)

	for i := 0; i < 105; i++ {
		s7a_seedOutbound(t, s.db, biz.ID, conv.ID, cust.Phone, "pending",
			time.Now().Add(-5*time.Minute).Add(time.Duration(i)*time.Second))
	}

	if err := SweepOutbox(t.Context(), s7a_deps(s, pub)); err != nil {
		t.Fatalf("SweepOutbox: %v", err)
	}
	if got := len(pub.recorded()); got != 100 {
		t.Errorf("re-enqueued = %d, want 100 (take:100 cap)", got)
	}

	rows, err := s.db.Query(`SELECT id FROM outbound_messages ORDER BY created_at ASC`)
	if err != nil {
		t.Fatalf("list stranded: %v", err)
	}
	defer func() { _ = rows.Close() }()
	var ordered []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err != nil {
			t.Fatalf("scan stranded: %v", err)
		}
		ordered = append(ordered, id)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("rows: %v", err)
	}
	enqueued := map[string]bool{}
	for _, c := range pub.recorded() {
		var job outboundSendJob
		if err := json.Unmarshal(c.Payload, &job); err != nil {
			t.Fatalf("decode: %v", err)
		}
		enqueued[job.OutboundMessageID] = true
	}
	for i, id := range ordered {
		want := i < 100
		if enqueued[id] != want {
			t.Errorf("row #%d (%s) enqueued=%v, want %v (oldest-first take:100)", i, id, enqueued[id], want)
		}
	}
}

func s7a_setProfile(t *testing.T, s *s7a_stack, customerID string, lastOrderAt time.Time, freqDays float64, lastReengagement *time.Time) {
	t.Helper()
	if _, err := s.db.Exec(`
		UPDATE customer_profiles
		   SET last_order_at = $2, order_frequency_days = $3,
		       last_reengagement_at = $4
		 WHERE customer_id = $1`,
		customerID, lastOrderAt.UTC(), freqDays, lastReengagement); err != nil {
		t.Fatalf("set profile: %v", err)
	}
}

func TestS7A_Retention_ThresholdMathAndCooldown(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	biz := factory.Business()
	now := time.Now().UTC()

	belowThreshold := factory.Customer(biz.ID)
	s7a_seedOrder(t, s.db, biz.ID, belowThreshold.ID, "", "DELIVERED", "10.00")
	s7a_setProfile(t, s, belowThreshold.ID, now.Add(-44*24*time.Hour), 30, nil)

	beyondThreshold := factory.Customer(biz.ID)
	s7a_seedOrder(t, s.db, biz.ID, beyondThreshold.ID, "", "DELIVERED", "10.00")
	s7a_setProfile(t, s, beyondThreshold.ID, now.Add(-46*24*time.Hour), 30, nil)

	inCooldown := factory.Customer(biz.ID)
	s7a_seedOrder(t, s.db, biz.ID, inCooldown.ID, "", "DELIVERED", "10.00")
	cooldownStamp := now.Add(-10 * 24 * time.Hour)
	s7a_setProfile(t, s, inCooldown.ID, now.Add(-90*24*time.Hour), 30, &cooldownStamp)

	outOfCooldown := factory.Customer(biz.ID)
	s7a_seedOrder(t, s.db, biz.ID, outOfCooldown.ID, "", "DELIVERED", "10.00")
	oldStamp := now.Add(-31 * 24 * time.Hour)
	s7a_setProfile(t, s, outOfCooldown.ID, now.Add(-100*24*time.Hour), 30, &oldStamp)

	deps := s7a_deps(s, pub)
	deps.ReengagementThresholdMultiplier = 1.5
	deps.ReengagementCooldownDays = 30

	if err := SweepRetention(t.Context(), deps); err != nil {
		t.Fatalf("SweepRetention: %v", err)
	}

	calls := pub.recorded()
	if len(calls) != 2 {
		t.Fatalf("enqueues = %d, want 2 (46d beyond threshold + out-of-cooldown; 44d below)", len(calls))
	}
	targets := map[string]bool{}
	for _, c := range calls {
		var fp FollowUpPayload
		if err := json.Unmarshal(c.Payload, &fp); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if fp.JobType != "re-engagement" {
			t.Errorf("jobType = %s, want re-engagement", fp.JobType)
		}
		if fp.OrderID == "" {
			t.Errorf("re-engagement for %s has empty orderId (most recent expected)", fp.CustomerID)
		}
		targets[fp.CustomerID] = true
	}
	if !targets[beyondThreshold.ID] || !targets[outOfCooldown.ID] {
		t.Errorf("targets = %v, want only %s and %s", targets, beyondThreshold.ID, outOfCooldown.ID)
	}
	if targets[belowThreshold.ID] {
		t.Errorf("below-threshold profile fired: daysSince must EXCEED freq x multiplier")
	}

	for _, cust := range []struct{ id string }{{beyondThreshold.ID}, {outOfCooldown.ID}} {
		if stamp := s7a_scalar(t, s.db, `SELECT COALESCE(last_reengagement_at::text,'') FROM customer_profiles WHERE customer_id = $1`, cust.id); stamp == "" {
			t.Errorf("customer %s cooldown not stamped after enqueue", cust.id)
		}
	}
	if stamp := s7a_scalar(t, s.db, `SELECT COALESCE(last_reengagement_at::text,'') FROM customer_profiles WHERE customer_id = $1`, belowThreshold.ID); stamp != "" {
		t.Errorf("below-threshold profile stamped %q but should be untouched", stamp)
	}
}

func TestS7A_Retention_MultiplierOverrideFromConfig(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	biz := factory.Business()
	cust := factory.Customer(biz.ID)
	s7a_seedOrder(t, s.db, biz.ID, cust.ID, "", "DELIVERED", "10.00")
	now := time.Now().UTC()
	s7a_setProfile(t, s, cust.ID, now.Add(-45*24*time.Hour), 30, nil)

	deps := s7a_deps(s, pub)
	deps.ReengagementThresholdMultiplier = 2.0
	deps.ReengagementCooldownDays = 30

	if err := SweepRetention(t.Context(), deps); err != nil {
		t.Fatalf("SweepRetention: %v", err)
	}
	if got := len(pub.recorded()); got != 0 {
		t.Errorf("enqueues = %d, want 0 (45d <= 30d x 2.0)", got)
	}
}

func TestS7A_TwoInstanceConcurrentSweeps_NoDoubleFire(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)

	biz := factory.Business()
	cust := factory.Customer(biz.ID)
	orderID := s7a_seedOrder(t, s.db, biz.ID, cust.ID, "", "CONFIRMED", "10.00")
	conv := factory.Conversation(biz.ID, cust.Phone)
	now := time.Now().UTC()

	const nFollowUps = 12
	for i := 0; i < nFollowUps; i++ {
		s7a_seedFollowUp(t, s.db, orderID, biz.ID, cust.ID, "abandoned-cart",
			now.Add(-1*time.Hour).Add(time.Duration(i)*time.Second))
	}
	const nStranded = 6
	stranded := map[string]bool{}
	for i := 0; i < nStranded; i++ {
		id := s7a_seedOutbound(t, s.db, biz.ID, conv.ID, cust.Phone, "pending",
			now.Add(-5*time.Minute).Add(time.Duration(i)*time.Second))
		stranded[id] = true
	}

	dueProfile := factory.Customer(biz.ID)
	s7a_seedOrder(t, s.db, biz.ID, dueProfile.ID, "", "DELIVERED", "10.00")
	s7a_setProfile(t, s, dueProfile.ID, now.Add(-90*24*time.Hour), 30, nil)
	deps := Deps{Pool: s.pool, Publisher: nil}
	deps.Publisher = &s7a_sharedPublisher{}
	deps.ReengagementThresholdMultiplier = 1.5
	deps.ReengagementCooldownDays = 30

	shared := deps.Publisher.(*s7a_sharedPublisher)
	shared.DedupTaskIDs = true

	var wg sync.WaitGroup
	errs := make(chan error, 8)
	for instance := 0; instance < 2; instance++ {
		wg.Add(3)
		go func() { defer wg.Done(); errs <- SweepFollowUps(t.Context(), deps) }()
		go func() { defer wg.Done(); errs <- SweepOutbox(t.Context(), deps) }()
		go func() { defer wg.Done(); errs <- SweepRetention(t.Context(), deps) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("sweep error: %v", err)
		}
	}

	if claimed := s7a_count(t, s.db, `SELECT COUNT(*) FROM scheduled_follow_ups WHERE executed_at IS NOT NULL`); claimed != nFollowUps {
		t.Errorf("claimed follow-ups = %d, want %d (every due row claimed exactly once)", claimed, nFollowUps)
	}
	if pending := s7a_count(t, s.db, `SELECT COUNT(*) FROM scheduled_follow_ups WHERE executed_at IS NULL AND cancelled_at IS NULL`); pending != 0 {
		t.Errorf("unclaimed due rows = %d, want 0", pending)
	}

	calls := shared.snapshot()
	followUpEnqueues := 0
	outboxEnqueues := 0
	retentionEnqueues := 0
	taskIDs := map[string]bool{}
	for _, c := range calls {
		switch c.TaskType {
		case queue.TaskFollowUpRun:
			if c.Queue == queue.QFollowUp {
				if c.Opts != nil && strings.HasPrefix(c.Opts.TaskID, "scheduled-follow-up:") {
					followUpEnqueues++
				} else {
					retentionEnqueues++
				}
			}
		case queue.TaskOutboundSend:
			outboxEnqueues++
			if c.Opts != nil {
				taskIDs[c.Opts.TaskID] = true
			}
		}
	}
	if followUpEnqueues != nFollowUps {
		t.Errorf("follow-up enqueues across both instances = %d, want %d (CAS claim is the dedup)", followUpEnqueues, nFollowUps)
	}
	if outboxEnqueues != nStranded {
		t.Errorf("outbox re-enqueues across both instances = %d, want %d (TaskID dedup + lock)", outboxEnqueues, nStranded)
	}
	if retentionEnqueues != 1 {
		t.Errorf("retention enqueues across both instances = %d, want 1 (cooldown stamp under lock)", retentionEnqueues)
	}
	if stale := s7a_count(t, s.db, `SELECT COUNT(*) FROM outbound_messages WHERE status = 'failed_stale'`); stale != 0 {
		t.Errorf("failed_stale rows = %d, want 0 in this scenario", stale)
	}
	_ = stranded
}

// s7a_sharedPublisher is one publisher shared by both fake instances so the
// combined enqueue record is observable; DedupTaskIDs mimics asynq.
type s7a_sharedPublisher struct {
	mu           sync.Mutex
	DedupTaskIDs bool
	calls        []s7a_enqueueCall
	taskIDs      map[string]bool
}

func (f *s7a_sharedPublisher) Enqueue(ctx context.Context, q, taskType string, payload any, opts *queue.EnqueueOpts) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	call := s7a_enqueueCall{Queue: q, TaskType: taskType, Payload: raw, Opts: opts}
	if f.DedupTaskIDs && opts != nil && opts.TaskID != "" {
		if f.taskIDs == nil {
			f.taskIDs = map[string]bool{}
		}
		if f.taskIDs[opts.TaskID] {
			return fmt.Errorf("asynq: task ID conflicts: %s", opts.TaskID)
		}
		f.taskIDs[opts.TaskID] = true
	}
	f.calls = append(f.calls, call)
	return nil
}

func (f *s7a_sharedPublisher) snapshot() []s7a_enqueueCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]s7a_enqueueCall, len(f.calls))
	copy(out, f.calls)
	return out
}
