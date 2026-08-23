package workers

import (
	"encoding/json"
	"github.com/novoapex/novoapex-backend-api/internal/harness"
	"github.com/novoapex/novoapex-backend-api/internal/queue"
	"strings"
	"testing"
)

func TestS7A_FollowUp_MessageCopyGolden_AllFiveTypes(t *testing.T) {
	cases := []struct {
		jobType string
		status  string
		golden  string
	}{
		{"abandoned-cart", "CONFIRMED", "s7a_followup_abandoned_cart"},
		{"unpaid-invoice-first", "PAYMENT_PENDING", "s7a_followup_unpaid_invoice_first"},
		{"unpaid-invoice-second", "CONFIRMED", "s7a_followup_unpaid_invoice_second"},
		{"delivery-confirmation", "DELIVERED", "s7a_followup_delivery_confirmation"},
		{"re-engagement", "CANCELLED", "s7a_followup_re_engagement"},
	}

	for _, tc := range cases {
		t.Run(tc.jobType, func(t *testing.T) {
			s := s7a_startStack(t)
			factory := harness.NewFactory(t, s.db)
			pub := &s7a_fakePublisher{}

			biz := factory.Business()
			cust := factory.Customer(biz.ID)
			s7a_renameCustomer(t, s.db, cust.ID, "Amara")
			conv := factory.Conversation(biz.ID, cust.Phone)
			orderID := s7a_seedOrder(t, s.db, biz.ID, cust.ID, conv.ID, tc.status, "51.00")

			fp := FollowUpPayload{JobType: tc.jobType, BusinessID: biz.ID, CustomerID: cust.ID, OrderID: orderID}
			if err := HandleFollowUp(t.Context(), s7a_deps(s, pub), fp); err != nil {
				t.Fatalf("HandleFollowUp: %v", err)
			}

			calls := pub.recorded()
			if len(calls) != 1 {
				t.Fatalf("publisher calls = %d, want 1", len(calls))
			}
			c := calls[0]
			if c.Queue != queue.QOutbound || c.TaskType != fuOutboundTaskType {
				t.Errorf("enqueue target = %s/%s, want %s/%s (source job name 'send')",
					c.Queue, c.TaskType, queue.QOutbound, fuOutboundTaskType)
			}
			var job fuOutboundJob
			if err := json.Unmarshal(c.Payload, &job); err != nil {
				t.Fatalf("decode outbound job payload: %v", err)
			}
			if job.RecipientPhone != cust.Phone || job.BusinessID != biz.ID || job.ConversationID != conv.ID {
				t.Errorf("job routing = %+v, want phone/conversation/business wired", job)
			}
			if strings.Contains(job.Text, "Fixture Customer") {
				t.Errorf("customer name fallback failed: %q", job.Text)
			}

			golden, _ := json.Marshal(map[string]any{
				"businessId":     "{{businessId}}",
				"conversationId": "{{conversationId}}",
				"recipientPhone": "{{recipientPhone}}",
				"text":           job.Text,
			})
			harness.AssertJSONGolden(t, tc.golden, golden)
		})
	}
}

func TestS7A_FollowUp_CurrencySymbolFormatting(t *testing.T) {
	cases := []struct {
		currency string
		want     string
	}{
		{"GHS", "GH₵51.00"},
		{"NGN", "₦51.00"},
		{"USD", "$51.00"},
		{"XXX", "GH₵51.00"},
	}

	for _, tc := range cases {
		s := s7a_startStack(t)
		factory := harness.NewFactory(t, s.db)
		pub := &s7a_fakePublisher{}

		biz := factory.Business()
		if _, err := s.db.Exec(`UPDATE businesses SET currency = $2 WHERE id = $1`, biz.ID, tc.currency); err != nil {
			t.Fatalf("set currency: %v", err)
		}
		cust := factory.Customer(biz.ID)
		s7a_renameCustomer(t, s.db, cust.ID, "Amara")
		factory.Conversation(biz.ID, cust.Phone)
		orderID := s7a_seedOrder(t, s.db, biz.ID, cust.ID, "", "CONFIRMED", "51.00")

		fp := FollowUpPayload{JobType: "unpaid-invoice-first", BusinessID: biz.ID, CustomerID: cust.ID, OrderID: orderID}
		if err := HandleFollowUp(t.Context(), s7a_deps(s, pub), fp); err != nil {
			t.Fatalf("%s: HandleFollowUp: %v", tc.currency, err)
		}
		calls := pub.recorded()
		if len(calls) != 1 {
			t.Fatalf("%s: publisher calls = %d, want 1", tc.currency, len(calls))
		}
		var job fuOutboundJob
		if err := json.Unmarshal(calls[0].Payload, &job); err != nil {
			t.Fatalf("decode: %v", err)
		}
		if !strings.Contains(job.Text, tc.want+" ") && !strings.Contains(job.Text, "for "+tc.want+".") {
			t.Errorf("%s: text %q missing formatted total %q", tc.currency, job.Text, tc.want)
		}
	}
}

func TestS7A_FollowUp_RelevanceRecheckSkipsStaleStatuses(t *testing.T) {
	cases := []struct {
		jobType, status string
	}{
		{"abandoned-cart", "PAID"},
		{"abandoned-cart", "CANCELLED"},
		{"unpaid-invoice-first", "PAID"},
		{"unpaid-invoice-second", "CANCELLED"},
		{"delivery-confirmation", "SHIPPED"},
		{"delivery-confirmation", "CONFIRMED"},
		{"unknown-type-x", "CONFIRMED"},
	}

	for _, tc := range cases {
		s := s7a_startStack(t)
		factory := harness.NewFactory(t, s.db)
		pub := &s7a_fakePublisher{}

		biz := factory.Business()
		cust := factory.Customer(biz.ID)
		factory.Conversation(biz.ID, cust.Phone)
		orderID := s7a_seedOrder(t, s.db, biz.ID, cust.ID, "", tc.status, "51.00")

		fp := FollowUpPayload{JobType: tc.jobType, BusinessID: biz.ID, CustomerID: cust.ID, OrderID: orderID}
		if err := HandleFollowUp(t.Context(), s7a_deps(s, pub), fp); err != nil {
			t.Fatalf("%s/%s: HandleFollowUp = %v, want silent skip", tc.jobType, tc.status, err)
		}
		if len(pub.recorded()) != 0 {
			t.Errorf("%s on %s: enqueued %d messages, want 0 (not relevant)", tc.jobType, tc.status, len(pub.recorded()))
		}
	}
}

func TestS7A_FollowUp_MissingOrderOrConversation_SkipsQuietly(t *testing.T) {
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)
	pub := &s7a_fakePublisher{}

	biz := factory.Business()
	cust := factory.Customer(biz.ID)

	noOrder := FollowUpPayload{JobType: "abandoned-cart", BusinessID: biz.ID, CustomerID: cust.ID, OrderID: "ord_missing"}
	if err := HandleFollowUp(t.Context(), s7a_deps(s, pub), noOrder); err != nil {
		t.Errorf("missing order: err = %v, want nil skip", err)
	}

	factory.Conversation(biz.ID, "+233210000001")
	orderID := s7a_seedOrder(t, s.db, biz.ID, cust.ID, "", "CONFIRMED", "20.00")
	noConv := FollowUpPayload{JobType: "abandoned-cart", BusinessID: biz.ID, CustomerID: cust.ID, OrderID: orderID}
	if err := HandleFollowUp(t.Context(), s7a_deps(s, pub), noConv); err != nil {
		t.Errorf("missing conversation: err = %v, want nil skip", err)
	}

	if len(pub.recorded()) != 0 {
		t.Errorf("publisher calls = %d, want 0", len(pub.recorded()))
	}
}

func TestS7A_RequiresEscalation_KeywordBoundaries(t *testing.T) {
	cases := []struct {
		text string
		want bool
	}{
		{"I want a refund", true},
		{"REFUND my money", true},
		{"call the police", true},
		{"this is a scam!!", true},
		{"I am so ANGRY right now", true},
		{"you were not helpful at all", true},
		{"I dispute this charge", true},
		{"Refunded orders are fine", false},
		{"refundable? no", false},
		{"angryface", false},
		{"unhelpful service", false},
		{"undisputed", false},
		{"", false},
		{"hello there", false},
	}
	for _, tc := range cases {
		if got := s7a_requiresEscalation(tc.text); got != tc.want {
			t.Errorf("requiresEscalation(%q) = %v, want %v", tc.text, got, tc.want)
		}
	}
}

func TestS7A_RegisterFollowUp_DecodesPayloadAndRuns(t *testing.T) {
	reg := &s7a_fakeRegistrar{}
	s := s7a_startStack(t)
	factory := harness.NewFactory(t, s.db)

	biz := factory.Business()
	cust := factory.Customer(biz.ID)
	factory.Conversation(biz.ID, cust.Phone)
	orderID := s7a_seedOrder(t, s.db, biz.ID, cust.ID, "", "PAID", "5.00")
	pub := &s7a_fakePublisher{}

	RegisterFollowUp(reg, Deps{Pool: s.pool, Publisher: pub})

	if reg.taskType != queue.TaskFollowUpRun {
		t.Fatalf("registered task type = %q, want %q", reg.taskType, queue.TaskFollowUpRun)
	}

	payload, _ := json.Marshal(FollowUpPayload{JobType: "abandoned-cart", BusinessID: biz.ID, CustomerID: cust.ID, OrderID: orderID})
	if err := reg.handler(t.Context(), payload); err != nil {
		t.Errorf("handler on irrelevant order should skip not fail: %v", err)
	}
	if len(pub.recorded()) != 0 {
		t.Errorf("enqueues = %d, want 0 (order already PAID)", len(pub.recorded()))
	}

	if err := reg.handler(t.Context(), []byte(`nope`)); err == nil {
		t.Error("malformed payload must error")
	}
}
