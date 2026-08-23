package domain

import "testing"

// TestS7b_RequiresEscalationKeywordMatrix ports safety.guard.spec.ts
// expectations plus the word-boundary contract documented in safety.guard.ts:
// \b avoids substring false positives ("refund" must NOT fire on
// "refunded"/"refundable", "angry" not on "angryface"); matching is
// case-insensitive.
func TestS7b_RequiresEscalationKeywordMatrix(t *testing.T) {
	cases := []struct {
		text string
		want bool
	}{
		{"I want a refund", true},
		{"REFUND MY MONEY", true},
		{"This was refunded already", false}, // word boundary: no match inside "refunded"
		{"Is this refundable?", false},       // word boundary: no match inside "refundable"
		{"call the police", true},
		{"POLICE!", true},
		{"this is a scam", true},
		{"Scammer detected", false}, // no boundary between "scam" and "mer"
		{"I am angry", true},
		{"angryface", false}, // characterized in safety.guard.ts comment
		{"you were not helpful", true},
		{"NOT HELPFUL at all", true},
		{"nothelpful", false},
		{"I dispute this charge", true},
		{"undisputed", false},
		{"pre-refund step", true}, // '-' is a word boundary in JS and RE2 alike
		{"", false},
		{"   ", false},
		{"hello world", false},
		{"friendly banter about politics", false}, // "police"/"politics" must not cross-match
	}

	for _, tc := range cases {
		if got := RequiresEscalation(tc.text); got != tc.want {
			t.Errorf("RequiresEscalation(%q) = %v, want %v", tc.text, got, tc.want)
		}
	}
}

// TestS7b_EscalationKeywordsMatchSource pins the exported keyword list to the
// verbatim ESCALATION_KEYWORDS (safety.guard.ts:1-8).
func TestS7b_EscalationKeywordsMatchSource(t *testing.T) {
	want := []string{"refund", "police", "scam", "angry", "not helpful", "dispute"}
	if len(EscalationKeywords) != len(want) {
		t.Fatalf("EscalationKeywords length = %d, want %d", len(EscalationKeywords), len(want))
	}
	for i := range want {
		if EscalationKeywords[i] != want[i] {
			t.Errorf("EscalationKeywords[%d] = %q, want %q", i, EscalationKeywords[i], want[i])
		}
	}
}
