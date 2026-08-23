// Exact port of libs/orchestrator/src/guards/safety.guard.ts (T7.x CRM wave):
// the keyword list and word-boundary matchers that fast-path a message into
// human escalation, bypassing the LLM entirely.
package domain

import (
	"regexp"
)

// EscalationKeywords mirrors ESCALATION_KEYWORDS (safety.guard.ts:1-8).
var EscalationKeywords = []string{
	"refund",
	"police",
	"scam",
	"angry",
	"not helpful",
	"dispute",
}

// escapeRegexpKeyword reproduces the source's metacharacter escape
// (safety.guard.ts:17): backslash-prefix every [.*+?^${}()|[\]\\] so an
// edited keyword list can never smuggle in regex syntax.
func escapeRegexpKeyword(keyword string) string {
	meta := regexp.MustCompile(`[.*+?^${}()|[\]\\]`)
	return meta.ReplaceAllStringFunc(keyword, func(c string) string { return "\\" + c })
}

// escalationPatterns are word-boundary matchers for each keyword
// (safety.guard.ts:15-18): \b avoids substring false positives — "refund" no
// longer fires on "refunded"/"refundable" and "angry" no longer fires on
// "angryface". Case-insensitive.
var escalationPatterns = func() []*regexp.Regexp {
	patterns := make([]*regexp.Regexp, len(EscalationKeywords))
	for i, keyword := range EscalationKeywords {
		patterns[i] = regexp.MustCompile(`(?i)\b` + escapeRegexpKeyword(keyword) + `\b`)
	}
	return patterns
}()

// RequiresEscalation is the fast-path check deciding whether a message should
// instantly bypass the LLM and escalate (safety.guard.ts:23-26).
func RequiresEscalation(textContent string) bool {
	if textContent == "" {
		return false
	}
	for _, pattern := range escalationPatterns {
		if pattern.MatchString(textContent) {
			return true
		}
	}
	return false
}
