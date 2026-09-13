package main

import (
	"fmt"
	"slices"
	"strings"
	"time"
)

// sentryChecks asserts on what the api and worker sent to the Sentry ingest
// mock over the whole journey: issues, transactions with spans, profiles and
// structured logs.
func (r *runner) sentryChecks() {
	r.section("Sentry: issues, tracing, profiling, logs")

	events := func() []sentryItem { return r.mock.sentryItems("event") }
	findEvent := func(match func(ev sentryItem) bool) (sentryItem, bool) {
		for _, ev := range events() {
			if match(ev) {
				return ev, true
			}
		}
		return sentryItem{}, false
	}
	exceptionType := func(ev sentryItem) string {
		values, _ := ev.Payload["exception"].([]any)
		if len(values) == 0 {
			return ""
		}
		last, _ := values[len(values)-1].(map[string]any)
		return str(last["type"])
	}
	tag := func(it sentryItem, key string) string { return str(dig(it.Payload, "tags", key)) }

	var guard sentryItem
	r.check("auth guard rejections are reported as UnauthorizedException", r.waitFor(20*time.Second, func() bool {
		var ok bool
		guard, ok = findEvent(func(ev sentryItem) bool {
			return exceptionType(ev) == "UnauthorizedException" && tag(ev, "path") == "/orders"
		})
		return ok
	}))
	r.check("issues are tagged with the service and method", tag(guard, "service") == "api" && tag(guard, "method") == "GET", guard.Payload["tags"])

	zod, ok := findEvent(func(ev sentryItem) bool {
		return exceptionType(ev) == "ZodValidationException" && tag(ev, "path") == "/auth/request-otp"
	})
	r.check("validation failures are reported with the request body as extra", ok &&
		str(dig(zod.Payload, "contexts", "extra", "body", "deliveryMethod")) == "pigeon", dig(zod.Payload, "contexts", "extra"))
	r.check("customer PII in reported bodies is scrubbed", ok &&
		str(dig(zod.Payload, "contexts", "extra", "body", "phone")) == "[redacted]" &&
		!strings.Contains(fmt.Sprint(zod.Payload["request"]), strings.TrimPrefix(ownerPhone, "+")), zod.Payload["request"])

	_, ok = findEvent(func(ev sentryItem) bool {
		return exceptionType(ev) == "BadRequestException" && tag(ev, "path") == "/payouts/request" &&
			dig(ev.Payload, "contexts", "extra", "body", "amount") == float64(500)
	})
	r.check("4xx HttpExceptions from handlers are reported (over-balance payout)", ok)

	_, ok = findEvent(func(ev sentryItem) bool { return strings.HasPrefix(tag(ev, "path"), "/health") })
	r.check("health probes are never reported", !ok)

	r.check("permanent task failures are reported as fatal issues by the worker", r.waitFor(10*time.Second, func() bool {
		_, ok := findEvent(func(ev sentryItem) bool {
			return tag(ev, "task") == "outbound-queue:send-message" && str(ev.Payload["level"]) == "fatal" && tag(ev, "service") == "worker"
		})
		return ok
	}))

	transactions := func(name string) []sentryItem {
		var out []sentryItem
		for _, tx := range r.mock.sentryItems("transaction") {
			if str(tx.Payload["transaction"]) == name {
				out = append(out, tx)
			}
		}
		return out
	}
	spanOps := func(tx sentryItem) map[string][]string {
		ops := map[string][]string{}
		spans, _ := tx.Payload["spans"].([]any)
		for _, s := range spans {
			sm, _ := s.(map[string]any)
			ops[str(sm["op"])] = append(ops[str(sm["op"])], str(sm["description"]))
		}
		return ops
	}
	hasDescription := func(descs []string, part string) bool {
		return slices.ContainsFunc(descs, func(d string) bool { return strings.Contains(d, part) })
	}

	orderTx := transactions("GET /orders/{id}")
	r.check("API requests become route-named http.server transactions with DB spans", len(orderTx) > 0 &&
		str(dig(orderTx[0].Payload, "contexts", "trace", "op")) == "http.server" &&
		str(dig(orderTx[0].Payload, "transaction_info", "source")) == "route" &&
		hasDescription(spanOps(orderTx[0])["db.sql.query"], "FROM orders"), len(orderTx))

	var runs []sentryItem
	r.check("orchestrator runs become queue.process transactions with DB, LLM and Meta spans", r.waitFor(10*time.Second, func() bool {
		runs = nil
		for _, tx := range transactions("orchestrator-queue:debounced") {
			ops := spanOps(tx)
			if str(dig(tx.Payload, "contexts", "trace", "op")) == "queue.process" && len(ops["db.sql.query"]) > 0 &&
				hasDescription(ops["http.client"], "/openai/responses") && hasDescription(ops["http.client"], "/graph/") {
				runs = append(runs, tx)
			}
		}
		return len(runs) > 0
	}))

	profiles := map[string]sentryItem{}
	for _, p := range r.mock.sentryItems("profile") {
		profiles[p.EventID] = p
	}
	profiled := 0
	for _, tx := range runs {
		p, ok := profiles[tx.EventID]
		if !ok {
			continue
		}
		samples, _ := dig(p.Payload, "profile", "samples").([]any)
		if str(p.Payload["version"]) == "1" && str(p.Payload["platform"]) == "go" && len(samples) >= 2 &&
			str(dig(p.Payload, "transaction", "id")) == tx.EventID &&
			str(dig(tx.Payload, "contexts", "profile", "profile_id")) == str(p.Payload["event_id"]) {
			profiled++
		}
	}
	r.check("orchestrator transactions carry sample-format profiles in the same envelope", profiled > 0, fmt.Sprintf("%d runs, %d profiles", len(runs), len(profiles)))

	workerTraces := map[string]bool{}
	for _, tx := range r.mock.sentryItems("transaction") {
		if tag(tx, "service") == "worker" {
			workerTraces[str(dig(tx.Payload, "contexts", "trace", "trace_id"))] = true
		}
	}
	propagated := false
	for _, id := range r.mock.propagatedTraceIDs() {
		propagated = propagated || workerTraces[id]
	}
	r.check("provider calls propagate the trace (sentry-trace on Meta sends)", propagated)

	logBodies := func() []string {
		var out []string
		for _, batch := range r.mock.sentryItems("log") {
			items, _ := batch.Payload["items"].([]any)
			for _, it := range items {
				out = append(out, str(it.(map[string]any)["body"]))
			}
		}
		return out
	}
	r.check("structured logs reach Sentry Logs from both processes", r.waitFor(20*time.Second, func() bool {
		bodies := logBodies()
		return hasDescription(bodies, "Processing job example-e2e-1 (e2e-hello)") && hasDescription(bodies, "request completed")
	}))
}

func str(v any) string {
	s, _ := v.(string)
	return s
}

func dig(m map[string]any, path ...string) any {
	var cur any = m
	for _, p := range path {
		mm, ok := cur.(map[string]any)
		if !ok {
			return nil
		}
		cur = mm[p]
	}
	return cur
}
