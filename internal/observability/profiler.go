package observability

import (
	"bytes"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	mrand "math/rand/v2"
	"net/http"
	"runtime"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/getsentry/sentry-go"
)

// Transaction profiling (instrument.ts: nodeProfilingIntegration with
// profilesSampleRate, relative to traced transactions). sentry-go no longer
// ships a profiler, so this one samples the stacks of a profiled
// transaction's goroutine — and every goroutine it started — at 101 Hz while
// the transaction runs, and sends them in Sentry's sample format v1 as a
// "profile" item in the transaction's envelope.
const (
	profileSampleInterval = time.Second / 101
	// maxProfileDuration is Relay's limit; longer profiles are truncated.
	maxProfileDuration = 30 * time.Second
	// minProfileSamples: Relay rejects profiles with fewer samples.
	minProfileSamples = 2
	// profileTTL bounds how long a finished profile waits for its transaction
	// to be sent (a dropped transaction never claims it).
	profileTTL = 2 * time.Minute
	// maxStackDump caps the all-goroutine stack buffer.
	maxStackDump = 32 << 20
)

type profiler struct {
	mu       sync.Mutex
	rate     float64
	sessions map[*profileSession]struct{}
	running  bool

	finished map[string]*finishedProfile // by transaction span id
	ready    map[string]readyProfile     // by transaction event id

	inAppPrefix string
}

type finishedProfile struct {
	payload *profilePayload
	at      time.Time
}

type readyProfile struct {
	payload []byte
	at      time.Time
}

var globalProfiler = newProfiler()

func newProfiler() *profiler {
	prefix := "main"
	if info, ok := debug.ReadBuildInfo(); ok && info.Main.Path != "" {
		prefix = info.Main.Path
	}
	return &profiler{
		sessions:    map[*profileSession]struct{}{},
		finished:    map[string]*finishedProfile{},
		ready:       map[string]readyProfile{},
		inAppPrefix: prefix,
	}
}

func (p *profiler) setRate(rate float64) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.rate = rate
}

// startProfile begins profiling tx when it is sampled and picked by the
// profiles sample rate; nil otherwise. It must be called on the goroutine that
// runs the transaction.
func startProfile(tx *sentry.Span) *profileSession {
	return globalProfiler.start(tx)
}

func (p *profiler) start(tx *sentry.Span) *profileSession {
	if tx == nil || !tx.Sampled.Bool() {
		return nil
	}
	p.mu.Lock()
	rate := p.rate
	p.mu.Unlock()
	if rate <= 0 || (rate < 1 && mrand.Float64() >= rate) {
		return nil
	}
	s := &profileSession{
		p:          p,
		tx:         tx,
		goid:       currentGoroutineID(),
		start:      time.Now(),
		frameIndex: map[string]int{},
		stackIndex: map[string]int{},
		threads:    map[uint64]struct{}{},
	}
	p.mu.Lock()
	p.sessions[s] = struct{}{}
	if !p.running {
		p.running = true
		go p.sample()
	}
	p.mu.Unlock()
	return s
}

// sample runs while any profile session is active.
func (p *profiler) sample() {
	ticker := time.NewTicker(profileSampleInterval)
	defer ticker.Stop()
	buf := make([]byte, 256<<10)
	for range ticker.C {
		p.mu.Lock()
		if len(p.sessions) == 0 {
			p.running = false
			p.mu.Unlock()
			return
		}
		sessions := make([]*profileSession, 0, len(p.sessions))
		for s := range p.sessions {
			sessions = append(sessions, s)
		}
		p.mu.Unlock()

		now := time.Now()
		n := runtime.Stack(buf, true)
		for n == len(buf) && len(buf) < maxStackDump {
			buf = make([]byte, 2*len(buf))
			n = runtime.Stack(buf, true)
		}
		dump := parseStackDump(buf[:n])
		for _, s := range sessions {
			s.add(now, dump)
		}
	}
}

// profileSession collects one transaction's samples.
type profileSession struct {
	p     *profiler
	tx    *sentry.Span
	goid  uint64
	start time.Time

	mu         sync.Mutex
	stopped    bool
	frames     []profileFrame
	frameIndex map[string]int
	stacks     [][]int
	stackIndex map[string]int
	samples    []profileSample
	threads    map[uint64]struct{}
	ticks      int
}

func (s *profileSession) add(now time.Time, dump *stackDump) {
	s.mu.Lock()
	defer s.mu.Unlock()
	elapsed := now.Sub(s.start)
	if s.stopped || elapsed > maxProfileDuration {
		return
	}
	sampled := false
	for _, g := range dump.goroutines {
		if !dump.descendsFrom(g.id, s.goid) {
			continue
		}
		frames := g.framesOf()
		stack := make([]int, 0, len(frames))
		var key strings.Builder
		for _, raw := range frames {
			idx, ok := s.frameIndex[raw.key]
			if !ok {
				idx = len(s.frames)
				s.frames = append(s.frames, s.p.frame(raw))
				s.frameIndex[raw.key] = idx
			}
			stack = append(stack, idx)
			key.WriteString(strconv.Itoa(idx))
			key.WriteByte(',')
		}
		if len(stack) == 0 {
			continue
		}
		stackID, ok := s.stackIndex[key.String()]
		if !ok {
			stackID = len(s.stacks)
			s.stacks = append(s.stacks, stack)
			s.stackIndex[key.String()] = stackID
		}
		s.samples = append(s.samples, profileSample{
			ElapsedSinceStartNS: uint64(elapsed.Nanoseconds()),
			StackID:             stackID,
			ThreadID:            strconv.FormatUint(g.id, 10),
		})
		s.threads[g.id] = struct{}{}
		sampled = true
	}
	if sampled {
		s.ticks++
	}
}

// stop ends sampling; call it right before finishing the transaction. The
// profile is attached when the transaction is sent (attachProfile).
func (s *profileSession) stop() {
	if s == nil {
		return
	}
	p := s.p
	p.mu.Lock()
	delete(p.sessions, s)
	p.mu.Unlock()

	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stopped {
		return
	}
	s.stopped = true
	if s.ticks < minProfileSamples {
		return
	}
	duration := time.Since(s.start)
	if duration > maxProfileDuration {
		duration = maxProfileDuration
	}
	threads := make(map[string]profileThread, len(s.threads))
	for id := range s.threads {
		threads[strconv.FormatUint(id, 10)] = profileThread{Name: "goroutine " + strconv.FormatUint(id, 10)}
	}
	payload := &profilePayload{
		Device:    profileDevice{Architecture: runtime.GOARCH},
		EventID:   newProfileID(),
		OS:        profileOS{Name: runtime.GOOS},
		Platform:  "go",
		Runtime:   profileRuntime{Name: "go", Version: runtime.Version()},
		Timestamp: s.start.UTC(),
		Profile: profileTrace{
			Frames:         s.frames,
			Samples:        s.samples,
			Stacks:         s.stacks,
			ThreadMetadata: threads,
		},
		Transaction: profileTransaction{
			ActiveThreadID:  strconv.FormatUint(s.goid, 10),
			DurationNS:      strconv.FormatInt(duration.Nanoseconds(), 10),
			RelativeStartNS: "0",
			RelativeEndNS:   strconv.FormatInt(duration.Nanoseconds(), 10),
			TraceID:         s.tx.TraceID.String(),
		},
		Version: "1",
	}
	now := time.Now()
	p.mu.Lock()
	p.purgeLocked(now)
	p.finished[s.tx.SpanID.String()] = &finishedProfile{payload: payload, at: now}
	p.mu.Unlock()
}

func (p *profiler) purgeLocked(now time.Time) {
	for k, f := range p.finished {
		if now.Sub(f.at) > profileTTL {
			delete(p.finished, k)
		}
	}
	for k, r := range p.ready {
		if now.Sub(r.at) > profileTTL {
			delete(p.ready, k)
		}
	}
}

// attachProfile runs in BeforeSendTransaction, once the transaction event has
// its id: it links the finished profile to the event (contexts.profile) and
// queues the payload for the envelope carrying that event.
func (p *profiler) attachProfile(event *sentry.Event) {
	if event == nil || event.Contexts == nil {
		return
	}
	spanID := fmt.Sprint(event.Contexts["trace"]["span_id"])
	p.mu.Lock()
	f, ok := p.finished[spanID]
	delete(p.finished, spanID)
	p.mu.Unlock()
	if !ok {
		return
	}
	payload := f.payload
	payload.Transaction.ID = string(event.EventID)
	payload.Transaction.Name = event.Transaction
	payload.Environment = event.Environment
	payload.Release = event.Release
	payload.Dist = event.Dist
	body, err := json.Marshal(payload)
	if err != nil {
		return
	}
	event.Contexts["profile"] = sentry.Context{"profile_id": payload.EventID}
	p.mu.Lock()
	p.ready[string(event.EventID)] = readyProfile{payload: body, at: time.Now()}
	p.mu.Unlock()
}

func (p *profiler) hasReady() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.ready) > 0
}

func (p *profiler) takeReady(eventID string) ([]byte, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	r, ok := p.ready[eventID]
	delete(p.ready, eventID)
	return r.payload, ok
}

// frame resolves a raw stack frame into a Sentry frame.
func (p *profiler) frame(raw rawFrame) profileFrame {
	module, function := splitGoFunction(raw.function)
	return profileFrame{
		Function: function,
		Module:   module,
		AbsPath:  raw.file,
		Lineno:   raw.line,
		InApp:    module == p.inAppPrefix || strings.HasPrefix(module, p.inAppPrefix+"/"),
	}
}

// splitGoFunction splits "net/http.(*conn).serve" into its package import
// path and function (sentry-go's splitQualifiedFunctionName).
func splitGoFunction(name string) (module, function string) {
	if strings.HasPrefix(name, "go.") || strings.HasPrefix(name, "type.") {
		return "", name
	}
	pathEnd := max(strings.LastIndex(name, "/"), 0)
	if i := strings.Index(name[pathEnd:], "."); i != -1 {
		return name[:pathEnd+i], name[pathEnd+i+1:]
	}
	return "", name
}

// profileTransport appends each queued profile to the envelope carrying its
// transaction. It wraps the transport Sentry's client sends envelopes with.
type profileTransport struct {
	base     http.RoundTripper
	profiles *profiler
}

func (t *profileTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	if req.Body == nil || req.Body == http.NoBody || !t.profiles.hasReady() {
		return t.base.RoundTrip(req)
	}
	body, err := io.ReadAll(req.Body)
	_ = req.Body.Close()
	if err != nil {
		return nil, err
	}
	if payload, ok := t.profiles.takeReady(envelopeEventID(body)); ok {
		body = appendEnvelopeItem(body, "profile", payload)
	}
	out := req.Clone(req.Context())
	out.Body = io.NopCloser(bytes.NewReader(body))
	out.ContentLength = int64(len(body))
	out.GetBody = func() (io.ReadCloser, error) { return io.NopCloser(bytes.NewReader(body)), nil }
	return t.base.RoundTrip(out)
}

func envelopeEventID(envelope []byte) string {
	header, _, _ := bytes.Cut(envelope, []byte("\n"))
	var h struct {
		EventID string `json:"event_id"`
	}
	_ = json.Unmarshal(header, &h)
	return h.EventID
}

func appendEnvelopeItem(envelope []byte, itemType string, payload []byte) []byte {
	out := make([]byte, 0, len(envelope)+len(payload)+64)
	out = append(out, envelope...)
	if len(out) > 0 && out[len(out)-1] != '\n' {
		out = append(out, '\n')
	}
	out = fmt.Appendf(out, `{"type":%q,"length":%d}`+"\n", itemType, len(payload))
	out = append(out, payload...)
	return append(out, '\n')
}

func newProfileID() string {
	var b [16]byte
	_, _ = rand.Read(b[:])
	return hex.EncodeToString(b[:])
}

// Sample format v1 (https://develop.sentry.dev/sdk/telemetry/profiles/sample-format-v1/).
type profilePayload struct {
	Device      profileDevice      `json:"device"`
	Environment string             `json:"environment,omitempty"`
	EventID     string             `json:"event_id"`
	OS          profileOS          `json:"os"`
	Platform    string             `json:"platform"`
	Release     string             `json:"release,omitempty"`
	Dist        string             `json:"dist,omitempty"`
	Runtime     profileRuntime     `json:"runtime"`
	Timestamp   time.Time          `json:"timestamp"`
	Profile     profileTrace       `json:"profile"`
	Transaction profileTransaction `json:"transaction"`
	Version     string             `json:"version"`
}

type profileDevice struct {
	Architecture string `json:"architecture"`
}

type profileOS struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type profileRuntime struct {
	Name    string `json:"name"`
	Version string `json:"version"`
}

type profileTrace struct {
	Frames         []profileFrame           `json:"frames"`
	Samples        []profileSample          `json:"samples"`
	Stacks         [][]int                  `json:"stacks"`
	ThreadMetadata map[string]profileThread `json:"thread_metadata"`
}

type profileFrame struct {
	Function string `json:"function,omitempty"`
	Module   string `json:"module,omitempty"`
	AbsPath  string `json:"abs_path,omitempty"`
	Lineno   int    `json:"lineno,omitempty"`
	InApp    bool   `json:"in_app"`
}

type profileSample struct {
	ElapsedSinceStartNS uint64 `json:"elapsed_since_start_ns"`
	StackID             int    `json:"stack_id"`
	ThreadID            string `json:"thread_id"`
}

type profileThread struct {
	Name string `json:"name,omitempty"`
}

type profileTransaction struct {
	ActiveThreadID  string `json:"active_thread_id"`
	DurationNS      string `json:"duration_ns"`
	ID              string `json:"id"`
	Name            string `json:"name"`
	RelativeStartNS string `json:"relative_start_ns"`
	RelativeEndNS   string `json:"relative_end_ns"`
	TraceID         string `json:"trace_id"`
}
