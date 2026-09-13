package observability

import (
	"runtime"
	"strings"
	"testing"
	"time"
)

func TestStackDumpFollowsGoroutineAncestry(t *testing.T) {
	root := currentGoroutineID()
	started := make(chan struct{})
	release := make(chan struct{})
	go func() {
		go func() {
			close(started)
			<-release
		}()
		<-release
	}()
	<-started
	defer close(release)
	time.Sleep(10 * time.Millisecond)

	buf := make([]byte, 1<<20)
	dump := parseStackDump(buf[:runtime.Stack(buf, true)])

	var mine []*dumpedGoroutine
	for _, g := range dump.goroutines {
		if dump.descendsFrom(g.id, root) {
			mine = append(mine, g)
		}
	}
	// This goroutine itself is the dump's caller and is skipped; its child
	// and grandchild remain.
	if len(mine) != 2 {
		t.Fatalf("descendants of goroutine %d = %d, want 2 (parents %v)", root, len(mine), dump.parent)
	}
	for _, g := range mine {
		frames := g.framesOf()
		if len(frames) == 0 || !strings.Contains(frames[len(frames)-1].function, "TestStackDumpFollowsGoroutineAncestry") {
			t.Errorf("goroutine %d frames = %+v", g.id, frames)
		}
		for _, f := range frames {
			if f.file == "" || f.line == 0 || strings.Contains(f.function, "(0x") {
				t.Errorf("badly parsed frame %+v", f)
			}
		}
	}
}

func TestSplitGoFunction(t *testing.T) {
	for in, want := range map[string][2]string{
		"net/http.(*conn).serve": {"net/http", "(*conn).serve"},
		"main.main":              {"main", "main"},
		"github.com/a/b/internal/x.Handler.func1": {"github.com/a/b/internal/x", "Handler.func1"},
		"github.com/a/b.v2/pkg.(*T[...]).Method":  {"github.com/a/b.v2/pkg", "(*T[...]).Method"},
		"runtime.goexit":                          {"runtime", "goexit"},
	} {
		module, function := splitGoFunction(in)
		if module != want[0] || function != want[1] {
			t.Errorf("splitGoFunction(%q) = %q, %q; want %q", in, module, function, want)
		}
	}
}
