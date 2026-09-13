package observability

import (
	"bytes"
	"runtime"
	"strconv"
)

// stackDump is a runtime.Stack(buf, true) snapshot. Headers and parent links
// are read up front; frames are parsed only for goroutines a profile needs.
type stackDump struct {
	goroutines []*dumpedGoroutine
	parent     map[uint64]uint64 // goroutine id -> id of the goroutine that started it
}

type dumpedGoroutine struct {
	id     uint64
	body   []byte // frame lines; valid only until the next dump
	frames []rawFrame
	parsed bool
}

type rawFrame struct {
	key      string // function + location, the frame's identity
	function string
	file     string
	line     int
}

// parseStackDump splits the text format of runtime.Stack into goroutines.
// The first goroutine (the caller, i.e. the sampler itself) is skipped.
//
//	goroutine 7 [chan receive]:
//	main.worker(0xc000010000)
//		/app/main.go:20 +0x3d
//	created by main.main in goroutine 1
//		/app/main.go:15 +0x25
func parseStackDump(dump []byte) *stackDump {
	d := &stackDump{parent: map[uint64]uint64{}}
	first := true
	for len(dump) > 0 {
		var block []byte
		if i := bytes.Index(dump, []byte("\n\n")); i >= 0 {
			block, dump = dump[:i], dump[i+2:]
		} else {
			block, dump = dump, nil
		}
		header, body, _ := bytes.Cut(block, []byte("\n"))
		id, ok := goroutineHeaderID(header)
		if !ok {
			continue
		}
		if i := bytes.LastIndex(body, []byte("created by ")); i >= 0 && (i == 0 || body[i-1] == '\n') {
			created, _, _ := bytes.Cut(body[i:], []byte("\n"))
			if _, in, found := bytes.Cut(created, []byte(" in goroutine ")); found {
				if parent, err := strconv.ParseUint(string(in), 10, 64); err == nil {
					d.parent[id] = parent
				}
			}
			body = body[:i]
		}
		if first {
			first = false
			continue
		}
		d.goroutines = append(d.goroutines, &dumpedGoroutine{id: id, body: body})
	}
	return d
}

// framesOf parses g's frames, leaf first.
func (g *dumpedGoroutine) framesOf() []rawFrame {
	if g.parsed {
		return g.frames
	}
	g.parsed = true
	rest := g.body
	for len(rest) > 0 {
		var fn, loc []byte
		fn, rest, _ = bytes.Cut(rest, []byte("\n"))
		if len(rest) > 0 && rest[0] == '\t' {
			loc, rest, _ = bytes.Cut(rest, []byte("\n"))
		}
		if len(fn) == 0 || len(loc) == 0 || bytes.HasPrefix(fn, []byte("...")) {
			continue
		}
		g.frames = append(g.frames, parseFrame(fn, loc))
	}
	return g.frames
}

// goroutineHeaderID reads N from "goroutine N [state]:".
func goroutineHeaderID(header []byte) (uint64, bool) {
	rest, ok := bytes.CutPrefix(header, []byte("goroutine "))
	if !ok {
		return 0, false
	}
	idText, _, _ := bytes.Cut(rest, []byte(" "))
	id, err := strconv.ParseUint(string(idText), 10, 64)
	return id, err == nil
}

// parseFrame reads "pkg.(*T).Method(0x1, 0x2)" and "\t/path/file.go:42 +0x1d".
func parseFrame(fn, loc []byte) rawFrame {
	if bytes.HasSuffix(fn, []byte(")")) {
		if i := bytes.LastIndexByte(fn, '('); i > 0 {
			fn = fn[:i]
		}
	}
	loc = bytes.TrimPrefix(loc, []byte("\t"))
	if i := bytes.Index(loc, []byte(" +0x")); i >= 0 {
		loc = loc[:i]
	}
	file, line := loc, 0
	if i := bytes.LastIndexByte(loc, ':'); i >= 0 {
		file = loc[:i]
		line, _ = strconv.Atoi(string(loc[i+1:]))
	}
	return rawFrame{
		key:      string(fn) + "@" + string(loc),
		function: string(fn),
		file:     string(file),
		line:     line,
	}
}

// descendsFrom reports whether goroutine id is root or was started by root,
// directly or through intermediate goroutines that are still alive. Ids are
// handed out from per-P caches, so a child's id can be lower than its
// parent's; the hop limit guards against cycles instead.
func (d *stackDump) descendsFrom(id, root uint64) bool {
	for range 64 {
		if id == root {
			return true
		}
		parent, ok := d.parent[id]
		if !ok {
			return false
		}
		id = parent
	}
	return false
}

// currentGoroutineID parses the calling goroutine's id from its stack header.
func currentGoroutineID() uint64 {
	var buf [64]byte
	n := runtime.Stack(buf[:], false)
	header, _, _ := bytes.Cut(buf[:n], []byte("\n"))
	id, _ := goroutineHeaderID(header)
	return id
}
