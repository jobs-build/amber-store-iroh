package server

import (
	"log/slog"
	"net"
	"path/filepath"
	"slices"
	"testing"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/core/refstore"
	"github.com/amber-store/transport-iroh/protocol"
)

// gcTestServer opens a Server over throwaway stores with one raw ref
// record named "present" (handlePin/handlePull only check existence).
func gcTestServer(t *testing.T) *Server {
	t.Helper()
	dir := t.TempDir()
	objects, err := packstore.Open(filepath.Join(dir, "objects"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { objects.Close() })
	refs, err := refstore.Open(filepath.Join(dir, "refs"), false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { refs.Close() })
	if err := refs.Put("present", []byte{0xa0}); err != nil { // any bytes: existence is all these paths check
		t.Fatal(err)
	}
	return New(slog.Default(), objects, refs)
}

// exchange runs one HandleStream conversation over a pipe.
func exchange(t *testing.T, srv *Server, send protocol.Msg) protocol.Msg {
	t.Helper()
	c, s := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.HandleStream("test-peer", s)
	}()
	if err := protocol.WriteMsg(c, send); err != nil {
		t.Fatal(err)
	}
	m, err := protocol.ReadMsg(c)
	if err != nil {
		t.Fatal(err)
	}
	c.Close()
	<-done
	return m
}

func TestPinAssert(t *testing.T) {
	srv := gcTestServer(t)
	var pinned []string
	srv.SetOnPin(func(name string) { pinned = append(pinned, name) })

	m := exchange(t, srv, protocol.Msg{Type: protocol.TPin, Names: []string{"present", "absent"}})
	if m.Type != protocol.TOK {
		t.Fatalf("reply type %d, want TOK", m.Type)
	}
	if !slices.Equal(pinned, []string{"present"}) {
		t.Fatalf("pinned = %v, want [present]", pinned)
	}
}

func TestPinWithoutHookStillOK(t *testing.T) {
	srv := gcTestServer(t)
	if m := exchange(t, srv, protocol.Msg{Type: protocol.TPin, Names: []string{"present"}}); m.Type != protocol.TOK {
		t.Fatalf("reply type %d, want TOK", m.Type)
	}
}

func TestPullFiresOnAccess(t *testing.T) {
	srv := gcTestServer(t)
	var accessed []string
	srv.SetOnAccess(func(name string) { accessed = append(accessed, name) })

	c, s := net.Pipe()
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.HandleStream("test-peer", s)
	}()
	if err := protocol.WriteMsg(c, protocol.Msg{Type: protocol.TPull, Name: "present"}); err != nil {
		t.Fatal(err)
	}
	m, err := protocol.ReadMsg(c)
	if err != nil || m.Type != protocol.TRef {
		t.Fatalf("m=%+v err=%v, want TRef", m, err)
	}
	// End the transfer: an empty TWants finishes the Send loop.
	if err := protocol.WriteMsg(c, protocol.Msg{Type: protocol.TWants}); err != nil {
		t.Fatal(err)
	}
	c.Close()
	<-done
	if !slices.Equal(accessed, []string{"present"}) {
		t.Fatalf("accessed = %v, want [present]", accessed)
	}
}

func TestPushFiresOnAccess(t *testing.T) {
	srv := testServer(t)
	var accessed []string
	srv.SetOnAccess(func(name string) { accessed = append(accessed, name) })
	st, root := clientStore(t)
	if m, err := doPush(t, srv, st, "seen/ref", root, true, nil); err != nil || m.Type != protocol.TOK {
		t.Fatalf("push: m=%+v err=%v", m, err)
	}
	if !slices.Equal(accessed, []string{"seen/ref"}) {
		t.Fatalf("accessed = %v, want [seen/ref]", accessed)
	}
}

func TestPushTakesRefGuard(t *testing.T) {
	srv := testServer(t)
	g := &recordingGuard{}
	srv.SetRefGuard(g)
	st, root := clientStore(t)
	m, err := doPush(t, srv, st, "guarded/ref", root, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != protocol.TOK {
		t.Fatalf("want TOK, got %+v", m)
	}
	if g.prepared != 1 || g.commits != 1 || g.aborts != 0 {
		t.Fatalf("guard prepared=%d commits=%d aborts=%d, want 1/1/0", g.prepared, g.commits, g.aborts)
	}
	if _, err := srv.refs.Get("guarded/ref"); err != nil {
		t.Fatalf("ref not written: %v", err)
	}
}

type recordingGuard struct {
	prepared int
	commits  int
	aborts   int
}

func (g *recordingGuard) PrepareRef(root key.Key) (func(), func(), error) {
	g.prepared++
	return func() { g.commits++ }, func() { g.aborts++ }, nil
}
