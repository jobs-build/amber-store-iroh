package main

import (
	"context"
	"encoding/hex"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fables-for-robots/amber-store-core/fstree"
	"github.com/fables-for-robots/amber-store-core/ingest"
	"github.com/fables-for-robots/amber-store-core/key"
	"github.com/fables-for-robots/amber-store-core/packstore"
	"github.com/fables-for-robots/amber-store-core/refstore"
	"github.com/fables-for-robots/amber-store-iroh/protocol"
	"github.com/fables-for-robots/amber-store-iroh/server"
	"github.com/fables-for-robots/amber-store-iroh/wantsync"
	"github.com/tmc/go-iroh/iroh"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
)

// startServer binds an in-process amber server on an ephemeral identity
// and returns its endpoint ID and direct --addr values. Everything stays
// on the local machine: no relays, no discovery.
func startServer(t *testing.T) (id string, addrArgs []string) {
	t.Helper()
	dir := t.TempDir()
	objects, err := packstore.Open(filepath.Join(dir, "packstore"), packstore.WithSync(true))
	if err != nil {
		t.Fatal(err)
	}
	refs, err := refstore.Open(filepath.Join(dir, "refs"), true)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	// Bind explicitly to the IPv6 loopback address rather than the default
	// wildcard: ep.Addr().IPAddrs() otherwise reports the unspecified
	// "[::]:port" bind address, which is not itself dialable (connect
	// hangs until timeout). Binding to loopback makes Addr() report a
	// concrete, locally-dialable "[::1]:port", matching how go-iroh's own
	// offline tests obtain a connectable local address.
	ep, err := iroh.Bind(ctx, iroh.WithALPNs(protocol.ALPN), iroh.WithBindAddr(netip.AddrPortFrom(netip.IPv6Loopback(), 0)))
	if err != nil {
		t.Fatal(err)
	}

	srv := server.New(slog.New(slog.NewTextHandler(os.Stderr, nil)), objects, refs)
	done := make(chan struct{})
	go func() {
		defer close(done)
		srv.Serve(ctx, ep, 5*time.Second)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		ep.Shutdown(context.Background())
		refs.Close()
		objects.Close()
	})

	for _, ap := range ep.Addr().IPAddrs() {
		addrArgs = append(addrArgs, "--addr", ap.String())
	}
	if len(addrArgs) == 0 {
		t.Fatal("server endpoint has no direct addresses")
	}
	return ep.ID().String(), addrArgs
}

func netArgs(id string, addrArgs []string, rest ...string) []string {
	return append(append([]string{"--server", id}, addrArgs...), rest...)
}

func TestE2EPushPullRestoreRoundTrip(t *testing.T) {
	id, addrArgs := startServer(t)
	src := setupSource(t)
	storeA, storeB := t.TempDir(), t.TempDir()

	out, err := runApp(t, "--store", storeA, "import", "--no-progress", "--ref", "snap", src)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	root := strings.TrimSpace(out)

	args := append([]string{"--store", storeA, "push", "--no-progress"}, netArgs(id, addrArgs, "snap")...)
	if out, err := runApp(t, args...); err != nil {
		t.Fatalf("push: %v (%s)", err, out)
	}

	// Remote listing sees the ref.
	args = append([]string{"refs"}, netArgs(id, addrArgs)...)
	out, err = runApp(t, args...)
	if err != nil {
		t.Fatalf("refs: %v", err)
	}
	if !strings.Contains(out, "snap") || !strings.Contains(out, root) {
		t.Fatalf("refs output missing pushed ref:\n%s", out)
	}

	// Pull into a fresh store and restore.
	args = append([]string{"--store", storeB, "pull", "--no-progress"}, netArgs(id, addrArgs, "snap")...)
	if out, err := runApp(t, args...); err != nil {
		t.Fatalf("pull: %v (%s)", err, out)
	}
	dest := filepath.Join(t.TempDir(), "restored")
	if _, err := runApp(t, "--store", storeB, "restore", "ref:snap", dest); err != nil {
		t.Fatalf("restore: %v", err)
	}
	storeC := t.TempDir()
	out, err = runApp(t, "--store", storeC, "import", "--no-progress", dest)
	if err != nil {
		t.Fatalf("re-import: %v", err)
	}
	if strings.TrimSpace(out) != root {
		t.Fatalf("restored tree re-imports to %s, want %s", strings.TrimSpace(out), root)
	}

	// Tracking refs stay hidden from ref list.
	out, err = runApp(t, "--store", storeA, "ref", "list")
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(out, "remotes/") {
		t.Fatalf("ref list leaks tracking refs:\n%s", out)
	}
}

func TestE2ECASConflictAndForce(t *testing.T) {
	id, addrArgs := startServer(t)
	srcA, srcB := setupSource(t), t.TempDir()
	if err := os.WriteFile(filepath.Join(srcB, "different.txt"), []byte("other content"), 0o644); err != nil {
		t.Fatal(err)
	}
	storeA, storeB := t.TempDir(), t.TempDir()

	// A creates the ref.
	if _, err := runApp(t, "--store", storeA, "import", "--no-progress", "--ref", "snap", srcA); err != nil {
		t.Fatal(err)
	}
	args := append([]string{"--store", storeA, "push", "--no-progress"}, netArgs(id, addrArgs, "snap")...)
	if _, err := runApp(t, args...); err != nil {
		t.Fatalf("initial push: %v", err)
	}

	// B, never having pulled, tries to create the same ref: CAS refuses.
	if _, err := runApp(t, "--store", storeB, "import", "--no-progress", "--ref", "snap", srcB); err != nil {
		t.Fatal(err)
	}
	args = append([]string{"--store", storeB, "push", "--no-progress"}, netArgs(id, addrArgs, "snap")...)
	_, err := runApp(t, args...)
	if err == nil || !strings.Contains(err.Error(), "pull first") {
		t.Fatalf("want cas-mismatch guidance, got %v", err)
	}

	// Idempotent re-push from A (tracking matches) still succeeds.
	args = append([]string{"--store", storeA, "push", "--no-progress"}, netArgs(id, addrArgs, "snap")...)
	if _, err := runApp(t, args...); err != nil {
		t.Fatalf("idempotent re-push: %v", err)
	}

	// B forces, then A's next push must now fail CAS.
	args = append([]string{"--store", storeB, "push", "--force", "--no-progress"}, netArgs(id, addrArgs, "snap")...)
	if _, err := runApp(t, args...); err != nil {
		t.Fatalf("forced push: %v", err)
	}
	args = append([]string{"--store", storeA, "push", "--no-progress"}, netArgs(id, addrArgs, "snap")...)
	if _, err := runApp(t, args...); err == nil {
		t.Fatal("A's push after B's force must fail CAS")
	}

	// A pulls (adopting B's tree), then pushes cleanly.
	args = append([]string{"--store", storeA, "pull", "--no-progress"}, netArgs(id, addrArgs, "snap")...)
	if _, err := runApp(t, args...); err != nil {
		t.Fatalf("pull after conflict: %v", err)
	}
	args = append([]string{"--store", storeA, "push", "--no-progress"}, netArgs(id, addrArgs, "snap")...)
	if _, err := runApp(t, args...); err != nil {
		t.Fatalf("push after pull: %v", err)
	}
}

// layeredSource builds a tree deep enough for a multi-round want loop:
// round 1 asks for the root directory, round 2 for its children (two
// top-level files plus the sub directory), round 3 for sub's children.
func layeredSource(t *testing.T) string {
	t.Helper()
	src := t.TempDir()
	if err := os.MkdirAll(filepath.Join(src, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	for p, content := range map[string]string{
		"a.txt":     "alpha top level",
		"b.txt":     "bravo top level",
		"sub/c.txt": "charlie nested",
		"sub/d.txt": "delta nested",
	} {
		if err := os.WriteFile(filepath.Join(src, p), []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	return src
}

// rawPush pushes root under name with a hand-rolled sender so the test can
// observe the server's want lists round by round, and can cut the
// connection mid-transfer. With interruptAfter > 0 it answers that many
// rounds and then drops the connection; otherwise it runs to completion
// and requires a final TOK. The store at storeDir must not be open
// elsewhere: packstores are single-owner.
func rawPush(t *testing.T, id string, addrArgs []string, storeDir, name string, root key.Key, interruptAfter int) (rounds [][]key.Key) {
	t.Helper()
	objects, err := packstore.Open(filepath.Join(storeDir, "packstore"))
	if err != nil {
		t.Fatal(err)
	}
	defer objects.Close()

	ctx := context.Background()
	ep, err := iroh.Bind(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer ep.Shutdown(context.Background())
	sid, err := irohkey.ParseEndpointID(id)
	if err != nil {
		t.Fatal(err)
	}
	addr := netaddr.NewEndpointAddr(sid)
	for i := 0; i < len(addrArgs); i += 2 {
		ap, err := netip.ParseAddrPort(addrArgs[i+1])
		if err != nil {
			t.Fatal(err)
		}
		addr = addr.WithIP(ap)
	}
	conn, err := ep.Connect(ctx, addr, protocol.ALPN)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	stream, err := conn.OpenStreamConn(ctx)
	if err != nil {
		t.Fatal(err)
	}

	// Forced push: CAS is not what this test is about.
	req := protocol.Msg{Type: protocol.TPush, Name: name, Root: root[:], CAS: false}
	if err := protocol.WriteMsg(stream, req); err != nil {
		t.Fatal(err)
	}
	for {
		m, err := protocol.ReadMsg(stream)
		if err != nil {
			t.Fatalf("round %d: read wants: %v (rounds so far: %s)", len(rounds)+1, err, formatRounds(rounds))
		}
		if m.Type != protocol.TWants {
			t.Fatalf("round %d: want TWants, got %+v", len(rounds)+1, m)
		}
		if len(m.Keys) == 0 {
			break
		}
		keys := make([]key.Key, len(m.Keys))
		for i, b := range m.Keys {
			k, err := key.Parse(b)
			if err != nil {
				t.Fatal(err)
			}
			keys[i] = k
		}
		rounds = append(rounds, keys)
		seq := func(yield func(fstree.Object, error) bool) {
			for _, k := range keys {
				data, err := objects.Get(k)
				if err != nil {
					yield(fstree.Object{}, err)
					return
				}
				if !yield(fstree.Object{Key: k, Bytes: data}, nil) {
					return
				}
			}
		}
		if err := protocol.SendPack(stream, seq); err != nil {
			t.Fatalf("round %d: send pack: %v", len(rounds), err)
		}
		if interruptAfter > 0 && len(rounds) == interruptAfter {
			// Wait for the next want list before dropping the
			// connection: closing a QUIC connection discards data the
			// peer has not read yet, and the next TWants frame is
			// proof this round's objects were verified and stored.
			if m, err := protocol.ReadMsg(stream); err != nil || m.Type != protocol.TWants {
				t.Fatalf("round %d: want a further TWants before interrupting, got %+v %v", len(rounds), m, err)
			}
			// Drop the connection mid-transfer, exactly as a killed
			// client would.
			conn.Close()
			return rounds
		}
	}
	if interruptAfter > 0 {
		t.Fatalf("transfer finished before round %d: %s", interruptAfter, formatRounds(rounds))
	}
	m, err := protocol.ReadMsg(stream)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != protocol.TOK {
		t.Fatalf("want TOK, got %+v", m)
	}
	return rounds
}

func formatRounds(rounds [][]key.Key) string {
	var b strings.Builder
	for i, r := range rounds {
		fmt.Fprintf(&b, "\nround %d (%d keys):", i+1, len(r))
		for _, k := range r {
			fmt.Fprintf(&b, " %s", k)
		}
	}
	return b.String()
}

// TestE2EInterruptedPushResume interrupts a push after the root's children
// land, then pushes again: the resumed push must skip the subtrees already
// complete on the server, still transfer the incomplete one, and commit
// the ref.
func TestE2EInterruptedPushResume(t *testing.T) {
	id, addrArgs := startServer(t)
	src := layeredSource(t)
	store := t.TempDir()

	out, err := runApp(t, "--store", store, "import", "--no-progress", "--ref", "snap", src)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	rootHex := strings.TrimSpace(out)
	rootBytes, err := hex.DecodeString(rootHex)
	if err != nil {
		t.Fatal(err)
	}
	root, err := key.Parse(rootBytes)
	if err != nil {
		t.Fatal(err)
	}

	first := rawPush(t, id, addrArgs, store, "snap", root, 2)
	if len(first) != 2 || len(first[0]) != 1 || first[0][0] != root {
		t.Fatalf("round 1 must be the root alone:%s", formatRounds(first))
	}
	if len(first[1]) < 2 {
		t.Fatalf("round 2 must carry the root's children:%s", formatRounds(first))
	}

	second := rawPush(t, id, addrArgs, store, "snap", root, 0)
	if len(second) == 0 {
		t.Fatal("resumed push transferred nothing: the sub subtree was still missing")
	}
	// The complete top-level blobs delivered before the interruption must
	// not be requested again.
	asked := map[key.Key]bool{}
	for _, r := range second {
		for _, k := range r {
			asked[k] = true
		}
	}
	reused := 0
	for _, k := range first[1] {
		if !asked[k] {
			reused++
		}
	}
	if reused == 0 {
		t.Fatalf("resume re-requested every already-delivered child:\nfirst:%s\nsecond:%s",
			formatRounds(first), formatRounds(second))
	}

	args := append([]string{"refs"}, netArgs(id, addrArgs)...)
	out, err = runApp(t, args...)
	if err != nil {
		t.Fatalf("refs: %v", err)
	}
	if !strings.Contains(out, "snap") || !strings.Contains(out, rootHex) {
		t.Fatalf("resumed push did not commit the ref:\n%s", out)
	}
}

// TestE2EPushPullRefuseTrackingNamespace checks the reserved namespace is
// refused locally, before any dial: the --server value is bogus, so a
// dial attempt would surface as a connect error instead.
func TestE2EPushPullRefuseTrackingNamespace(t *testing.T) {
	store := t.TempDir()
	for _, op := range []string{"push", "pull"} {
		_, err := runApp(t, "--store", store, op, "--server", "not-an-endpoint-id", "remotes/x/y")
		if err == nil || !strings.Contains(err.Error(), "reserved for remote-tracking refs") {
			t.Fatalf("%s remotes/x/y: want reserved-namespace error, got %v", op, err)
		}
	}
}

func TestE2EPullUnknownRef(t *testing.T) {
	id, addrArgs := startServer(t)
	store := t.TempDir()
	args := append([]string{"--store", store, "pull", "--no-progress"}, netArgs(id, addrArgs, "nope")...)
	_, err := runApp(t, args...)
	if err == nil || !strings.Contains(err.Error(), protocol.CodeUnknownRef) {
		t.Fatalf("want unknown-ref, got %v", err)
	}
}

// TestE2EDialRacesDeadCandidates pins the gx10 incident: candidate lists
// can carry unreachable addresses (container bridges) that sort before
// the live ones, and a serial walk exhausts the handshake budget on
// them. The dial must race candidates so one dead, low-sorting address
// costs nothing.
func TestE2EDialRacesDeadCandidates(t *testing.T) {
	id, addrArgs := startServer(t)
	// addrArgs alternates "--addr", "host:port"; keep the values only.
	var live []string
	for i := 1; i < len(addrArgs); i += 2 {
		live = append(live, addrArgs[i])
	}
	// 192.0.2.0/24 (TEST-NET-1) is reserved and never routed; it sorts
	// before every loopback/LAN candidate the server yields.
	cands := append([]string{"192.0.2.1:9"}, live...)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	start := time.Now()
	sc, err := dialServer(ctx, id, cands, "")
	if err != nil {
		t.Fatalf("dial with dead candidate: %v", err)
	}
	defer sc.Close()
	elapsed := time.Since(start)
	if elapsed > 4*time.Second {
		t.Fatalf("dial took %v; a dead candidate must not delay the race", elapsed)
	}
}

// TestRunSendersOldServerFallback speaks to a peer that ignores
// DataConns and opens straight with TWants — the pre-sharding protocol.
// The consumed frame must be replayed and the transfer completed over
// the single control channel.
func TestRunSendersOldServerFallback(t *testing.T) {
	srcDir := setupSource(t)
	src, err := packstore.Open(filepath.Join(t.TempDir(), "packstore"))
	if err != nil {
		t.Fatal(err)
	}
	defer src.Close()
	root, _, err := ingest.Dir(src, srcDir, ingest.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	dest, err := packstore.Open(filepath.Join(t.TempDir(), "packstore"))
	if err != nil {
		t.Fatal(err)
	}
	defer dest.Close()

	a, b := net.Pipe()
	defer a.Close()
	recvDone := make(chan error, 1)
	go func() {
		_, err := wantsync.Receive([]io.ReadWriter{b}, dest, root, 0, nil)
		recvDone <- err
		b.Close()
	}()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := runSenders(ctx, nil, a, src, nil, 4); err != nil {
		t.Fatalf("fallback send: %v", err)
	}
	if err := <-recvDone; err != nil {
		t.Fatalf("old-style receiver: %v", err)
	}
	if err := fstree.CheckComplete(root, dest.Get, dest.Has, 0); err != nil {
		t.Fatalf("dest incomplete after fallback transfer: %v", err)
	}
}

// TestE2ESingleConn pins --conns 1: the request carries DataConns 0, the
// server sends no TAccept, and the exchange is byte-identical to the
// pre-sharding protocol.
func TestE2ESingleConn(t *testing.T) {
	id, addrArgs := startServer(t)
	src := setupSource(t)
	storeA, storeB := t.TempDir(), t.TempDir()

	out, err := runApp(t, "--store", storeA, "import", "--no-progress", "--ref", "one", src)
	if err != nil {
		t.Fatal(err)
	}
	root := strings.TrimSpace(out)
	args := append([]string{"--store", storeA, "push", "--no-progress", "--conns", "1"}, netArgs(id, addrArgs, "one")...)
	if _, err := runApp(t, args...); err != nil {
		t.Fatalf("push --conns 1: %v", err)
	}
	args = append([]string{"--store", storeB, "pull", "--no-progress", "--conns", "1"}, netArgs(id, addrArgs, "one")...)
	if _, err := runApp(t, args...); err != nil {
		t.Fatalf("pull --conns 1: %v", err)
	}
	out, err = runApp(t, "--store", storeB, "ref", "get", "one")
	if err != nil {
		t.Fatal(err)
	}
	if strings.TrimSpace(out) != root {
		t.Fatalf("pulled ref at %s, want %s", strings.TrimSpace(out), root)
	}
}
