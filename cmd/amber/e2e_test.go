package main

import (
	"context"
	"log/slog"
	"net/netip"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/fables-for-robots/amber-store-core/packstore"
	"github.com/fables-for-robots/amber-store-core/refstore"
	"github.com/fables-for-robots/amber-store-iroh/protocol"
	"github.com/fables-for-robots/amber-store-iroh/server"
	"github.com/tmc/go-iroh/iroh"
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

	args := append([]string{"--store", storeA, "push"}, netArgs(id, addrArgs, "snap")...)
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
	args = append([]string{"--store", storeB, "pull"}, netArgs(id, addrArgs, "snap")...)
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
	args := append([]string{"--store", storeA, "push"}, netArgs(id, addrArgs, "snap")...)
	if _, err := runApp(t, args...); err != nil {
		t.Fatalf("initial push: %v", err)
	}

	// B, never having pulled, tries to create the same ref: CAS refuses.
	if _, err := runApp(t, "--store", storeB, "import", "--no-progress", "--ref", "snap", srcB); err != nil {
		t.Fatal(err)
	}
	args = append([]string{"--store", storeB, "push"}, netArgs(id, addrArgs, "snap")...)
	_, err := runApp(t, args...)
	if err == nil || !strings.Contains(err.Error(), "pull first") {
		t.Fatalf("want cas-mismatch guidance, got %v", err)
	}

	// Idempotent re-push from A (tracking matches) still succeeds.
	args = append([]string{"--store", storeA, "push"}, netArgs(id, addrArgs, "snap")...)
	if _, err := runApp(t, args...); err != nil {
		t.Fatalf("idempotent re-push: %v", err)
	}

	// B forces, then A's next push must now fail CAS.
	args = append([]string{"--store", storeB, "push", "--force"}, netArgs(id, addrArgs, "snap")...)
	if _, err := runApp(t, args...); err != nil {
		t.Fatalf("forced push: %v", err)
	}
	args = append([]string{"--store", storeA, "push"}, netArgs(id, addrArgs, "snap")...)
	if _, err := runApp(t, args...); err == nil {
		t.Fatal("A's push after B's force must fail CAS")
	}

	// A pulls (adopting B's tree), then pushes cleanly.
	args = append([]string{"--store", storeA, "pull"}, netArgs(id, addrArgs, "snap")...)
	if _, err := runApp(t, args...); err != nil {
		t.Fatalf("pull after conflict: %v", err)
	}
	args = append([]string{"--store", storeA, "push"}, netArgs(id, addrArgs, "snap")...)
	if _, err := runApp(t, args...); err != nil {
		t.Fatalf("push after pull: %v", err)
	}
}

func TestE2EPullUnknownRef(t *testing.T) {
	id, addrArgs := startServer(t)
	store := t.TempDir()
	args := append([]string{"--store", store, "pull"}, netArgs(id, addrArgs, "nope")...)
	_, err := runApp(t, args...)
	if err == nil || !strings.Contains(err.Error(), protocol.CodeUnknownRef) {
		t.Fatalf("want unknown-ref, got %v", err)
	}
}
