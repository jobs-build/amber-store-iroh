package wantsync

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/fables-for-robots/amber-store-core/ingest"
	"github.com/fables-for-robots/amber-store-core/key"
	"github.com/fables-for-robots/amber-store-core/packstore"
)

// buildTree ingests a small directory tree into a fresh packstore and
// returns the store and root key.
func buildTree(t *testing.T) (*packstore.Store, key.Key) {
	t.Helper()
	src := t.TempDir()
	for p, content := range map[string]string{
		"a.txt":         "alpha",
		"sub/b.txt":     "beta",
		"sub/deep/c.go": "gamma content that is a bit longer",
	} {
		full := filepath.Join(src, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	st := openStore(t)
	root, _, err := ingest.Dir(st, src, ingest.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	return st, root
}

func openStore(t *testing.T) *packstore.Store {
	t.Helper()
	st, err := packstore.Open(filepath.Join(t.TempDir(), "packstore"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	return st
}

func TestWantsEmptyStoreWantsRoot(t *testing.T) {
	_, root := buildTree(t)
	dest := openStore(t)
	wants, err := Wants(dest, []key.Key{root}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(wants) != 1 || wants[0] != root {
		t.Fatalf("want [root], got %v", wants)
	}
}

func TestWantsCompleteStorePrunes(t *testing.T) {
	st, root := buildTree(t)
	wants, err := Wants(st, []key.Key{root}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(wants) != 0 {
		t.Fatalf("complete store must prune, got wants %v", wants)
	}
}

// TestWantsPartialSubtreeDescends is the resume-correctness case from the
// spec: the root object is present but its children are missing (an
// interrupted transfer stores parents before children), so presence alone
// must NOT prune.
func TestWantsPartialSubtreeDescends(t *testing.T) {
	st, root := buildTree(t)
	dest := openStore(t)
	rootBytes, err := st.Get(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := dest.Put(root, rootBytes); err != nil {
		t.Fatal(err)
	}
	wants, err := Wants(dest, []key.Key{root}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(wants) != 1 || wants[0] != root {
		t.Fatalf("partial subtree must still be wanted, got %v", wants)
	}
}

func TestWantsDedupes(t *testing.T) {
	_, root := buildTree(t)
	dest := openStore(t)
	wants, err := Wants(dest, []key.Key{root, root, root}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if len(wants) != 1 {
		t.Fatalf("want deduped [root], got %v", wants)
	}
}

func TestKeyCodec(t *testing.T) {
	_, root := buildTree(t)
	ks, err := decodeKeys(encodeKeys([]key.Key{root}))
	if err != nil {
		t.Fatal(err)
	}
	if len(ks) != 1 || ks[0] != root {
		t.Fatalf("round trip: %v", ks)
	}
	if _, err := decodeKeys([][]byte{{1, 2, 3}}); err == nil {
		t.Fatal("short key must fail")
	}
}
