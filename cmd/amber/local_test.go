package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// runApp runs the CLI with a captured writer, returning stdout.
func runApp(t *testing.T, args ...string) (string, error) {
	t.Helper()
	app := newApp()
	var out bytes.Buffer
	app.Writer = &out
	err := app.Run(append([]string{"amber"}, args...))
	return out.String(), err
}

func setupSource(t *testing.T) string {
	t.Helper()
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "hello.txt"), []byte("hello"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(src, "sub"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "sub", "world.txt"), []byte("world"), 0o644); err != nil {
		t.Fatal(err)
	}
	return src
}

func TestImportLsRestoreRoundTrip(t *testing.T) {
	store := t.TempDir()
	src := setupSource(t)

	out, err := runApp(t, "--store", store, "import", "--no-progress", "--ref", "snap", src)
	if err != nil {
		t.Fatalf("import: %v", err)
	}
	root := strings.TrimSpace(out)
	if len(root) != 64 {
		t.Fatalf("import output not a hex key: %q", out)
	}

	out, err = runApp(t, "--store", store, "ls", "ref:snap")
	if err != nil {
		t.Fatalf("ls: %v", err)
	}
	if !strings.Contains(out, "hello.txt") || !strings.Contains(out, "sub") {
		t.Fatalf("ls output missing entries:\n%s", out)
	}

	dest := filepath.Join(t.TempDir(), "restored")
	if _, err := runApp(t, "--store", store, "restore", "ref:snap", dest); err != nil {
		t.Fatalf("restore: %v", err)
	}
	// Re-importing the restored tree must reproduce the same root key.
	store2 := t.TempDir()
	out, err = runApp(t, "--store", store2, "import", "--no-progress", dest)
	if err != nil {
		t.Fatalf("re-import: %v", err)
	}
	if strings.TrimSpace(out) != root {
		t.Fatalf("restored tree re-imports to %s, want %s", strings.TrimSpace(out), root)
	}
}

func TestRefListHidesTrackingNamespace(t *testing.T) {
	store := t.TempDir()
	src := setupSource(t)
	out, err := runApp(t, "--store", store, "import", "--no-progress", "--ref", "visible", src)
	if err != nil {
		t.Fatal(err)
	}
	root := strings.TrimSpace(out)

	// Plant a tracking ref directly (as push/pull will) via the store API:
	// use ref get to prove it resolves, but list must hide it.
	if _, err := runApp(t, "--store", store, "ref", "set", "remotes/abc/visible", root); err == nil {
		t.Fatal("ref set must refuse the remotes/ namespace")
	}

	out, err = runApp(t, "--store", store, "ref", "list")
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out, "visible") {
		t.Fatalf("ref list lost the normal ref:\n%s", out)
	}
	if strings.Contains(out, "remotes/") {
		t.Fatalf("ref list must hide remotes/:\n%s", out)
	}
}

func TestImportRefusesTrackingNamespace(t *testing.T) {
	store := t.TempDir()
	src := setupSource(t)
	if _, err := runApp(t, "--store", store, "import", "--no-progress", "--ref", "remotes/x/y", src); err == nil {
		t.Fatal("import --ref remotes/... must be refused")
	}
}
