# Amber-Store Iroh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** A p2p distributed amber-store: `amber-serve` hosts a store over iroh QUIC; the `amber` client owns a local store copy, imports directories, and pushes/pulls refs with have/want negotiation.

**Architecture:** Two binaries over four packages. `protocol` defines CBOR-framed wire messages and chunked amberpack streaming; `wantsync` implements both halves of the server-driven want loop (completeness-check pruning, verified writes); `server` dispatches push/pull/ref-list per QUIC stream with per-name CAS locking; `cmd/amber` mirrors amber-store-core's CLI locally and adds `push`/`pull`/`refs` network commands with remote-tracking refs.

**Tech Stack:** Go 1.26, `github.com/fables-for-robots/amber-store-core` (packstore, refstore, reference, fstree, amberpack, ingest, tarexport, tarextract), `github.com/tmc/go-iroh`, `github.com/urfave/cli/v2`, `github.com/fxamacker/cbor/v2`.

**Spec:** `docs/superpowers/specs/2026-07-21-amber-store-iroh-design.md`. One deliberate deviation: received packs are NOT staged through core's `inbox` package — the receiver decodes the amberpack stream and writes objects straight into the packstore with `WriteParallel(..., WriteOpts{Verify: true})`. This is simpler for a synchronous round-based loop and adds content-vs-key verification of untrusted peer data; durability comes from `packstore.WithSync(true)`. The spec file is updated to match in Task 10.

## Global Constraints

- Repo: `/Users/dragan/fables-for-robots/amber-store-iroh`, module `github.com/fables-for-robots/amber-store-iroh`, Go `1.26.5`.
- Every Go command runs through the flake: `nix develop -c go <args>` (no system Go).
- Dependency versions: `github.com/fables-for-robots/amber-store-core` at branch `main` (commit `a37d35fa4ecfb2a6919fac30ca614941a9734e06` is the tested one), `github.com/tmc/go-iroh v0.0.0-20260714221401-b17af420bb03` (same as irohese), `github.com/urfave/cli/v2 v2.27.7`, `github.com/fxamacker/cbor/v2 v2.9.2` (same as core).
- amber-store-core is a private GitHub repo reached over SSH: before `go get`, run `git config --global url."git@github.com:fables-for-robots/".insteadOf "https://github.com/fables-for-robots/"` only if not already configured, and set `GOPRIVATE=github.com/fables-for-robots/*` (use `nix develop -c bash -c 'GOPRIVATE=github.com/fables-for-robots/* go get ...'`). If the fetch fails, STOP and report — do not switch to a `replace` directive.
- ALPN string is exactly `amber-store-iroh/1` everywhere (`protocol.ALPN`); never retype it inline.
- The local refstore namespace prefix for remote-tracking refs is exactly `remotes/` (`trackingPrefix` in `cmd/amber`).
- Two `key` packages exist: amber-store-core's (`key.Key`, 32-byte content keys) and go-iroh's (`key.EndpointID`, `key.SecretKey`). In files needing both, import go-iroh's as `irohkey`.
- Reference CLI conventions from the sibling repos: `urfave/cli/v2` apps, errors returned (not printed) from actions, `slog` text logging to stdout in servers.
- Commit after every task with a conventional message; run `nix develop -c gofmt -l .` (expect no output) and `nix develop -c go vet ./...` before each commit.

---

### Task 1: Dependencies + protocol frame codec

**Files:**
- Modify: `go.mod` (via `go get`)
- Create: `protocol/protocol.go`
- Test: `protocol/protocol_test.go`

**Interfaces:**
- Consumes: nothing (first task).
- Produces (later tasks rely on these exact names):
  - `const ALPN = "amber-store-iroh/1"`, `MaxFrame`, `ChunkSize`
  - Msg type constants `TPush, TPull, TRefList, TRef, TRefs, TWants, TData, TDataEnd, TOK, TErr` (1..10)
  - Error codes `CodeCASMismatch = "cas-mismatch"`, `CodeUnknownRef = "unknown-ref"`, `CodeBadRequest = "bad-request"`, `CodeInternal = "internal"`
  - `type Msg struct{...}` (fields below), `type RefInfo struct{...}`
  - `func WriteMsg(w io.Writer, m Msg) error`, `func ReadMsg(r io.Reader) (Msg, error)`
  - `var ErrProtocol = errors.New("protocol: unexpected frame")`
  - `type RemoteError struct{ Code, Text string; Current []byte }` with `Error() string`; `func RemoteFromMsg(m Msg) *RemoteError`

- [ ] **Step 1: Add dependencies**

```sh
cd /Users/dragan/fables-for-robots/amber-store-iroh
nix develop -c bash -c 'GOPRIVATE=github.com/fables-for-robots/* go get github.com/fables-for-robots/amber-store-core@main github.com/tmc/go-iroh@v0.0.0-20260714221401-b17af420bb03 github.com/urfave/cli/v2@v2.27.7 github.com/fxamacker/cbor/v2@v2.9.2'
nix develop -c go mod tidy   # will prune until code exists; rerun after Step 3
```
Expected: go.mod gains the requires (some marked `// indirect` until code imports them). If the core fetch fails with auth errors, STOP and report.

- [ ] **Step 2: Write the failing test**

`protocol/protocol_test.go`:

```go
package protocol

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"testing"
)

func TestMsgRoundTrip(t *testing.T) {
	msgs := []Msg{
		{Type: TPush, Name: "backups/home", Root: bytes.Repeat([]byte{7}, 32), CAS: true, ExpectedOld: bytes.Repeat([]byte{9}, 32)},
		{Type: TPush, Name: "n", Root: bytes.Repeat([]byte{7}, 32), CAS: true}, // ExpectedOld nil = must not exist
		{Type: TPull, Name: "backups/home"},
		{Type: TRefList},
		{Type: TRef, Record: []byte{0xa1, 0x00, 0x01}},
		{Type: TRefs, Refs: []RefInfo{{Name: "a", Key: bytes.Repeat([]byte{1}, 32), CreatedAt: 42, User: "u"}}},
		{Type: TWants, Keys: [][]byte{bytes.Repeat([]byte{2}, 32), bytes.Repeat([]byte{3}, 32)}},
		{Type: TWants}, // empty wants = done
		{Type: TData, Data: []byte("payload")},
		{Type: TDataEnd},
		{Type: TOK, Key: bytes.Repeat([]byte{4}, 32)},
		{Type: TErr, Code: CodeCASMismatch, Text: "remote ref changed", Current: bytes.Repeat([]byte{5}, 32)},
	}
	var buf bytes.Buffer
	for _, m := range msgs {
		if err := WriteMsg(&buf, m); err != nil {
			t.Fatalf("write %+v: %v", m, err)
		}
	}
	for _, want := range msgs {
		got, err := ReadMsg(&buf)
		if err != nil {
			t.Fatalf("read (want %+v): %v", want, err)
		}
		checkMsgEqual(t, got, want)
	}
	if _, err := ReadMsg(&buf); !errors.Is(err, io.EOF) {
		t.Fatalf("read past end: want io.EOF, got %v", err)
	}
}

func checkMsgEqual(t *testing.T, got, want Msg) {
	t.Helper()
	if got.Type != want.Type || got.Name != want.Name || got.CAS != want.CAS ||
		!bytes.Equal(got.Root, want.Root) || !bytes.Equal(got.ExpectedOld, want.ExpectedOld) ||
		!bytes.Equal(got.Record, want.Record) || !bytes.Equal(got.Data, want.Data) ||
		!bytes.Equal(got.Key, want.Key) || got.Code != want.Code || got.Text != want.Text ||
		!bytes.Equal(got.Current, want.Current) || len(got.Refs) != len(want.Refs) || len(got.Keys) != len(want.Keys) {
		t.Fatalf("round trip mismatch:\n got %+v\nwant %+v", got, want)
	}
	for i := range want.Refs {
		g, w := got.Refs[i], want.Refs[i]
		if g.Name != w.Name || g.CreatedAt != w.CreatedAt || g.User != w.User || !bytes.Equal(g.Key, w.Key) {
			t.Fatalf("refs[%d] mismatch: got %+v want %+v", i, g, w)
		}
	}
	for i := range want.Keys {
		if !bytes.Equal(got.Keys[i], want.Keys[i]) {
			t.Fatalf("keys[%d] mismatch", i)
		}
	}
}

func TestReadMsgRejectsOversizeFrame(t *testing.T) {
	var buf bytes.Buffer
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], MaxFrame+1)
	buf.Write(hdr[:])
	if _, err := ReadMsg(&buf); err == nil {
		t.Fatal("want error for oversize frame, got nil")
	}
}

func TestReadMsgTruncatedFrame(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMsg(&buf, Msg{Type: TDataEnd}); err != nil {
		t.Fatal(err)
	}
	trunc := buf.Bytes()[:buf.Len()-1]
	if _, err := ReadMsg(bytes.NewReader(trunc)); err == nil {
		t.Fatal("want error for truncated frame, got nil")
	}
}

func TestRemoteError(t *testing.T) {
	m := Msg{Type: TErr, Code: CodeUnknownRef, Text: "ref \"x\" not found"}
	re := RemoteFromMsg(m)
	if re.Code != CodeUnknownRef || re.Text != m.Text {
		t.Fatalf("RemoteFromMsg: %+v", re)
	}
	if re.Error() == "" {
		t.Fatal("empty Error()")
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `nix develop -c go test ./protocol/`
Expected: FAIL — compile errors (undefined `Msg`, `WriteMsg`, ...).

- [ ] **Step 4: Write the implementation**

`protocol/protocol.go`:

```go
// Package protocol defines the amber-store-iroh wire protocol: one
// bidirectional QUIC stream per operation carrying length-prefixed CBOR
// frames, with amberpack payloads chunked into TData frames.
package protocol

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"

	"github.com/fxamacker/cbor/v2"
)

const (
	// ALPN identifies this protocol during the QUIC handshake. Server and
	// client must agree byte-for-byte or connections are rejected.
	ALPN = "amber-store-iroh/1"

	// MaxFrame bounds one frame's CBOR payload, limiting the memory a
	// peer can force us to allocate.
	MaxFrame = 16 << 20

	// ChunkSize is how many amberpack bytes ride in one TData frame.
	ChunkSize = 1 << 20
)

// Frame types. Type selects which Msg fields are meaningful.
const (
	TPush    = 1  // client→server: push request (Name, Root, CAS, ExpectedOld)
	TPull    = 2  // client→server: pull request (Name)
	TRefList = 3  // client→server: list all refs
	TRef     = 4  // server→client: raw reference record (Record)
	TRefs    = 5  // server→client: ref listing (Refs)
	TWants   = 6  // receiver→sender: keys to transfer next (Keys; empty = done)
	TData    = 7  // sender→receiver: amberpack payload chunk (Data)
	TDataEnd = 8  // sender→receiver: end of one pack payload
	TOK      = 9  // server→client: push committed (Key)
	TErr     = 10 // either direction: terminal failure (Code, Text, Current)
)

// Error codes carried in TErr frames.
const (
	CodeCASMismatch = "cas-mismatch"
	CodeUnknownRef  = "unknown-ref"
	CodeBadRequest  = "bad-request"
	CodeInternal    = "internal"
)

// ErrProtocol reports a frame that is valid CBOR but wrong for the moment
// it arrived in the exchange.
var ErrProtocol = errors.New("protocol: unexpected frame")

// RefInfo is one reference in a TRefs listing.
type RefInfo struct {
	Name      string `cbor:"0,keyasint"`
	Key       []byte `cbor:"1,keyasint"`
	CreatedAt int64  `cbor:"2,keyasint"` // ns since the Unix epoch
	User      string `cbor:"3,keyasint,omitempty"`
}

// Msg is the single frame payload type.
type Msg struct {
	Type        int       `cbor:"0,keyasint"`
	Name        string    `cbor:"1,keyasint,omitempty"`
	Root        []byte    `cbor:"2,keyasint,omitempty"`
	CAS         bool      `cbor:"3,keyasint,omitempty"` // push precondition present (--force omits it)
	ExpectedOld []byte    `cbor:"4,keyasint,omitempty"` // with CAS: expected current key; nil = ref must not exist
	Record      []byte    `cbor:"5,keyasint,omitempty"` // canonical reference encoding, signature carried opaquely
	Refs        []RefInfo `cbor:"6,keyasint,omitempty"`
	Keys        [][]byte  `cbor:"7,keyasint,omitempty"`
	Data        []byte    `cbor:"8,keyasint,omitempty"`
	Key         []byte    `cbor:"9,keyasint,omitempty"`
	Code        string    `cbor:"10,keyasint,omitempty"`
	Text        string    `cbor:"11,keyasint,omitempty"`
	Current     []byte    `cbor:"12,keyasint,omitempty"` // cas-mismatch: the server's current key (nil = absent)
}

var (
	encMode cbor.EncMode
	decMode cbor.DecMode
)

func init() {
	var err error
	encMode, err = cbor.CanonicalEncOptions().EncMode()
	if err != nil {
		panic(err)
	}
	decMode, err = cbor.DecOptions{}.DecMode()
	if err != nil {
		panic(err)
	}
}

// WriteMsg writes one frame: 4-byte big-endian payload length, then the
// CBOR-encoded Msg.
func WriteMsg(w io.Writer, m Msg) error {
	payload, err := encMode.Marshal(m)
	if err != nil {
		return fmt.Errorf("protocol: encode frame: %w", err)
	}
	if len(payload) > MaxFrame {
		return fmt.Errorf("protocol: frame of %d bytes exceeds limit %d", len(payload), MaxFrame)
	}
	var hdr [4]byte
	binary.BigEndian.PutUint32(hdr[:], uint32(len(payload)))
	if _, err := w.Write(hdr[:]); err != nil {
		return err
	}
	_, err = w.Write(payload)
	return err
}

// ReadMsg reads one frame. A clean end-of-stream before the header
// surfaces as io.EOF; a stream cut mid-frame as io.ErrUnexpectedEOF.
func ReadMsg(r io.Reader) (Msg, error) {
	var hdr [4]byte
	if _, err := io.ReadFull(r, hdr[:]); err != nil {
		if errors.Is(err, io.ErrUnexpectedEOF) {
			return Msg{}, io.ErrUnexpectedEOF
		}
		return Msg{}, err
	}
	n := binary.BigEndian.Uint32(hdr[:])
	if n > MaxFrame {
		return Msg{}, fmt.Errorf("protocol: frame of %d bytes exceeds limit %d", n, MaxFrame)
	}
	payload := make([]byte, n)
	if _, err := io.ReadFull(r, payload); err != nil {
		return Msg{}, fmt.Errorf("protocol: short frame: %w", err)
	}
	var m Msg
	if err := decMode.Unmarshal(payload, &m); err != nil {
		return Msg{}, fmt.Errorf("protocol: decode frame: %w", err)
	}
	return m, nil
}

// RemoteError is a TErr frame surfaced as a Go error on the peer.
type RemoteError struct {
	Code    string
	Text    string
	Current []byte // cas-mismatch: the server's current key, nil when the ref is absent
}

func (e *RemoteError) Error() string {
	return fmt.Sprintf("remote: %s: %s", e.Code, e.Text)
}

// RemoteFromMsg converts a TErr frame into a *RemoteError.
func RemoteFromMsg(m Msg) *RemoteError {
	return &RemoteError{Code: m.Code, Text: m.Text, Current: m.Current}
}
```

- [ ] **Step 5: Run test to verify it passes**

Run: `nix develop -c bash -c 'go mod tidy && go test ./protocol/'`
Expected: `ok  github.com/fables-for-robots/amber-store-iroh/protocol`

- [ ] **Step 6: Commit**

```sh
nix develop -c gofmt -l . && nix develop -c go vet ./...
git add go.mod go.sum protocol/
git commit -m "feat: wire protocol frame codec (CBOR frames over QUIC streams)"
```

---

### Task 2: Pack streaming over frames

**Files:**
- Create: `protocol/pack.go`
- Test: `protocol/pack_test.go`

**Interfaces:**
- Consumes: Task 1 (`WriteMsg`, `ReadMsg`, `Msg`, `TData`, `TDataEnd`, `TErr`, `ErrProtocol`, `RemoteFromMsg`, `ChunkSize`).
- Produces:
  - `func SendPack(w io.Writer, objs iter.Seq2[fstree.Object, error]) error` — serializes objects as one amberpack, chunked into TData frames, terminated by TDataEnd. Returns the first error from `objs` without sending TDataEnd (caller sends TErr).
  - `func NewPackReader(r io.Reader) io.Reader` — reader over the pack bytes of a TData…TDataEnd sequence; a TErr frame surfaces as `*RemoteError`, any other frame as `ErrProtocol`.

- [ ] **Step 1: Write the failing test**

`protocol/pack_test.go`:

```go
package protocol

import (
	"bytes"
	"errors"
	"io"
	"iter"
	"testing"

	"github.com/fables-for-robots/amber-store-core/amberpack"
	"github.com/fables-for-robots/amber-store-core/fstree"
)

// testObjects builds n distinct valid blobs.
func testObjects(t *testing.T, n int) []fstree.Object {
	t.Helper()
	objs := make([]fstree.Object, n)
	for i := range objs {
		o, err := fstree.EncodeBlob(bytes.Repeat([]byte{byte(i + 1)}, 100+i))
		if err != nil {
			t.Fatal(err)
		}
		objs[i] = o
	}
	return objs
}

func seqOf(objs []fstree.Object, failAfter int, failErr error) iter.Seq2[fstree.Object, error] {
	return func(yield func(fstree.Object, error) bool) {
		for i, o := range objs {
			if failErr != nil && i == failAfter {
				yield(fstree.Object{}, failErr)
				return
			}
			if !yield(o, nil) {
				return
			}
		}
	}
}

func TestPackRoundTrip(t *testing.T) {
	objs := testObjects(t, 5)
	var buf bytes.Buffer
	if err := SendPack(&buf, seqOf(objs, -1, nil)); err != nil {
		t.Fatalf("SendPack: %v", err)
	}
	var got []fstree.Object
	for o, err := range amberpack.NewReader(NewPackReader(&buf)).All() {
		if err != nil {
			t.Fatalf("read pack: %v", err)
		}
		got = append(got, o)
	}
	if len(got) != len(objs) {
		t.Fatalf("got %d objects, want %d", len(got), len(objs))
	}
	for i := range objs {
		if got[i].Key != objs[i].Key || !bytes.Equal(got[i].Bytes, objs[i].Bytes) {
			t.Fatalf("object %d differs", i)
		}
	}
	// The stream must be positioned exactly after TDataEnd.
	if _, err := ReadMsg(&buf); !errors.Is(err, io.EOF) {
		t.Fatalf("stream not fully consumed: %v", err)
	}
}

func TestSendPackPropagatesSourceError(t *testing.T) {
	objs := testObjects(t, 3)
	boom := errors.New("boom")
	var buf bytes.Buffer
	if err := SendPack(&buf, seqOf(objs, 2, boom)); !errors.Is(err, boom) {
		t.Fatalf("want boom, got %v", err)
	}
}

func TestPackReaderSurfacesRemoteError(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMsg(&buf, Msg{Type: TData, Data: []byte("junk")}); err != nil {
		t.Fatal(err)
	}
	if err := WriteMsg(&buf, Msg{Type: TErr, Code: CodeInternal, Text: "sender died"}); err != nil {
		t.Fatal(err)
	}
	_, err := io.ReadAll(NewPackReader(&buf))
	var re *RemoteError
	if !errors.As(err, &re) || re.Code != CodeInternal {
		t.Fatalf("want RemoteError{internal}, got %v", err)
	}
}

func TestPackReaderRejectsUnexpectedFrame(t *testing.T) {
	var buf bytes.Buffer
	if err := WriteMsg(&buf, Msg{Type: TWants}); err != nil {
		t.Fatal(err)
	}
	_, err := io.ReadAll(NewPackReader(&buf))
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("want ErrProtocol, got %v", err)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `nix develop -c go test ./protocol/`
Expected: FAIL — undefined `SendPack`, `NewPackReader`.

- [ ] **Step 3: Write the implementation**

`protocol/pack.go`:

```go
package protocol

import (
	"fmt"
	"io"
	"iter"

	"github.com/fables-for-robots/amber-store-core/amberpack"
	"github.com/fables-for-robots/amber-store-core/fstree"
)

// SendPack serializes objs as one amberpack embedded in TData frames and
// terminates it with TDataEnd. An error from objs aborts the pack without
// the terminator and is returned; the caller decides how to tell the peer
// (typically a TErr frame, which NewPackReader surfaces as *RemoteError).
func SendPack(w io.Writer, objs iter.Seq2[fstree.Object, error]) error {
	cw := &chunkWriter{w: w}
	pw := amberpack.NewWriter(cw)
	for o, err := range objs {
		if err != nil {
			return err
		}
		if err := pw.Add(o); err != nil {
			return err
		}
	}
	if err := pw.Close(); err != nil {
		return err
	}
	return cw.finish()
}

// chunkWriter buffers pack bytes and emits them as ChunkSize TData frames.
type chunkWriter struct {
	w   io.Writer
	buf []byte
}

func (c *chunkWriter) Write(p []byte) (int, error) {
	n := len(p)
	for len(p) > 0 {
		take := min(ChunkSize-len(c.buf), len(p))
		c.buf = append(c.buf, p[:take]...)
		p = p[take:]
		if len(c.buf) == ChunkSize {
			if err := c.flush(); err != nil {
				return 0, err
			}
		}
	}
	return n, nil
}

func (c *chunkWriter) flush() error {
	if len(c.buf) == 0 {
		return nil
	}
	err := WriteMsg(c.w, Msg{Type: TData, Data: c.buf})
	c.buf = c.buf[:0]
	return err
}

func (c *chunkWriter) finish() error {
	if err := c.flush(); err != nil {
		return err
	}
	return WriteMsg(c.w, Msg{Type: TDataEnd})
}

// NewPackReader returns a reader over the pack bytes of a TData…TDataEnd
// frame sequence on r. It reads exactly through the TDataEnd frame, so the
// underlying stream is positioned for the next frame afterwards.
func NewPackReader(r io.Reader) io.Reader {
	return &packReader{r: r}
}

type packReader struct {
	r    io.Reader
	cur  []byte
	done bool
	err  error
}

func (p *packReader) Read(b []byte) (int, error) {
	for len(p.cur) == 0 {
		if p.err != nil {
			return 0, p.err
		}
		if p.done {
			return 0, io.EOF
		}
		m, err := ReadMsg(p.r)
		if err != nil {
			p.err = err
			return 0, err
		}
		switch m.Type {
		case TData:
			p.cur = m.Data
		case TDataEnd:
			p.done = true
		case TErr:
			p.err = RemoteFromMsg(m)
			return 0, p.err
		default:
			p.err = fmt.Errorf("%w: type %d during pack transfer", ErrProtocol, m.Type)
			return 0, p.err
		}
	}
	n := copy(b, p.cur)
	p.cur = p.cur[n:]
	return n, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `nix develop -c bash -c 'go mod tidy && go test ./protocol/'`
Expected: PASS.

- [ ] **Step 5: Commit**

```sh
nix develop -c gofmt -l . && nix develop -c go vet ./...
git add protocol/ go.mod go.sum
git commit -m "feat: chunked amberpack streaming inside protocol frames"
```

---

### Task 3: wantsync.Wants — frontier partitioning with completeness pruning

**Files:**
- Create: `wantsync/wantsync.go` (Wants + key helpers only; Receive/Send come in Task 4)
- Test: `wantsync/wantsync_test.go`

**Interfaces:**
- Consumes: amber-store-core `packstore`, `fstree`, `key`, `ingest`.
- Produces:
  - `func Wants(st *packstore.Store, frontier []key.Key, jobs int) ([]key.Key, error)` — dedupes frontier; a key is pruned only when present AND `fstree.CheckComplete` passes; present-but-incomplete keys are wanted (re-sent — cheap, and the receiver then re-walks their children).
  - `func encodeKeys(keys []key.Key) [][]byte`, `func decodeKeys(bs [][]byte) ([]key.Key, error)` (package-private, used by Task 4).

- [ ] **Step 1: Write the failing test**

`wantsync/wantsync_test.go`:

```go
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `nix develop -c go test ./wantsync/`
Expected: FAIL — undefined `Wants`, `encodeKeys`, `decodeKeys`.

- [ ] **Step 3: Write the implementation**

`wantsync/wantsync.go`:

```go
// Package wantsync implements both halves of the have/want object-transfer
// loop: the receiver announces which keys it is missing below a root, the
// sender answers each round with an amberpack of exactly those objects.
package wantsync

import (
	"errors"

	"github.com/fables-for-robots/amber-store-core/fstree"
	"github.com/fables-for-robots/amber-store-core/key"
	"github.com/fables-for-robots/amber-store-core/packstore"
)

// Wants partitions frontier into the keys that must be transferred. A key
// is pruned only when its object is present AND the whole subtree below it
// is complete — presence alone is not enough, because an interrupted
// transfer stores parents before their children. A present-but-incomplete
// key is re-requested whole; its redundant bytes are trivial and the
// receiver re-walks its children from the fresh copy.
func Wants(st *packstore.Store, frontier []key.Key, jobs int) ([]key.Key, error) {
	var wants []key.Key
	seen := make(map[key.Key]bool, len(frontier))
	for _, k := range frontier {
		if seen[k] {
			continue
		}
		seen[k] = true
		ok, err := st.Has(k)
		if err != nil {
			return nil, err
		}
		if !ok {
			wants = append(wants, k)
			continue
		}
		err = fstree.CheckComplete(k, st.Get, st.Has, jobs)
		switch {
		case err == nil: // complete subtree: prune
		case isMissing(err):
			wants = append(wants, k)
		default:
			return nil, err
		}
	}
	return wants, nil
}

// isMissing reports whether a CheckComplete failure means "object absent"
// (leaves surface as *fstree.MissingObjectError, interior nodes as the
// store's not-found error) rather than a real store fault.
func isMissing(err error) bool {
	var m *fstree.MissingObjectError
	return errors.As(err, &m) || errors.Is(err, packstore.ErrNotFound)
}

// encodeKeys flattens keys for a TWants frame.
func encodeKeys(keys []key.Key) [][]byte {
	out := make([][]byte, len(keys))
	for i, k := range keys {
		kk := k
		out[i] = kk[:]
	}
	return out
}

// decodeKeys parses and validates TWants keys.
func decodeKeys(bs [][]byte) ([]key.Key, error) {
	out := make([]key.Key, len(bs))
	for i, b := range bs {
		k, err := key.Parse(b)
		if err != nil {
			return nil, err
		}
		out[i] = k
	}
	return out, nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `nix develop -c go test ./wantsync/`
Expected: PASS (5 tests).

- [ ] **Step 5: Commit**

```sh
nix develop -c gofmt -l . && nix develop -c go vet ./...
git add wantsync/
git commit -m "feat: want-list computation with completeness-check pruning"
```

---

### Task 4: wantsync.Receive / wantsync.Send — the transfer loop

**Files:**
- Modify: `wantsync/wantsync.go` (append Receive, Send)
- Test: `wantsync/loop_test.go`

**Interfaces:**
- Consumes: Tasks 1-3.
- Produces:
  - `func Receive(rw io.ReadWriter, st *packstore.Store, root key.Key, jobs int) error` — sends TWants rounds (final round empty), ingests packs with `WriteParallel(..., Verify: true)`, expands the frontier via `fstree.ChildKeys`.
  - `func Send(rw io.ReadWriter, st *packstore.Store) error` — answers TWants rounds with `protocol.SendPack`; returns nil on an empty TWants; returns `*protocol.RemoteError` if the peer sends TErr; on a local read failure sends a TErr frame then returns the error.

- [ ] **Step 1: Write the failing test**

`wantsync/loop_test.go`:

```go
package wantsync

import (
	"errors"
	"io"
	"sync"
	"testing"

	"github.com/fables-for-robots/amber-store-core/fstree"
	"github.com/fables-for-robots/amber-store-core/key"
	"github.com/fables-for-robots/amber-store-core/packstore"
	"github.com/fables-for-robots/amber-store-iroh/protocol"
)

// duplex joins one side's reader with its writer.
type duplex struct {
	io.Reader
	io.Writer
}

// pipePair returns two connected in-memory duplex streams.
func pipePair() (duplex, duplex) {
	ar, aw := io.Pipe()
	br, bw := io.Pipe()
	return duplex{ar, bw}, duplex{br, aw}
}

// runLoop drives Send on src and Receive on dest concurrently.
func runLoop(t *testing.T, src, dest *packstore.Store, root key.Key) (sendErr, recvErr error) {
	t.Helper()
	a, b := pipePair()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sendErr = Send(a, src)
		// Unblock the peer if the sender bailed early.
		if c, ok := a.Writer.(io.Closer); ok && sendErr != nil {
			c.Close()
		}
	}()
	recvErr = Receive(b, dest, root, 0)
	wg.Wait()
	return sendErr, recvErr
}

func TestLoopSyncsIntoEmptyStore(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)
	sendErr, recvErr := runLoop(t, src, dest, root)
	if sendErr != nil || recvErr != nil {
		t.Fatalf("send=%v recv=%v", sendErr, recvErr)
	}
	if err := fstree.CheckComplete(root, dest.Get, dest.Has, 0); err != nil {
		t.Fatalf("dest incomplete after sync: %v", err)
	}
}

func TestLoopIsIdempotent(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)
	if se, re := runLoop(t, src, dest, root); se != nil || re != nil {
		t.Fatalf("first sync: send=%v recv=%v", se, re)
	}
	if se, re := runLoop(t, src, dest, root); se != nil || re != nil {
		t.Fatalf("second sync: send=%v recv=%v", se, re)
	}
	if err := fstree.CheckComplete(root, dest.Get, dest.Has, 0); err != nil {
		t.Fatal(err)
	}
}

// TestLoopResumesPartialTransfer plants only the root object in dest —
// the on-disk state an interrupted push leaves behind — and verifies the
// loop completes the tree.
func TestLoopResumesPartialTransfer(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)
	rootBytes, err := src.Get(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := dest.Put(root, rootBytes); err != nil {
		t.Fatal(err)
	}
	if se, re := runLoop(t, src, dest, root); se != nil || re != nil {
		t.Fatalf("send=%v recv=%v", se, re)
	}
	if err := fstree.CheckComplete(root, dest.Get, dest.Has, 0); err != nil {
		t.Fatalf("dest incomplete after resume: %v", err)
	}
}

// TestLoopSenderMissingObject syncs from a sender that lacks the tree:
// the sender must report a remote error and the receiver must fail, not
// hang or succeed.
func TestLoopSenderMissingObject(t *testing.T) {
	_, root := buildTree(t)
	emptySrc := openStore(t)
	dest := openStore(t)
	sendErr, recvErr := runLoop(t, emptySrc, dest, root)
	if !errors.Is(sendErr, packstore.ErrNotFound) {
		t.Fatalf("sender error: %v", sendErr)
	}
	var re *protocol.RemoteError
	if !errors.As(recvErr, &re) || re.Code != protocol.CodeInternal {
		t.Fatalf("receiver error: %v", recvErr)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `nix develop -c go test ./wantsync/`
Expected: FAIL — undefined `Receive`, `Send`.

- [ ] **Step 3: Write the implementation**

Append to `wantsync/wantsync.go` (add imports `fmt`, `io`, `github.com/fables-for-robots/amber-store-core/amberpack`, `github.com/fables-for-robots/amber-store-iroh/protocol`):

```go
// Receive runs the receiving half of the want loop over rw: rounds of
// TWants → amberpack until nothing below root is missing. The final,
// empty TWants tells the sender the loop is over. Received objects are
// verified against their keys before being stored — the peer is untrusted.
func Receive(rw io.ReadWriter, st *packstore.Store, root key.Key, jobs int) error {
	frontier := []key.Key{root}
	for {
		wants, err := Wants(st, frontier, jobs)
		if err != nil {
			return err
		}
		if err := protocol.WriteMsg(rw, protocol.Msg{Type: protocol.TWants, Keys: encodeKeys(wants)}); err != nil {
			return err
		}
		if len(wants) == 0 {
			return nil
		}
		var next []key.Key
		pr := amberpack.NewReader(protocol.NewPackReader(rw))
		seq := func(yield func(packstore.Object, error) bool) {
			for o, err := range pr.All() {
				if err != nil {
					yield(packstore.Object{}, err)
					return
				}
				kids, err := fstree.ChildKeys(o.Key, o.Bytes)
				if err != nil {
					yield(packstore.Object{}, err)
					return
				}
				next = append(next, kids...)
				if !yield(packstore.Object{Key: o.Key, Data: o.Bytes}, nil) {
					return
				}
			}
		}
		if _, err := st.WriteParallel(seq, packstore.WriteOpts{Verify: true}); err != nil {
			return err
		}
		frontier = next
	}
}

// Send runs the sending half: answer each TWants round with a pack of
// exactly the requested objects, until an empty TWants ends the loop. A
// local read failure is reported to the peer as a TErr frame and returned.
func Send(rw io.ReadWriter, st *packstore.Store) error {
	for {
		m, err := protocol.ReadMsg(rw)
		if err != nil {
			return err
		}
		switch m.Type {
		case protocol.TErr:
			return protocol.RemoteFromMsg(m)
		case protocol.TWants:
		default:
			return fmt.Errorf("%w: type %d, want TWants", protocol.ErrProtocol, m.Type)
		}
		if len(m.Keys) == 0 {
			return nil
		}
		keys, err := decodeKeys(m.Keys)
		if err != nil {
			return err
		}
		st.SortByLocation(keys)
		seq := func(yield func(fstree.Object, error) bool) {
			for _, k := range keys {
				data, err := st.Get(k)
				if err != nil {
					yield(fstree.Object{}, fmt.Errorf("object %s: %w", k, err))
					return
				}
				if !yield(fstree.Object{Key: k, Bytes: data}, nil) {
					return
				}
			}
		}
		if err := protocol.SendPack(rw, seq); err != nil {
			// Best effort: tell the peer why the pack stopped short.
			_ = protocol.WriteMsg(rw, protocol.Msg{Type: protocol.TErr, Code: protocol.CodeInternal, Text: err.Error()})
			return err
		}
	}
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `nix develop -c go test ./wantsync/`
Expected: PASS. If `TestLoopSenderMissingObject` hangs, the receiver is not surfacing the TErr from inside the pack — check `NewPackReader`'s TErr branch; do not add timeouts to paper over it.

- [ ] **Step 5: Commit**

```sh
nix develop -c gofmt -l . && nix develop -c go vet ./...
git add wantsync/
git commit -m "feat: want-loop transfer halves with verified writes"
```

---

### Task 5: server package — per-stream dispatch, CAS, per-name locks

**Files:**
- Create: `server/server.go`
- Test: `server/server_test.go`

**Interfaces:**
- Consumes: Tasks 1-4; core `refstore`, `reference`, `packstore`, `key`.
- Produces:
  - `func New(log *slog.Logger, objects *packstore.Store, refs *refstore.Store) *Server`
  - `func (s *Server) HandleStream(rw io.ReadWriteCloser)` — reads one request frame, dispatches, closes the stream. Errors are logged, never panicked.
  - (Task 6 adds `Serve`/`serveConn` to this file.)

- [ ] **Step 1: Write the failing test**

`server/server_test.go`:

```go
package server

import (
	"errors"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/fables-for-robots/amber-store-core/fstree"
	"github.com/fables-for-robots/amber-store-core/ingest"
	"github.com/fables-for-robots/amber-store-core/key"
	"github.com/fables-for-robots/amber-store-core/packstore"
	"github.com/fables-for-robots/amber-store-core/reference"
	"github.com/fables-for-robots/amber-store-core/refstore"
	"github.com/fables-for-robots/amber-store-iroh/protocol"
	"github.com/fables-for-robots/amber-store-iroh/wantsync"
)

func testServer(t *testing.T) *Server {
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
	t.Cleanup(func() { refs.Close(); objects.Close() })
	return New(slog.New(slog.NewTextHandler(os.Stderr, nil)), objects, refs)
}

func clientStore(t *testing.T) (*packstore.Store, key.Key) {
	t.Helper()
	src := t.TempDir()
	if err := os.WriteFile(filepath.Join(src, "f.txt"), []byte("hello p2p"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(src, "d"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(src, "d", "g.txt"), []byte("nested"), 0o644); err != nil {
		t.Fatal(err)
	}
	st, err := packstore.Open(filepath.Join(t.TempDir(), "packstore"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { st.Close() })
	root, _, err := ingest.Dir(st, src, ingest.Opts{})
	if err != nil {
		t.Fatal(err)
	}
	return st, root
}

// doPush runs one full client-side push against srv over net.Pipe and
// returns the final frame (TOK or TErr) or the client-side error.
func doPush(t *testing.T, srv *Server, st *packstore.Store, name string, root key.Key, cas bool, expectedOld []byte) (protocol.Msg, error) {
	t.Helper()
	c, s := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); srv.HandleStream(s) }()
	defer func() { c.Close(); <-done }()
	req := protocol.Msg{Type: protocol.TPush, Name: name, Root: root[:], CAS: cas, ExpectedOld: expectedOld}
	if err := protocol.WriteMsg(c, req); err != nil {
		return protocol.Msg{}, err
	}
	if err := wantsync.Send(c, st); err != nil {
		return protocol.Msg{}, err
	}
	return protocol.ReadMsg(c)
}

func TestPushCreatesRefAndTransfersObjects(t *testing.T) {
	srv := testServer(t)
	st, root := clientStore(t)
	m, err := doPush(t, srv, st, "backups/home", root, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != protocol.TOK {
		t.Fatalf("want TOK, got %+v", m)
	}
	if err := fstree.CheckComplete(root, srv.objects.Get, srv.objects.Has, 0); err != nil {
		t.Fatalf("server store incomplete: %v", err)
	}
	raw, err := srv.refs.Get("backups/home")
	if err != nil {
		t.Fatal(err)
	}
	rec, err := reference.Decode(raw)
	if err != nil {
		t.Fatal(err)
	}
	k, err := key.Parse(rec.Key)
	if err != nil || k != root {
		t.Fatalf("ref points at %v (err %v), want %v", k, err, root)
	}
}

func TestPushCASMismatchOnExistingRef(t *testing.T) {
	srv := testServer(t)
	st, root := clientStore(t)
	if m, err := doPush(t, srv, st, "r", root, true, nil); err != nil || m.Type != protocol.TOK {
		t.Fatalf("first push: %+v %v", m, err)
	}
	// Second create-push (ExpectedOld nil) must be rejected before any
	// transfer: wantsync.Send surfaces the TErr as *RemoteError.
	_, err := doPush(t, srv, st, "r", root, true, nil)
	var re *protocol.RemoteError
	if !errors.As(err, &re) || re.Code != protocol.CodeCASMismatch {
		t.Fatalf("want cas-mismatch, got %v", err)
	}
	if !errorsIsKey(re.Current, root) {
		t.Fatalf("Current = %x, want the committed root", re.Current)
	}
}

func errorsIsKey(b []byte, k key.Key) bool {
	kk, err := key.Parse(b)
	return err == nil && kk == k
}

func TestPushCASMatchUpdatesRef(t *testing.T) {
	srv := testServer(t)
	st, root := clientStore(t)
	if m, err := doPush(t, srv, st, "r", root, true, nil); err != nil || m.Type != protocol.TOK {
		t.Fatalf("first push: %+v %v", m, err)
	}
	// Same root again, but with the correct expected-old: allowed.
	if m, err := doPush(t, srv, st, "r", root, true, root[:]); err != nil || m.Type != protocol.TOK {
		t.Fatalf("cas update push: %+v %v", m, err)
	}
	// Forced push (no CAS) is always allowed.
	if m, err := doPush(t, srv, st, "r", root, false, nil); err != nil || m.Type != protocol.TOK {
		t.Fatalf("forced push: %+v %v", m, err)
	}
}

func TestPushRejectsBadName(t *testing.T) {
	srv := testServer(t)
	st, root := clientStore(t)
	_, err := doPush(t, srv, st, "bad@name", root, true, nil)
	var re *protocol.RemoteError
	if !errors.As(err, &re) || re.Code != protocol.CodeBadRequest {
		t.Fatalf("want bad-request, got %v", err)
	}
}

func TestPullUnknownRef(t *testing.T) {
	srv := testServer(t)
	c, s := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); srv.HandleStream(s) }()
	defer func() { c.Close(); <-done }()
	if err := protocol.WriteMsg(c, protocol.Msg{Type: protocol.TPull, Name: "nope"}); err != nil {
		t.Fatal(err)
	}
	m, err := protocol.ReadMsg(c)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != protocol.TErr || m.Code != protocol.CodeUnknownRef {
		t.Fatalf("want unknown-ref, got %+v", m)
	}
}

func TestPullTransfersTree(t *testing.T) {
	srv := testServer(t)
	st, root := clientStore(t)
	if m, err := doPush(t, srv, st, "r", root, true, nil); err != nil || m.Type != protocol.TOK {
		t.Fatalf("push: %+v %v", m, err)
	}
	dest, err := packstore.Open(filepath.Join(t.TempDir(), "packstore"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { dest.Close() })

	c, s := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); srv.HandleStream(s) }()
	defer func() { c.Close(); <-done }()
	if err := protocol.WriteMsg(c, protocol.Msg{Type: protocol.TPull, Name: "r"}); err != nil {
		t.Fatal(err)
	}
	m, err := protocol.ReadMsg(c)
	if err != nil || m.Type != protocol.TRef {
		t.Fatalf("want TRef, got %+v %v", m, err)
	}
	rec, err := reference.Decode(m.Record)
	if err != nil {
		t.Fatal(err)
	}
	k, err := key.Parse(rec.Key)
	if err != nil {
		t.Fatal(err)
	}
	if err := wantsync.Receive(c, dest, k, 0); err != nil {
		t.Fatal(err)
	}
	if err := fstree.CheckComplete(k, dest.Get, dest.Has, 0); err != nil {
		t.Fatalf("pulled tree incomplete: %v", err)
	}
}

func TestRefList(t *testing.T) {
	srv := testServer(t)
	st, root := clientStore(t)
	if m, err := doPush(t, srv, st, "r1", root, true, nil); err != nil || m.Type != protocol.TOK {
		t.Fatalf("push: %+v %v", m, err)
	}
	c, s := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); srv.HandleStream(s) }()
	defer func() { c.Close(); <-done }()
	if err := protocol.WriteMsg(c, protocol.Msg{Type: protocol.TRefList}); err != nil {
		t.Fatal(err)
	}
	m, err := protocol.ReadMsg(c)
	if err != nil || m.Type != protocol.TRefs {
		t.Fatalf("want TRefs, got %+v %v", m, err)
	}
	if len(m.Refs) != 1 || m.Refs[0].Name != "r1" || m.Refs[0].CreatedAt == 0 {
		t.Fatalf("refs: %+v", m.Refs)
	}
	if time.Since(time.Unix(0, m.Refs[0].CreatedAt)) > time.Minute {
		t.Fatalf("CreatedAt implausible: %d", m.Refs[0].CreatedAt)
	}
}

func TestUnknownOperation(t *testing.T) {
	srv := testServer(t)
	c, s := net.Pipe()
	done := make(chan struct{})
	go func() { defer close(done); srv.HandleStream(s) }()
	defer func() { c.Close(); <-done }()
	if err := protocol.WriteMsg(c, protocol.Msg{Type: 99}); err != nil {
		t.Fatal(err)
	}
	m, err := protocol.ReadMsg(c)
	if err != nil {
		t.Fatal(err)
	}
	if m.Type != protocol.TErr || m.Code != protocol.CodeBadRequest {
		t.Fatalf("want bad-request, got %+v", m)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `nix develop -c go test ./server/`
Expected: FAIL — undefined `New`, `Server`, `HandleStream`.

- [ ] **Step 3: Write the implementation**

`server/server.go`:

```go
// Package server implements the amber-store-iroh server: it owns a store
// directory and answers push/pull/ref-list operations, one per stream.
package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/fables-for-robots/amber-store-core/key"
	"github.com/fables-for-robots/amber-store-core/packstore"
	"github.com/fables-for-robots/amber-store-core/reference"
	"github.com/fables-for-robots/amber-store-core/refstore"
	"github.com/fables-for-robots/amber-store-iroh/protocol"
	"github.com/fables-for-robots/amber-store-iroh/wantsync"
)

// Server answers amber-store-iroh operations against a single store.
// Access is open by design: any peer that can connect may push and pull.
type Server struct {
	log     *slog.Logger
	objects *packstore.Store
	refs    *refstore.Store
	jobs    int // completeness-walk parallelism; 0 = GOMAXPROCS

	mu       sync.Mutex
	refLocks map[string]*sync.Mutex
}

// New wires a Server over an open packstore and refstore. The caller keeps
// ownership of both and closes them after the server stops.
func New(log *slog.Logger, objects *packstore.Store, refs *refstore.Store) *Server {
	return &Server{log: log, objects: objects, refs: refs, refLocks: map[string]*sync.Mutex{}}
}

// lockRef serializes ref commits per name so compare-and-swap is
// race-free under concurrent pushes. Entries are never removed; the map
// is bounded by the number of distinct ref names ever pushed.
func (s *Server) lockRef(name string) (unlock func()) {
	s.mu.Lock()
	l := s.refLocks[name]
	if l == nil {
		l = &sync.Mutex{}
		s.refLocks[name] = l
	}
	s.mu.Unlock()
	l.Lock()
	return l.Unlock
}

// HandleStream serves one operation on one stream and closes it. The
// stream is FIN-closed after the final response frame; the connection
// stays up for further streams.
func (s *Server) HandleStream(rw io.ReadWriteCloser) {
	defer rw.Close()
	m, err := protocol.ReadMsg(rw)
	if err != nil {
		s.log.Error("read request", "error", err)
		return
	}
	switch m.Type {
	case protocol.TRefList:
		err = s.handleRefList(rw)
	case protocol.TPush:
		err = s.handlePush(rw, m)
	case protocol.TPull:
		err = s.handlePull(rw, m)
	default:
		err = s.fail(rw, protocol.CodeBadRequest, fmt.Errorf("unknown operation %d", m.Type))
	}
	if err != nil {
		s.log.Error("operation failed", "op", m.Type, "name", m.Name, "error", err)
	}
}

// fail sends a TErr frame (best effort) and returns err for logging.
func (s *Server) fail(w io.Writer, code string, err error) error {
	_ = protocol.WriteMsg(w, protocol.Msg{Type: protocol.TErr, Code: code, Text: err.Error()})
	return err
}

func (s *Server) handleRefList(rw io.ReadWriter) error {
	records, err := s.refs.All()
	if err != nil {
		return s.fail(rw, protocol.CodeInternal, err)
	}
	infos := make([]protocol.RefInfo, 0, len(records))
	for _, r := range records {
		rec, err := reference.Decode(r.Data)
		if err != nil {
			return s.fail(rw, protocol.CodeInternal, fmt.Errorf("reference %q: %w", r.Name, err))
		}
		infos = append(infos, protocol.RefInfo{Name: r.Name, Key: rec.Key, CreatedAt: rec.CreatedAt, User: rec.User})
	}
	return protocol.WriteMsg(rw, protocol.Msg{Type: protocol.TRefs, Refs: infos})
}

func (s *Server) handlePush(rw io.ReadWriter, m protocol.Msg) error {
	if err := reference.ValidateName(m.Name); err != nil {
		return s.fail(rw, protocol.CodeBadRequest, err)
	}
	root, err := key.Parse(m.Root)
	if err != nil {
		return s.fail(rw, protocol.CodeBadRequest, fmt.Errorf("root key: %w", err))
	}
	// Early precondition check: reject before any transfer happens. The
	// authoritative re-check happens under the per-name lock at commit.
	if m.CAS {
		if err := s.checkCAS(rw, m); err != nil {
			return err
		}
	}
	if err := wantsync.Receive(rw, s.objects, root, s.jobs); err != nil {
		return err
	}
	unlock := s.lockRef(m.Name)
	defer unlock()
	if m.CAS {
		if err := s.checkCAS(rw, m); err != nil {
			return err
		}
	}
	rec := reference.Reference{Name: m.Name, Key: root[:], CreatedAt: time.Now().UnixNano()}
	raw, err := rec.Encode()
	if err != nil {
		return s.fail(rw, protocol.CodeInternal, err)
	}
	if err := s.refs.Put(m.Name, raw); err != nil {
		return s.fail(rw, protocol.CodeInternal, err)
	}
	return protocol.WriteMsg(rw, protocol.Msg{Type: protocol.TOK, Key: root[:]})
}

// checkCAS verifies the push precondition: ExpectedOld must equal the
// ref's current key (nil meaning "ref must not exist"). On mismatch it
// sends the cas-mismatch frame carrying the current key and returns an
// error that ends the operation.
func (s *Server) checkCAS(rw io.ReadWriter, m protocol.Msg) error {
	var current []byte
	raw, err := s.refs.Get(m.Name)
	switch {
	case err == nil:
		rec, decErr := reference.Decode(raw)
		if decErr != nil {
			return s.fail(rw, protocol.CodeInternal, decErr)
		}
		current = rec.Key
	case errors.Is(err, refstore.ErrNotFound):
		// current stays nil: the ref does not exist.
	default:
		return s.fail(rw, protocol.CodeInternal, err)
	}
	if bytes.Equal(current, m.ExpectedOld) {
		return nil
	}
	_ = protocol.WriteMsg(rw, protocol.Msg{
		Type:    protocol.TErr,
		Code:    protocol.CodeCASMismatch,
		Text:    fmt.Sprintf("remote ref %q changed", m.Name),
		Current: current,
	})
	return fmt.Errorf("cas mismatch on %q", m.Name)
}

func (s *Server) handlePull(rw io.ReadWriter, m protocol.Msg) error {
	raw, err := s.refs.Get(m.Name)
	if errors.Is(err, refstore.ErrNotFound) {
		return s.fail(rw, protocol.CodeUnknownRef, fmt.Errorf("ref %q not found", m.Name))
	}
	if err != nil {
		return s.fail(rw, protocol.CodeInternal, err)
	}
	if err := protocol.WriteMsg(rw, protocol.Msg{Type: protocol.TRef, Record: raw}); err != nil {
		return err
	}
	return wantsync.Send(rw, s.objects)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `nix develop -c go test ./server/`
Expected: PASS (8 tests).

- [ ] **Step 5: Commit**

```sh
nix develop -c gofmt -l . && nix develop -c go vet ./...
git add server/
git commit -m "feat: server stream dispatch with CAS ref commits"
```

---

### Task 6: Serve loop + cmd/amber-serve binary

**Files:**
- Modify: `server/server.go` (append Serve, serveConn)
- Create: `cmd/amber-serve/main.go`
- Modify: `.gitignore` (add `server.key`)

**Interfaces:**
- Consumes: Tasks 1-5; go-iroh `iroh`, `dns`, `key`, `relay`.
- Produces:
  - `func (s *Server) Serve(ctx context.Context, ep *iroh.Endpoint, grace time.Duration) error` — accept loop; returns after ctx cancel once in-flight handlers finish or grace elapses. Task 9's e2e test calls this.
  - `amber-serve` binary: flags `--store` (env `AMBER_STORE`), `--key` (default `server.key`).

- [ ] **Step 1: Append the accept loop to `server/server.go`**

Add imports `context` and `github.com/tmc/go-iroh/iroh`:

```go
// Serve accepts connections on ep until ctx is canceled, dispatching every
// stream to HandleStream. After cancel it waits up to grace for in-flight
// handlers before returning.
func (s *Server) Serve(ctx context.Context, ep *iroh.Endpoint, grace time.Duration) error {
	var wg sync.WaitGroup
	for ctx.Err() == nil {
		conn, err := ep.Accept(ctx)
		if err != nil {
			if ctx.Err() != nil {
				break
			}
			s.log.Error("accept", "error", err)
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.serveConn(ctx, conn)
		}()
	}
	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(grace):
		s.log.Warn("shutdown grace elapsed with handlers in flight")
	}
	return nil
}

// serveConn accepts streams on one connection until the peer closes it or
// ctx is canceled. Closing the QUIC connection discards undelivered
// stream data, so the connection is only closed once the peer goes away —
// except on ctx cancel, where AfterFunc closes it to unblock handlers.
func (s *Server) serveConn(ctx context.Context, conn *iroh.Conn) {
	defer conn.Close()
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	log := s.log.With("remote", conn.RemoteID())
	log.Info("connection")
	var wg sync.WaitGroup
	defer wg.Wait()
	for {
		stream, err := conn.AcceptStreamConn(ctx)
		if err != nil {
			return // peer closed the connection, or ctx canceled
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			s.HandleStream(stream)
		}()
	}
}
```

- [ ] **Step 2: Write `cmd/amber-serve/main.go`**

Modeled on irohese's server main (`/Users/dragan/fables-for-robots/irohese/cmd/server/main.go`), minus the actor system, plus store opening:

```go
// Command amber-serve hosts an amber store over iroh QUIC. Access is
// open: any peer that knows the endpoint ID may push and pull.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/fables-for-robots/amber-store-core/packstore"
	"github.com/fables-for-robots/amber-store-core/refstore"
	"github.com/fables-for-robots/amber-store-iroh/protocol"
	"github.com/fables-for-robots/amber-store-iroh/server"
	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/iroh"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/relay"
	"github.com/urfave/cli/v2"
)

const shutdownGrace = 10 * time.Second

// loadOrCreateSecretKey reads the hex-encoded secret key from path,
// generating and persisting a fresh one on first run. Deleting the file
// changes the server's endpoint ID.
func loadOrCreateSecretKey(path string) (irohkey.SecretKey, error) {
	b, err := os.ReadFile(path)
	if err == nil {
		sk, err := irohkey.ParseSecretKey(strings.TrimSpace(string(b)))
		if err != nil {
			return irohkey.SecretKey{}, fmt.Errorf("parse key file %s: %w", path, err)
		}
		return sk, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return irohkey.SecretKey{}, fmt.Errorf("read key file: %w", err)
	}
	sk, err := irohkey.GenerateSecretKey()
	if err != nil {
		return irohkey.SecretKey{}, err
	}
	seed := sk.Bytes()
	if err := os.WriteFile(path, []byte(hex.EncodeToString(seed[:])+"\n"), 0o600); err != nil {
		return irohkey.SecretKey{}, fmt.Errorf("write key file: %w", err)
	}
	return sk, nil
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	app := &cli.App{
		Name:  "amber-serve",
		Usage: "host an amber store over iroh QUIC",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "store",
				Usage:   "store directory (layout: <dir>/packstore, <dir>/refs)",
				EnvVars: []string{"AMBER_STORE"},
			},
			&cli.StringFlag{
				Name:  "key",
				Value: "server.key",
				Usage: "path to the secret key file (generated on first run)",
			},
		},
		Action: func(c *cli.Context) error {
			dir := c.String("store")
			if dir == "" {
				return fmt.Errorf("no store directory: set --store or $AMBER_STORE")
			}
			objects, err := packstore.Open(filepath.Join(dir, "packstore"), packstore.WithSync(true))
			if err != nil {
				return err
			}
			defer objects.Close()
			refs, err := refstore.Open(filepath.Join(dir, "refs"), true)
			if err != nil {
				return err
			}
			defer refs.Close()

			sk, err := loadOrCreateSecretKey(c.String("key"))
			if err != nil {
				return fmt.Errorf("load secret key: %w", err)
			}

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			ep, err := iroh.Bind(
				ctx,
				iroh.WithSecretKey(sk),
				iroh.WithALPNs(protocol.ALPN),
				iroh.WithRelayMode(relay.ModeDefault()),
			)
			if err != nil {
				return fmt.Errorf("bind: %w", err)
			}
			defer ep.Shutdown(context.Background())

			if err := ep.Online(ctx); err != nil {
				return fmt.Errorf("connect to relay: %w", err)
			}

			// Publish the relay address so clients can resolve the
			// endpoint ID over the internet; re-published in the
			// background every 5 minutes.
			pub, err := iroh.N0PkarrPublisher(sk, nil)
			if err != nil {
				return fmt.Errorf("pkarr publisher: %w", err)
			}
			defer pub.Close()
			pub.Publish(dns.NewEndpointData(ep.Addr().Addrs()...))

			log.Info("server started", "id", ep.ID())
			log.Info("server listening", "addr", ep.Addr())

			srv := server.New(log, objects, refs)
			err = srv.Serve(ctx, ep, shutdownGrace)
			log.Info("server stopped")
			return err
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Error("run", "error", err)
		os.Exit(1)
	}
}
```

- [ ] **Step 3: Add `server.key` to `.gitignore`**

Append to `.gitignore`:

```
server.key
```

- [ ] **Step 4: Verify build and existing tests**

Run: `nix develop -c bash -c 'go mod tidy && go build ./... && go test ./...'`
Expected: builds clean; protocol/wantsync/server tests PASS. (The Serve loop over real iroh endpoints is exercised by Task 9's e2e.)

- [ ] **Step 5: Commit**

```sh
nix develop -c gofmt -l . && nix develop -c go vet ./...
git add server/ cmd/amber-serve/ .gitignore go.mod go.sum
git commit -m "feat: amber-serve binary with iroh accept loop"
```

---

### Task 7: cmd/amber — local store commands

**Files:**
- Create: `cmd/amber/main.go` (new — command table below)
- Create by copying **verbatim** from `/Users/dragan/fables-for-robots/amber-store-core/cmd/amber-store/` (same `package main`, no changes except noted):
  - `store.go` → `cmd/amber/store.go` (verbatim)
  - `spec.go` → `cmd/amber/spec.go` (verbatim)
  - `ls.go` → `cmd/amber/ls.go` (verbatim)
  - `export.go` → `cmd/amber/export.go` (verbatim)
  - `restore.go` → `cmd/amber/restore.go` (verbatim)
  - `progress.go` → `cmd/amber/progress.go` (verbatim)
  - `progress_test.go` → `cmd/amber/progress_test.go` (verbatim)
  - `ingest.go` → `cmd/amber/import.go` (edits below)
  - `ref.go` → `cmd/amber/ref.go` (edits below)
- Create: `cmd/amber/chunk.go` (chunkConfig/chunkFlags moved out of core's main.go)
- Test: `cmd/amber/local_test.go`

**Interfaces:**
- Consumes: amber-store-core packages; helpers copied above (`openStore`, `closeStore`, `resolveSpec`, `parseHexKey`).
- Produces (Task 8 and 9 rely on):
  - `func newApp() *cli.App` in `cmd/amber/main.go` — commands `import`, `ls`, `export`, `restore`, `ref`, plus (Task 8) `push`, `pull`, `refs`.
  - `const trackingPrefix = "remotes/"` in `cmd/amber/ref.go`.

- [ ] **Step 1: Copy the verbatim files**

```sh
cd /Users/dragan/fables-for-robots/amber-store-iroh
mkdir -p cmd/amber
for f in store.go spec.go ls.go export.go restore.go progress.go progress_test.go; do
  cp /Users/dragan/fables-for-robots/amber-store-core/cmd/amber-store/$f cmd/amber/$f
done
cp /Users/dragan/fables-for-robots/amber-store-core/cmd/amber-store/ingest.go cmd/amber/import.go
cp /Users/dragan/fables-for-robots/amber-store-core/cmd/amber-store/ref.go cmd/amber/ref.go
```

- [ ] **Step 2: Write `cmd/amber/main.go`**

```go
// Command amber is the amber-store-iroh client: a full local
// content-addressed store plus push/pull/refs against an amber-serve
// server reached over iroh QUIC.
package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	if err := newApp().Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "amber: %v\n", err)
		os.Exit(1)
	}
}

// newApp creates the CLI application. Local commands operate on the store
// directory named by --store / $AMBER_STORE; network commands also take
// --server.
func newApp() *cli.App {
	return &cli.App{
		Name:  "amber",
		Usage: "p2p distributed content-addressed filesystem tree store",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "store",
				Usage:   "store directory (layout: <dir>/packstore, <dir>/refs)",
				EnvVars: []string{"AMBER_STORE"},
			},
		},
		Commands: []*cli.Command{
			importCommand(),
			lsCommand(),
			exportCommand(),
			restoreCommand(),
			refCommand(),
			// Task 8 appends: pushCommand(), pullCommand(), refsCommand()
		},
	}
}
```

- [ ] **Step 3: Write `cmd/amber/chunk.go`**

Copy the `chunkConfig` type, its `chunkOpts()` method, and the `chunkFlags(...)` function **verbatim** from `/Users/dragan/fables-for-robots/amber-store-core/cmd/amber-store/main.go` (they are everything in that file below `newApp`; keep their imports: `fmt`, `github.com/fables-for-robots/amber-store-core/chunkers`, `github.com/fables-for-robots/amber-store-core/ingest`, `github.com/urfave/cli/v2`) into a new file starting with:

```go
package main
```

- [ ] **Step 4: Edit `cmd/amber/import.go`**

Three changes to the copied `ingest.go`:
1. Rename `ingestConfig` → keep as is; rename `ingestCommand` → `importCommand` and `runIngest` → `runImport` (update the internal call).
2. In the `cli.Command` literal change `Name: "ingest"` → `Name: "import"` and the usage string to `"import PATH (a directory or a single file) into the local store and print the root key"`.
3. In `runImport`, reject the tracking namespace: after the existing `reference.ValidateName(cfg.ref)` check, add:

```go
		if strings.HasPrefix(cfg.ref, trackingPrefix) {
			return fmt.Errorf("ref %q: the %q namespace is reserved for remote-tracking refs", cfg.ref, trackingPrefix)
		}
```

(add `strings` to imports). Also update the retry hint string `amber-store ref set` → `amber ref set`.

- [ ] **Step 5: Edit `cmd/amber/ref.go`**

Three changes to the copied `ref.go`:
1. Add the namespace constant at the top (after imports):

```go
// trackingPrefix is the reserved local-refstore namespace that records the
// last-seen server-side value of each ref, keyed by server endpoint ID:
// remotes/<endpoint-id>/<name>. Hidden from listings; ref set refuses it.
const trackingPrefix = "remotes/"
```

2. In `runRefList`, skip tracking refs — after `for _, r := range records {` insert:

```go
		if strings.HasPrefix(r.Name, trackingPrefix) {
			continue
		}
```

(add `strings` to imports).
3. In `runRefSet`, refuse the namespace — after `name := c.Args().Get(0)` insert:

```go
	if strings.HasPrefix(name, trackingPrefix) {
		return fmt.Errorf("ref %q: the %q namespace is reserved for remote-tracking refs", name, trackingPrefix)
	}
```

(`ref get` and `ref rm` deliberately keep working on tracking refs — `rm` is how you reset tracking state.)

- [ ] **Step 6: Write the failing test**

`cmd/amber/local_test.go`:

```go
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
```

- [ ] **Step 7: Run tests**

Run: `nix develop -c bash -c 'go mod tidy && go build ./... && go test ./cmd/amber/'`
Expected: PASS. (`TestRefListHidesTrackingNamespace` exercises the hiding logic only via the refused `ref set` — full tracking-ref hiding is re-verified through push in Task 9.) If copied files fail to compile, compare imports against the originals rather than editing logic.

- [ ] **Step 8: Commit**

```sh
nix develop -c gofmt -l . && nix develop -c go vet ./...
git add cmd/amber/ go.mod go.sum
git commit -m "feat: amber client local commands (import/ls/export/restore/ref)"
```

---

### Task 8: cmd/amber — network commands (push, pull, refs)

**Files:**
- Create: `cmd/amber/dial.go`
- Create: `cmd/amber/push.go`
- Create: `cmd/amber/pull.go`
- Create: `cmd/amber/refs.go`
- Modify: `cmd/amber/main.go` (register the three commands)

**Interfaces:**
- Consumes: Tasks 1-7; go-iroh.
- Produces:
  - `func dialServer(ctx context.Context, serverID string, directAddrs []string) (*iroh.Conn, func(), error)` — resolves and connects; with directAddrs it skips discovery and relays entirely (offline/e2e path). The returned func closes conn and endpoint.
  - `func serverFlags(server *string, addrs *cli.StringSlice) []cli.Flag` — shared `--server` (required) and `--addr` (repeatable, optional) flags.
  - `func trackingRef(serverID string, name string) string` — `"remotes/" + serverID + "/" + name`.
  - Commands `push NAME [--force]`, `pull NAME`, `refs`.

- [ ] **Step 1: Write `cmd/amber/dial.go`**

```go
package main

import (
	"context"
	"fmt"
	"net/netip"

	"github.com/fables-for-robots/amber-store-iroh/protocol"
	"github.com/tmc/go-iroh/iroh"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
	"github.com/urfave/cli/v2"
)

// serverFlags returns the flags shared by every network command.
func serverFlags(server *string, addrs *cli.StringSlice) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "server",
			Usage:       "endpoint ID of the server (printed in its startup log)",
			Required:    true,
			Destination: server,
		},
		&cli.StringSliceFlag{
			Name:        "addr",
			Usage:       "direct server address host:port (repeatable; skips discovery and relays)",
			Destination: addrs,
		},
	}
}

// trackingRef is the local refstore name recording the last-seen
// server-side value of ref name on the given server.
func trackingRef(serverID string, name string) string {
	return trackingPrefix + serverID + "/" + name
}

// dialServer connects to the server with an ephemeral client identity
// (access is open, so no stable key is needed). With direct addresses it
// dials straight at them — no discovery, no relays — which is also how
// the offline end-to-end tests connect. Without them it resolves the
// endpoint ID via pkarr and DNS like the irohese client.
func dialServer(ctx context.Context, serverID string, directAddrs []string) (*iroh.Conn, func(), error) {
	id, err := irohkey.ParseEndpointID(serverID)
	if err != nil {
		return nil, nil, fmt.Errorf("parse endpoint id: %w", err)
	}

	if len(directAddrs) > 0 {
		ep, err := iroh.Bind(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("bind: %w", err)
		}
		addr := netaddr.NewEndpointAddr(id)
		for _, s := range directAddrs {
			ap, err := netip.ParseAddrPort(s)
			if err != nil {
				ep.Shutdown(ctx)
				return nil, nil, fmt.Errorf("parse --addr %q: %w", s, err)
			}
			addr = addr.WithIP(ap)
		}
		conn, err := ep.Connect(ctx, addr, protocol.ALPN)
		if err != nil {
			ep.Shutdown(ctx)
			return nil, nil, fmt.Errorf("connect: %w", err)
		}
		return conn, func() { conn.Close(); ep.Shutdown(context.Background()) }, nil
	}

	pkarrResolver, err := iroh.N0PkarrResolver(nil)
	if err != nil {
		return nil, nil, fmt.Errorf("pkarr resolver: %w", err)
	}
	var services iroh.AddressLookupServices
	services.AddResolver(pkarrResolver)
	services.AddResolver(iroh.N0DNSAddressLookup(nil))

	ep, err := iroh.Bind(
		ctx,
		iroh.WithAddressLookup(&services),
		iroh.WithRelayMode(relay.ModeDefault()),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("bind: %w", err)
	}

	// Connect does no discovery on its own: it only dials addresses
	// already present in the EndpointAddr, so resolve first.
	addr := netaddr.NewEndpointAddr(id)
	resolved := false
	for item, err := range services.Resolve(ctx, id) {
		if err != nil {
			continue
		}
		addr = item.Addr()
		resolved = true
		break
	}
	if !resolved {
		ep.Shutdown(ctx)
		return nil, nil, fmt.Errorf("no address found for endpoint %s", id)
	}

	conn, err := ep.Connect(ctx, addr, protocol.ALPN)
	if err != nil {
		ep.Shutdown(ctx)
		return nil, nil, fmt.Errorf("connect: %w", err)
	}
	return conn, func() { conn.Close(); ep.Shutdown(context.Background()) }, nil
}
```

- [ ] **Step 2: Write `cmd/amber/push.go`**

```go
package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/fables-for-robots/amber-store-core/key"
	"github.com/fables-for-robots/amber-store-core/reference"
	"github.com/fables-for-robots/amber-store-core/refstore"
	"github.com/fables-for-robots/amber-store-iroh/protocol"
	"github.com/fables-for-robots/amber-store-iroh/wantsync"
	"github.com/urfave/cli/v2"
)

func pushCommand() *cli.Command {
	var (
		server string
		addrs  cli.StringSlice
		force  bool
	)
	flags := append(serverFlags(&server, &addrs),
		&cli.BoolFlag{
			Name:        "force",
			Usage:       "overwrite the remote ref even if it changed since the last pull/push",
			Destination: &force,
		},
	)
	return &cli.Command{
		Name:      "push",
		Usage:     "push local ref NAME (and every missing object below it) to the server",
		ArgsUsage: "NAME",
		Flags:     flags,
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("push requires exactly one NAME argument, got %d", c.NArg())
			}
			return runPush(c, server, addrs.Value(), force, c.Args().First())
		},
	}
}

func runPush(c *cli.Context, server string, addrs []string, force bool, name string) error {
	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	defer closeStore(objects, refs)

	root, _, err := resolveSpec(refs, "ref:"+name)
	if err != nil {
		return err
	}

	// The tracking ref records what this store last saw on the server;
	// it is the compare-and-swap expectation. Absent tracking ref means
	// "the server must not have this ref yet".
	var expectedOld []byte
	tname := trackingRef(server, name)
	if raw, err := refs.Get(tname); err == nil {
		rec, err := reference.Decode(raw)
		if err != nil {
			return fmt.Errorf("tracking ref %q: %w", tname, err)
		}
		expectedOld = rec.Key
	} else if !errors.Is(err, refstore.ErrNotFound) {
		return err
	}

	conn, closeConn, err := dialServer(c.Context, server, addrs)
	if err != nil {
		return err
	}
	defer closeConn()
	stream, err := conn.OpenStreamConn(c.Context)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	// A QUIC stream is invisible to the server until data flows, so the
	// request goes out before any read.
	req := protocol.Msg{Type: protocol.TPush, Name: name, Root: root[:], CAS: !force, ExpectedOld: expectedOld}
	if err := protocol.WriteMsg(stream, req); err != nil {
		return err
	}
	if err := wantsync.Send(stream, objects); err != nil {
		return pushError(name, err)
	}
	m, err := protocol.ReadMsg(stream)
	if err != nil {
		return err
	}
	switch m.Type {
	case protocol.TOK:
	case protocol.TErr:
		return pushError(name, protocol.RemoteFromMsg(m))
	default:
		return fmt.Errorf("%w: type %d, want TOK", protocol.ErrProtocol, m.Type)
	}
	if err := stream.Close(); err != nil {
		return err
	}

	// Record the new server-side value for the next push's CAS check.
	trec := reference.Reference{Name: tname, Key: root[:], CreatedAt: time.Now().UnixNano()}
	raw, err := trec.Encode()
	if err != nil {
		return err
	}
	if err := refs.Put(tname, raw); err != nil {
		return fmt.Errorf("push succeeded but recording tracking ref failed: %w", err)
	}
	fmt.Fprintf(c.App.Writer, "%s -> %s\n", name, root)
	return nil
}

// pushError rewrites a cas-mismatch into an actionable message.
func pushError(name string, err error) error {
	var re *protocol.RemoteError
	if !errors.As(err, &re) || re.Code != protocol.CodeCASMismatch {
		return err
	}
	current := "absent"
	if len(re.Current) > 0 {
		if k, kerr := key.Parse(re.Current); kerr == nil {
			current = k.String()
		} else {
			current = hex.EncodeToString(re.Current)
		}
	}
	return fmt.Errorf("remote ref %q changed (now %s): pull first, or push --force", name, current)
}
```

- [ ] **Step 3: Write `cmd/amber/pull.go`**

```go
package main

import (
	"fmt"

	"github.com/fables-for-robots/amber-store-core/key"
	"github.com/fables-for-robots/amber-store-core/reference"
	"github.com/fables-for-robots/amber-store-iroh/protocol"
	"github.com/fables-for-robots/amber-store-iroh/wantsync"
	"github.com/urfave/cli/v2"
)

func pullCommand() *cli.Command {
	var (
		server string
		addrs  cli.StringSlice
	)
	return &cli.Command{
		Name:      "pull",
		Usage:     "fetch ref NAME (and every missing object below it) from the server and set the local ref",
		ArgsUsage: "NAME",
		Flags:     serverFlags(&server, &addrs),
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("pull requires exactly one NAME argument, got %d", c.NArg())
			}
			return runPull(c, server, addrs.Value(), c.Args().First())
		},
	}
}

func runPull(c *cli.Context, server string, addrs []string, name string) error {
	if err := reference.ValidateName(name); err != nil {
		return err
	}
	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	defer closeStore(objects, refs)

	conn, closeConn, err := dialServer(c.Context, server, addrs)
	if err != nil {
		return err
	}
	defer closeConn()
	stream, err := conn.OpenStreamConn(c.Context)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	if err := protocol.WriteMsg(stream, protocol.Msg{Type: protocol.TPull, Name: name}); err != nil {
		return err
	}
	m, err := protocol.ReadMsg(stream)
	if err != nil {
		return err
	}
	switch m.Type {
	case protocol.TRef:
	case protocol.TErr:
		return protocol.RemoteFromMsg(m)
	default:
		return fmt.Errorf("%w: type %d, want TRef", protocol.ErrProtocol, m.Type)
	}
	rec, err := reference.Decode(m.Record)
	if err != nil {
		return fmt.Errorf("server ref record: %w", err)
	}
	root, err := key.Parse(rec.Key)
	if err != nil {
		return fmt.Errorf("server ref record key: %w", err)
	}

	if err := wantsync.Receive(stream, objects, root, 0); err != nil {
		return err
	}
	if err := stream.Close(); err != nil {
		return err
	}

	// Store the server's record verbatim under both the local name and
	// the tracking name: verbatim keeps opaque signature fields intact,
	// and the tracking copy is the next push's CAS expectation. The
	// local update is unconditional — the store is single-owner.
	if err := refs.Put(name, m.Record); err != nil {
		return err
	}
	if err := refs.Put(trackingRef(server, name), m.Record); err != nil {
		return fmt.Errorf("pull succeeded but recording tracking ref failed: %w", err)
	}
	fmt.Fprintf(c.App.Writer, "%s <- %s\n", name, root)
	return nil
}
```

- [ ] **Step 4: Write `cmd/amber/refs.go`**

```go
package main

import (
	"fmt"
	"time"

	"github.com/fables-for-robots/amber-store-core/key"
	"github.com/fables-for-robots/amber-store-iroh/protocol"
	"github.com/urfave/cli/v2"
)

func refsCommand() *cli.Command {
	var (
		server string
		addrs  cli.StringSlice
	)
	return &cli.Command{
		Name:  "refs",
		Usage: "list the references on the server: name, key, creation time, creator",
		Flags: serverFlags(&server, &addrs),
		Action: func(c *cli.Context) error {
			if c.NArg() != 0 {
				return fmt.Errorf("refs takes no arguments, got %d", c.NArg())
			}
			return runRefs(c, server, addrs.Value())
		},
	}
}

// runRefs lists remote refs; it needs no local store.
func runRefs(c *cli.Context, server string, addrs []string) error {
	conn, closeConn, err := dialServer(c.Context, server, addrs)
	if err != nil {
		return err
	}
	defer closeConn()
	stream, err := conn.OpenStreamConn(c.Context)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}
	if err := protocol.WriteMsg(stream, protocol.Msg{Type: protocol.TRefList}); err != nil {
		return err
	}
	m, err := protocol.ReadMsg(stream)
	if err != nil {
		return err
	}
	switch m.Type {
	case protocol.TRefs:
	case protocol.TErr:
		return protocol.RemoteFromMsg(m)
	default:
		return fmt.Errorf("%w: type %d, want TRefs", protocol.ErrProtocol, m.Type)
	}
	if err := stream.Close(); err != nil {
		return err
	}
	for _, r := range m.Refs {
		k, err := key.Parse(r.Key)
		if err != nil {
			return fmt.Errorf("reference %q: %w", r.Name, err)
		}
		line := fmt.Sprintf("%s %s %s", r.Name, k, time.Unix(0, r.CreatedAt).UTC().Format(time.RFC3339))
		if r.User != "" {
			line += " " + r.User
		}
		if _, err := fmt.Fprintln(c.App.Writer, line); err != nil {
			return err
		}
	}
	return nil
}
```

- [ ] **Step 5: Register the commands**

In `cmd/amber/main.go`, replace the `// Task 8 appends` comment line with:

```go
			pushCommand(),
			pullCommand(),
			refsCommand(),
```

- [ ] **Step 6: Verify build and existing tests**

Run: `nix develop -c bash -c 'go mod tidy && go build ./... && go test ./...'`
Expected: everything builds; all prior tests PASS. Network commands get their tests in Task 9.

- [ ] **Step 7: Commit**

```sh
nix develop -c gofmt -l . && nix develop -c go vet ./...
git add cmd/amber/ go.mod go.sum
git commit -m "feat: amber push/pull/refs over iroh with remote-tracking CAS"
```

---

### Task 9: Offline end-to-end test over in-process iroh endpoints

**Files:**
- Test: `cmd/amber/e2e_test.go`

**Interfaces:**
- Consumes: everything. Server side uses `server.New` + `Serve`; client side drives `newApp().Run(...)` with `--addr` direct addresses from `ep.Addr().IPAddrs()` — no pkarr, no DNS, no relays.

- [ ] **Step 1: Write the test**

`cmd/amber/e2e_test.go`:

```go
package main

import (
	"context"
	"log/slog"
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
	ep, err := iroh.Bind(ctx, iroh.WithALPNs(protocol.ALPN))
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
```

- [ ] **Step 2: Run the e2e tests**

Run: `nix develop -c go test ./cmd/amber/ -run 'TestE2E' -v -timeout 120s`
Expected: PASS. Failure modes to check in order: (1) `no direct addresses` — bind the server endpoint before reading `Addr()` and make sure `iroh.Bind` is given no relay option (defaults apply); (2) connect hangs — verify the client's `--addr` path skips resolution and that both sides use `protocol.ALPN`; (3) push hangs at the end — the client must close its connection only after reading TOK (the `closeConn` defer order in push.go already guarantees this).

- [ ] **Step 3: Run the whole suite**

Run: `nix develop -c go test ./...`
Expected: all packages PASS.

- [ ] **Step 4: Commit**

```sh
nix develop -c gofmt -l . && nix develop -c go vet ./...
git add cmd/amber/e2e_test.go
git commit -m "test: offline end-to-end push/pull over in-process iroh endpoints"
```

---

### Task 10: README, spec sync, final verification

**Files:**
- Create: `README.md`
- Modify: `docs/superpowers/specs/2026-07-21-amber-store-iroh-design.md` (inbox deviation)

- [ ] **Step 1: Update the spec for the inbox deviation**

In `docs/superpowers/specs/2026-07-21-amber-store-iroh-design.md`:
1. Architecture diagram line: `amber-store-core: packstore, refstore, reference, fstree, ingest, amberpack, inbox, tarexport, tarextract` — delete `inbox, `.
2. `cmd/amber-serve` bullet: change `` `inbox/` for durable pack receiving `` — delete that clause so the layout reads `` `packstore/` for objects, `refs/` for the refstore ``.
3. Client section: delete the parenthetical `(no `inbox/` needed — pulls drain packs straight into the packstore since the client is a single process and rerunning a pull is cheap)`.
4. Push step 4: replace `Server persists the pack durably via `inbox`, drains it into the packstore,` with `Server verifies each received object against its key and writes it straight into the packstore (parallel verified writes; durability via synced writes),`.
5. In "Interrupted pushes are safe" wording, replace `received packs are durable and deduplicated` with `received objects are fsynced and deduplicated`.

- [ ] **Step 2: Write `README.md`**

```markdown
# Amber-Store Iroh

A peer-to-peer distributed layer over
[amber-store-core](https://github.com/fables-for-robots/amber-store-core):
`amber-serve` hosts an amber store reachable over [iroh](https://iroh.computer)
QUIC; `amber` owns a local store copy, imports directories, and pushes/pulls
refs (with only the missing objects crossing the wire).

**Access is open by design**: anyone who knows the server's endpoint ID can
push and pull. Ref updates are compare-and-swap, tracked per server under
`remotes/<endpoint-id>/<name>` in the client's refstore; `--force` overrides.

## Server

```sh
amber-serve --store ./srv-store --key server.key   # prints its endpoint ID
```

The identity key is generated on first run (hex, `server.key`, gitignored).

## Client

```sh
amber --store ./st import --ref snap ./some/dir    # ingest; prints root key
amber --store ./st push --server ENDPOINT_ID snap  # CAS; --force to override
amber --store ./st pull --server ENDPOINT_ID snap
amber refs --server ENDPOINT_ID                    # list remote refs
amber --store ./st ls ref:snap                     # local commands work offline
amber --store ./st restore ref:snap ./dest
```

`--addr host:port` (repeatable) dials the server directly, skipping
discovery and relays — useful on a LAN and used by the offline e2e tests.

## Development

```sh
direnv allow          # or: nix develop
nix develop -c go build ./...
nix develop -c go test ./...
```

The wire protocol (CBOR frames, have/want rounds, chunked amberpack
payloads) is specified in
[`docs/superpowers/specs/2026-07-21-amber-store-iroh-design.md`](docs/superpowers/specs/2026-07-21-amber-store-iroh-design.md).

- Module: `github.com/fables-for-robots/amber-store-iroh` (fetching the
  private core module needs `GOPRIVATE=github.com/fables-for-robots/*`).
- Go: 1.26+
```

- [ ] **Step 3: Final verification**

```sh
nix develop -c bash -c 'gofmt -l . && go vet ./... && go build ./... && go test ./...'
```
Expected: gofmt silent, vet clean, all tests PASS. Also confirm no stray built binaries in the tree (`git status --porcelain` shows only intended files); delete any.

- [ ] **Step 4: Commit**

```sh
git add README.md docs/
git commit -m "docs: README and spec sync (inbox replaced by verified direct writes)"
```
