# De-vendoring jobs-iroh Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Bring transport-iroh to parity with jobs-iroh's vendored `amberiroh` copy, add a single-import facade, release v0.2.0, then make jobs-iroh import it and delete the copy, releasing v0.32.0.

**Architecture:** Phase 1 (Tasks 1–8) lives in `amber-store/transport-iroh`: the vendored changes are ported by diff into the existing `protocol`, `wantsync` and `server` packages with their tests, go-iroh is bumped to the version jobs-iroh already uses, and a new `amberiroh` package re-exports the four packages' surface. Phase 2 (Tasks 9–11) lives in `jobs-build/jobs-iroh`: delete `amberiroh/`, point the 13 importing files at the facade, update CLAUDE.md, release with Docker images.

**Tech Stack:** Go 1.26.5, `github.com/amber-store/core` v0.0.4, `github.com/tmc/go-iroh` v0.1.0, `github.com/fxamacker/cbor/v2` (keyasint CBOR frames), `gh` CLI, Docker buildx.

**Spec:** `docs/superpowers/specs/2026-09-07-devendor-jobs-iroh-design.md`

## Global Constraints

- Go `1.26.5` in both go.mod files; the local toolchain is 1.26.3 with `GOTOOLCHAIN=auto`, which downloads 1.26.5 on demand.
- The wire ALPN `amber-store-iroh/1` is never changed.
- All new wire fields are additive: `DataEndpoints` at CBOR key `16`, `Names` at key `17`, both `omitempty`; `TPin = 13`.
- `Serve`, `serveConn` and `protocol.SendPack` stay in transport-iroh (the CLIs use them). `shardWants` stays (its test uses it).
- Every commit must leave `gofmt -l $(git ls-files '*.go')` empty and `go build ./... && go vet ./... && go test ./...` green in the repo being changed.
- jobs-iroh additionally requires `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go vet ./...` (its CLAUDE.md rule) before a commit that touches Go files.
- Commit messages end with the two attribution lines used throughout this session:
  `Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>` and
  `Claude-Session: https://claude.ai/code/session_01GAJmL5jzzyWvFiikdwFCEn`.
- Paths: phase 1 work happens in the clone at
  `/private/tmp/claude-502/-Users-dragan-jobs-build-amber-store-core/32131b9f-765f-4003-8381-d843b1046270/scratchpad/deps/amber-store-iroh` (called `$T` below), on branch `backport-parity`, which already holds the spec commit. Phase 2 happens in `~/jobs-build/jobs-iroh` (called `$J`), currently clean on `main` at v0.31.1.
- The vendored reference copy is `$J/amberiroh/` at commit `b37082d`; when in doubt about a detail, diff against it.

---

## Phase 1: transport-iroh

### Task 1: protocol — DataEndpointRec, DataEndpoints/Names fields, TPin

**Files:**
- Modify: `$T/protocol/protocol.go:28-82`
- Test: `$T/protocol/protocol_test.go`

**Interfaces:**
- Produces: `protocol.DataEndpointRec{ID []byte; Addrs []string}`, `protocol.Msg.DataEndpoints []DataEndpointRec`, `protocol.Msg.Names []string`, `protocol.TPin = 13`. Tasks 4, 5 and 7 use all four.

- [ ] **Step 1: Add the failing tests**

Add `"reflect"` to the import block of `protocol/protocol_test.go` (between `"io"` and `"testing"`), then append at the end of the file:

```go
func TestMsgDataEndpointsRoundTrip(t *testing.T) {
	in := Msg{Type: TAccept, Token: []byte{1}, DataPorts: []uint16{4001, 4002},
		DataEndpoints: []DataEndpointRec{
			{ID: bytes.Repeat([]byte{7}, 32), Addrs: []string{"ip:192.168.1.5:4001", "relay:https://euc1-1.relay.example./"}},
			{ID: bytes.Repeat([]byte{8}, 32), Addrs: []string{"ip:192.168.1.5:4002"}},
		}}
	var buf bytes.Buffer
	if err := WriteMsg(&buf, in); err != nil {
		t.Fatal(err)
	}
	out, err := ReadMsg(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch:\n in %+v\nout %+v", in, out)
	}
}

// Old peers must interoperate across field 16: an old decoder ignores it on
// new frames, and a new decoder yields nil for its absence on old frames.
func TestMsgDataEndpointsCompat(t *testing.T) {
	type oldMsg struct {
		Type      int      `cbor:"0,keyasint"`
		Token     []byte   `cbor:"13,keyasint,omitempty"`
		DataPorts []uint16 `cbor:"15,keyasint,omitempty"`
	}
	in := Msg{Type: TAccept, Token: []byte{1}, DataPorts: []uint16{4001},
		DataEndpoints: []DataEndpointRec{{ID: bytes.Repeat([]byte{7}, 32), Addrs: []string{"ip:127.0.0.1:4001"}}}}
	payload, err := encMode.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	var old oldMsg
	if err := decMode.Unmarshal(payload, &old); err != nil {
		t.Fatalf("old decoder rejects new frame: %v", err)
	}
	if old.Type != TAccept || len(old.DataPorts) != 1 || len(old.Token) != 1 {
		t.Fatalf("old decode mangled fields: %+v", old)
	}
	oldPayload, err := encMode.Marshal(oldMsg{Type: TAccept, Token: []byte{1}, DataPorts: []uint16{4001}})
	if err != nil {
		t.Fatal(err)
	}
	var m Msg
	if err := decMode.Unmarshal(oldPayload, &m); err != nil {
		t.Fatal(err)
	}
	if m.DataEndpoints != nil {
		t.Fatalf("absent field decoded non-nil: %+v", m.DataEndpoints)
	}
}

func TestMsgPinNamesRoundTrip(t *testing.T) {
	in := Msg{Type: TPin, Names: []string{"a/b", "c"}}
	var buf bytes.Buffer
	if err := WriteMsg(&buf, in); err != nil {
		t.Fatal(err)
	}
	out, err := ReadMsg(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if out.Type != 13 || !reflect.DeepEqual(out.Names, in.Names) {
		t.Fatalf("got %+v", out)
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd $T && go test ./protocol/ -run 'TestMsgDataEndpoints|TestMsgPinNames' 2>&1 | head`
Expected: build failure, `undefined: DataEndpointRec` and `undefined: TPin`.

- [ ] **Step 3: Implement**

In `protocol/protocol.go`, add `TPin` to the frame-type const block after `TAccept`:

```go
	TAccept  = 12 // server→client: sharded transfer accepted (Token)
	TPin     = 13 // client→server: keep these refs forever (Names) — GC pin-assert
```

Insert the record type between `RefInfo` and `Msg` (after the `RefInfo` struct's closing brace):

```go
// DataEndpointRec describes one of the server's data endpoints to a
// sharding client: the endpoint's own identity (data endpoints carry their
// own keys so each can hold a relay home connection — relays key sessions
// by endpoint ID) and its dial candidates as netaddr.TransportAddr strings
// ("ip:host:port", "relay:url"). The client trusts the identity because the
// record arrives on the control connection, which authenticated the server;
// the shard handshake then proves possession of the advertised key. Old
// peers ignore the field and keep direct-dialing DataPorts.
type DataEndpointRec struct {
	ID    []byte   `cbor:"0,keyasint"`
	Addrs []string `cbor:"1,keyasint,omitempty"`
}
```

Append two fields to `Msg` after `DataPorts`:

```go
	DataPorts   []uint16  `cbor:"15,keyasint,omitempty"` // TAccept/TRef: server data-endpoint UDP ports for the extra connections
	// DataEndpoints describes the data endpoints as punchable peers on
	// TAccept/TRef. Its presence doubles as the capability signal that the
	// server gathers attaches for the longer punch-friendly window.
	DataEndpoints []DataEndpointRec `cbor:"16,keyasint,omitempty"`
	// Names are the ref names of a TPin assert. Additive: old peers
	// ignore the field, old servers answer TPin itself with TErr.
	Names []string `cbor:"17,keyasint,omitempty"`
```

Run `gofmt -w protocol/protocol.go` (the struct alignment changes).

- [ ] **Step 4: Run the package tests**

Run: `cd $T && go test ./protocol/`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
cd $T && git add protocol/protocol.go protocol/protocol_test.go && git commit -m "protocol: DataEndpoints and Names wire fields, TPin frame type

Ported from jobs-iroh's amberiroh copy (c0380b2, ca7670c). Both fields
are additive: old peers ignore them, old servers answer TPin with TErr.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01GAJmL5jzzyWvFiikdwFCEn"
```

---

### Task 2: wantsync — throughput-weighted want dealing

**Files:**
- Modify: `$T/wantsync/wantsync.go:174-209` (Receive) and after `shardWants` (~line 334)
- Test: `$T/wantsync/wantsync_test.go`

**Interfaces:**
- Produces: `dealWants(wants []key.Key, weights []int64) [][]key.Key` (unexported; used only inside `Receive`).

- [ ] **Step 1: Add the failing tests**

Add `"encoding/binary"` as the first entry of the standard-library import group in `wantsync/wantsync_test.go`, then append at the end of the file:

```go
var lengthKeySeq uint32

// lengthKey builds a distinct key whose embedded logical length is n.
func lengthKey(t *testing.T, n uint64) key.Key {
	t.Helper()
	lengthKeySeq++
	var h [key.Size]byte
	binary.BigEndian.PutUint32(h[:4], lengthKeySeq)
	k, err := key.NewFromHash(key.Blob, n, h)
	if err != nil {
		t.Fatal(err)
	}
	return k
}

func TestDealWantsProportionalToWeights(t *testing.T) {
	var wants []key.Key
	for range 100 {
		wants = append(wants, lengthKey(t, 1000))
	}
	shards := dealWants(wants, []int64{3000, 1000})
	if len(shards) != 2 {
		t.Fatalf("shards %d, want 2", len(shards))
	}
	total := len(shards[0]) + len(shards[1])
	if total != 100 {
		t.Fatalf("dealt %d keys, want 100", total)
	}
	// 3:1 weights over uniform keys: the fast channel gets ~75, slow ~25.
	if len(shards[0]) < 65 || len(shards[0]) > 85 {
		t.Fatalf("fast shard got %d of 100, want ~75", len(shards[0]))
	}
}

func TestDealWantsZeroWeightsFallBackToEven(t *testing.T) {
	var wants []key.Key
	for range 10 {
		wants = append(wants, lengthKey(t, 100))
	}
	shards := dealWants(wants, []int64{0, 0})
	if len(shards[0]) != 5 || len(shards[1]) != 5 {
		t.Fatalf("zero weights: %d/%d, want 5/5", len(shards[0]), len(shards[1]))
	}
}

func TestDealWantsSingleChannel(t *testing.T) {
	wants := []key.Key{lengthKey(t, 1), lengthKey(t, 2)}
	shards := dealWants(wants, []int64{7})
	if len(shards) != 1 || len(shards[0]) != 2 {
		t.Fatalf("single channel must carry everything")
	}
}

func TestDealWantsCoversAllKeysOnce(t *testing.T) {
	var wants []key.Key
	for i := range 31 {
		wants = append(wants, lengthKey(t, uint64(i+1)*17))
	}
	shards := dealWants(wants, []int64{5, 0, 2})
	seen := map[key.Key]int{}
	n := 0
	for _, sh := range shards {
		for _, k := range sh {
			seen[k]++
			n++
		}
	}
	if n != 31 {
		t.Fatalf("dealt %d keys, want 31", n)
	}
	for k, c := range seen {
		if c != 1 {
			t.Fatalf("key %s dealt %d times", k, c)
		}
	}
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cd $T && go test ./wantsync/ -run TestDealWants 2>&1 | head -3`
Expected: `undefined: dealWants`.

- [ ] **Step 3: Implement dealWants**

Append after `shardWants` in `wantsync/wantsync.go` (before the `checkDelivered` comment):

```go
// dealWants distributes wants across len(weights) shards proportionally to
// the weights — observed per-channel throughput, so a slow channel (say, a
// shard stuck on the relay while its siblings punched) stops being the
// per-round straggler every other channel barriers on. Keys are weighed by
// their embedded logical lengths; each key goes to the channel with the
// lowest assigned-bytes-to-weight ratio (ties to the lower index).
// Non-positive weights are floored to 1 so no channel is locked out — it
// keeps a probe-sized share and earns its weight back.
func dealWants(wants []key.Key, weights []int64) [][]key.Key {
	n := len(weights)
	shards := make([][]key.Key, n)
	if n == 1 {
		shards[0] = wants
		return shards
	}
	w := make([]float64, n)
	for i, wt := range weights {
		if wt < 1 {
			wt = 1
		}
		w[i] = float64(wt)
	}
	assigned := make([]float64, n)
	for _, k := range wants {
		best := 0
		for i := 1; i < n; i++ {
			if assigned[i]/w[i] < assigned[best]/w[best] {
				best = i
			}
		}
		bytes := float64(k.Length())
		if bytes < 1 {
			bytes = 1
		}
		assigned[best] += bytes
		shards[best] = append(shards[best], k)
	}
	return shards
}
```

- [ ] **Step 4: Wire it into Receive**

In `Receive`, after `frontier := []key.Key{root}` (line 187) and before `for {`, add:

```go
	// Per-channel throughput sampling for dealWants: each round's byte
	// deltas weight the next round's shards, so channel-speed asymmetry
	// (a relayed straggler among punched siblings) stops dictating the
	// whole round's pace.
	prevWire := make([]int64, len(channels))
	weights := make([]int64, len(channels))
```

Replace the single line `shards := shardWants(wants, len(channels))` with:

```go
		for i, cr := range crs {
			weights[i] = cr.n - prevWire[i]
			prevWire[i] = cr.n
		}
		shards := dealWants(wants, weights)
```

- [ ] **Step 5: Run the package tests**

Run: `cd $T && go test ./wantsync/`
Expected: `ok` (including the existing `TestShardWants` and the loop tests, which exercise `Receive` end to end).

- [ ] **Step 6: Commit**

```bash
cd $T && git add wantsync/wantsync.go wantsync/wantsync_test.go && git commit -m "wantsync: deal wants by observed per-channel throughput

Ported from jobs-iroh's amberiroh copy (8610e8a). Round-robin dealing
let one slow channel pace every round; Receive now samples each
channel's wire bytes per round and deals the next round proportionally.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01GAJmL5jzzyWvFiikdwFCEn"
```

---

### Task 3: server — retire streams fully, 10s attach window

**Files:**
- Modify: `$T/server/server.go:115-139, 164-197, 330-335, 398-403`
- Test: `$T/server/server_test.go`

**Interfaces:**
- Produces: `closeStream(rw io.Closer)` (unexported), used by Tasks 4 and 5's code paths.

- [ ] **Step 1: Add the failing test**

Append to `server/server_test.go`:

```go
func TestAttachWaitDefaultCoversPunching(t *testing.T) {
	s := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil)
	if s.attachWait != 10*time.Second {
		t.Fatalf("attachWait %v, want 10s (punching attaches ride the relay first)", s.attachWait)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd $T && go test ./server/ -run TestAttachWaitDefaultCoversPunching`
Expected: FAIL, `attachWait 5s, want 10s`.

- [ ] **Step 3: Implement**

In `server/server.go`:

Change the `attachWait` field comment (lines 34–36) to:

```go
	// attachWait bounds how long a sharded transfer waits for the
	// client's promised data connections before proceeding with
	// whatever attached. 10s covers a punching attach — relay connect
	// then TAttach — while gather's early exit keeps fast attaches free.
	attachWait time.Duration
```

Insert before the `// drop unregisters the token` comment (line 115):

```go
// closeStream fully terminates a server-side stream: Close finishes the
// send side (FIN), CancelRead sends STOP_SENDING so the client→server half
// completes without the handler reading to EOF. Without the cancel the
// stream never retires and its MAX_STREAMS credit is never returned — a
// sharding client that reuses one connection for many transfers runs dry
// at exactly the initial stream budget (observed in the field as
// OpenStreamSync hanging after precisely 100 attaches).
func closeStream(rw io.Closer) {
	_ = rw.Close()
	if cr, ok := rw.(interface{ CancelRead(code uint64) }); ok {
		cr.CancelRead(0)
	}
}
```

Then replace every server-side close call:

- In `drop`: `rw.Close()` → `closeStream(rw)`.
- In `New`: `attachWait: 5 * time.Second` → `attachWait: 10 * time.Second`.
- In `HandleStream`: the three occurrences `rw.Close()` (after the read error, after the unknown-token TErr, and `defer rw.Close()`) → `closeStream(rw)` / `defer closeStream(rw)`.
- In `shardChannels`'s `release` closure: `e.Close()` → `closeStream(e)`.
- In `handlePull`'s deferred cleanup: `e.Close()` → `closeStream(e)`.

Check nothing was missed: `grep -n '\.Close()' server/server.go` must show only calls on `conn` inside `serveConn` (the QUIC connection, not a stream).

- [ ] **Step 4: Run the package tests**

Run: `cd $T && go test ./server/`
Expected: `ok`. (`net.Pipe` ends in the tests have no `CancelRead`, so the type assertion is simply false there.)

- [ ] **Step 5: Commit**

```bash
cd $T && git add server/server.go server/server_test.go && git commit -m "server: retire streams with CancelRead; 10s attach gather window

Ported from jobs-iroh's amberiroh copy (3da06c4, 22ac70b). Streams that
were only Close()d never returned MAX_STREAMS credit, starving pooled
clients after exactly 100 attaches.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01GAJmL5jzzyWvFiikdwFCEn"
```

---

### Task 3b (added during execution): amber CLI tolerates the server's STOP_SENDING

**Why:** after Task 3 the server retires each request stream with `CancelRead` as soon as it has answered. The `amber` CLI's push, pull and refs commands did `if err := stream.Close(); err != nil { return err }` after reading the final frame, and go-iroh reports `close called for canceled stream N` when the peer has already canceled the send side — so all four `cmd/amber` e2e tests failed. jobs-iroh's `amberclient.CloseStream` has handled exactly this since July.

**Files:**
- Modify: `$T/cmd/amber/dial.go` (new helper), `$T/cmd/amber/push.go:142`, `$T/cmd/amber/pull.go:124`, `$T/cmd/amber/refs.go:56`

- [x] **Step 1: Failing tests** — `go test ./cmd/amber/` fails `TestE2EPushPullRestoreRoundTrip`, `TestE2ECASConflictAndForce`, `TestE2EInterruptedPushResume`, `TestE2ESingleConn` with `close called for canceled stream 0`.

- [x] **Step 2: Implement** — append to `cmd/amber/dial.go`:

```go
// closeStream ends a one-request stream once the final frame has been
// read. Close is best effort: the server retires its side with
// STOP_SENDING as soon as it has answered, and closing a stream the peer
// already canceled reports an error that carries no information.
// CancelRead then completes the receive half so the stream fully retires
// and its MAX_STREAMS credit comes back — without it every operation on a
// reused connection leaks one stream until the 101st open blocks.
func closeStream(stream io.Closer) {
	_ = stream.Close()
	if cr, ok := stream.(interface{ CancelRead(code uint64) }); ok {
		cr.CancelRead(0)
	}
}
```

and replace the three `if err := stream.Close(); err != nil { return err }` blocks with `closeStream(stream)`.

- [x] **Step 3: Verify** — `go test ./cmd/amber/` and the full suite pass.

- [x] **Step 4: Commit** — `amber: best-effort stream close after the final frame`.

---

### Task 4: server — advertise data endpoints on TAccept and TRef

**Files:**
- Modify: `$T/server/server.go` (struct, setters after `SetDataPorts`, `shardChannels`, `handlePull`)
- Test: `$T/server/server_test.go`

**Interfaces:**
- Consumes: `protocol.DataEndpointRec`, `protocol.Msg.DataEndpoints` (Task 1).
- Produces: `func (s *Server) SetDataEndpoints(f func() []protocol.DataEndpointRec)`.

- [ ] **Step 1: Add the failing test**

Add `"reflect"` to the import block of `server/server_test.go` (after `"path/filepath"`). Append:

```go
func TestShardChannelsAdvertisesDataEndpoints(t *testing.T) {
	s := New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil)
	s.attachWait = 50 * time.Millisecond
	s.SetDataPorts([]uint16{4001})
	rec := protocol.DataEndpointRec{ID: bytes.Repeat([]byte{7}, 32), Addrs: []string{"ip:127.0.0.1:4001"}}
	s.SetDataEndpoints(func() []protocol.DataEndpointRec { return []protocol.DataEndpointRec{rec} })

	cli, srv := net.Pipe()
	got := make(chan protocol.Msg, 1)
	go func() {
		m, err := protocol.ReadMsg(cli)
		if err != nil {
			t.Error(err)
		}
		got <- m
		cli.Close()
	}()
	channels, release, err := s.shardChannels(srv, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	if len(channels) != 1 {
		t.Fatalf("gathered %d channels, want control only", len(channels))
	}
	m := <-got
	if m.Type != protocol.TAccept {
		t.Fatalf("type %d, want TAccept", m.Type)
	}
	if !reflect.DeepEqual(m.DataEndpoints, []protocol.DataEndpointRec{rec}) {
		t.Fatalf("DataEndpoints %+v, want %+v", m.DataEndpoints, rec)
	}
	if !reflect.DeepEqual(m.DataPorts, []uint16{4001}) {
		t.Fatalf("DataPorts %+v", m.DataPorts)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd $T && go test ./server/ -run TestShardChannelsAdvertisesDataEndpoints 2>&1 | head -3`
Expected: `s.SetDataEndpoints undefined`.

- [ ] **Step 3: Implement**

Add a field after `dataPorts []uint16` in the `Server` struct:

```go
	dataPorts []uint16
	// dataEndpoints, when set, snapshots the data endpoints' identities and
	// live dial candidates for TAccept/TRef — a closure because relay and
	// QAD candidates appear asynchronously after bind.
	dataEndpoints func() []protocol.DataEndpointRec
```

Add a setter directly after `SetDataPorts`:

```go
// SetDataEndpoints installs the data-endpoint snapshot advertised to
// sharding clients; call before Serve, like SetDataPorts.
func (s *Server) SetDataEndpoints(f func() []protocol.DataEndpointRec) { s.dataEndpoints = f }
```

In `shardChannels`, replace

```go
	if err := protocol.WriteMsg(rw, protocol.Msg{Type: protocol.TAccept, Token: token, DataPorts: s.dataPorts}); err != nil {
```

with

```go
	accept := protocol.Msg{Type: protocol.TAccept, Token: token, DataPorts: s.dataPorts}
	if s.dataEndpoints != nil {
		accept.DataEndpoints = s.dataEndpoints()
	}
	if err := protocol.WriteMsg(rw, accept); err != nil {
```

In `handlePull`, after `ref.DataPorts = s.dataPorts` add:

```go
		ref.DataPorts = s.dataPorts
		if s.dataEndpoints != nil {
			ref.DataEndpoints = s.dataEndpoints()
		}
```

- [ ] **Step 4: Run the package tests**

Run: `cd $T && go test ./server/`
Expected: `ok`.

- [ ] **Step 5: Commit**

```bash
cd $T && git add server/server.go server/server_test.go && git commit -m "server: advertise data endpoints on TAccept and TRef

Ported from jobs-iroh's amberiroh copy (22ac70b). SetDataEndpoints
installs a snapshot closure; its presence on the wire is the signal
that the server gathers attaches for the punch-friendly window.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01GAJmL5jzzyWvFiikdwFCEn"
```

---

### Task 5: server — TPin, access and pin hooks, push ref guard

**Files:**
- Modify: `$T/server/server.go` (struct, new `RefGuard` type, setters, `HandleStream` switch, new `handlePin`, `handlePush`, `handlePull`)
- Create: `$T/server/server_gc_test.go`

**Interfaces:**
- Consumes: `protocol.TPin`, `protocol.Msg.Names` (Task 1); `doPush`, `testServer`, `clientStore` helpers already in `server_test.go`.
- Produces: `type RefGuard interface { PrepareRef(root key.Key) (commit, abort func(), err error) }`, `SetOnAccess(func(name string))`, `SetOnPin(func(name string))`, `SetRefGuard(RefGuard)`. Task 7 aliases `RefGuard`.

- [ ] **Step 1: Write the failing tests**

Create `server/server_gc_test.go`:

```go
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
```

- [ ] **Step 2: Run them to verify they fail**

Run: `cd $T && go test ./server/ -run 'TestPin|TestPullFiresOnAccess|TestPushFiresOnAccess|TestPushTakesRefGuard' 2>&1 | head -5`
Expected: `srv.SetOnPin undefined`, `srv.SetOnAccess undefined`, `srv.SetRefGuard undefined`.

- [ ] **Step 3: Implement**

In `server/server.go`:

Add hook fields to the `Server` struct after `dataEndpoints`:

```go
	dataEndpoints func() []protocol.DataEndpointRec

	// onAccess, when set, is called with every ref name a pull resolved or
	// a push committed — the GC access-tracking seam. onPin is called for
	// every existing name in a TPin assert. guard, when set, brackets the
	// push path's reference write with the collector's PrepareRef. All
	// three are set before Serve, like SetDataPorts.
	onAccess func(name string)
	onPin    func(name string)
	guard    RefGuard
```

Add the interface right after the `Server` struct's closing brace:

```go
// RefGuard is the GC write barrier around reference publication.
// *gc.Collector from amber-store-core satisfies it directly.
type RefGuard interface {
	PrepareRef(root key.Key) (commit, abort func(), err error)
}
```

Add setters after `SetDataEndpoints`:

```go
// SetOnAccess installs the ref-access hook. Call before Serve.
func (s *Server) SetOnAccess(f func(name string)) { s.onAccess = f }

// SetOnPin installs the pin-assert hook. Call before Serve.
func (s *Server) SetOnPin(f func(name string)) { s.onPin = f }

// SetRefGuard installs the reference write barrier. Call before Serve.
func (s *Server) SetRefGuard(g RefGuard) { s.guard = g }
```

In `HandleStream`'s switch, add a case after `case protocol.TPull:`:

```go
	case protocol.TPull:
		err = s.handlePull(rw, m)
	case protocol.TPin:
		err = s.handlePin(rw, m)
```

Add `handlePin` after `handleRefList`:

```go
// handlePin marks the named refs kept-forever. Nonexistent names are
// ignored, not an error — a registry may assert ahead of a re-resolve. Any
// other refs.Get error (a real store problem, not a missing name) fails the
// whole request instead of being silently swallowed.
func (s *Server) handlePin(rw io.ReadWriter, m protocol.Msg) error {
	for _, name := range m.Names {
		if _, err := s.refs.Get(name); err != nil {
			if errors.Is(err, refstore.ErrNotFound) {
				continue
			}
			return s.fail(rw, protocol.CodeInternal, err)
		}
		if s.onPin != nil {
			s.onPin(name)
		}
	}
	return protocol.WriteMsg(rw, protocol.Msg{Type: protocol.TOK})
}
```

In `handlePush`, replace

```go
	if err := s.refs.Put(m.Name, raw); err != nil {
		return s.fail(rw, protocol.CodeInternal, err)
	}
	if err := protocol.WriteMsg(rw, protocol.Msg{Type: protocol.TOK, Key: root[:]}); err != nil {
```

with

```go
	if s.guard != nil {
		commit, abort, gerr := s.guard.PrepareRef(root)
		if gerr != nil {
			return s.fail(rw, protocol.CodeInternal, fmt.Errorf("gc guard: %w", gerr))
		}
		if err := s.refs.Put(m.Name, raw); err != nil {
			abort()
			return s.fail(rw, protocol.CodeInternal, err)
		}
		commit()
	} else if err := s.refs.Put(m.Name, raw); err != nil {
		return s.fail(rw, protocol.CodeInternal, err)
	}
	if s.onAccess != nil {
		s.onAccess(m.Name)
	}
	if err := protocol.WriteMsg(rw, protocol.Msg{Type: protocol.TOK, Key: root[:]}); err != nil {
```

In `handlePull`, after the `if err != nil { return s.fail(rw, protocol.CodeInternal, err) }` that follows `s.refs.Get(m.Name)`, and before `ref := protocol.Msg{...}`, add:

```go
	if s.onAccess != nil {
		s.onAccess(m.Name)
	}
```

- [ ] **Step 4: Run the whole repo's tests**

Run: `cd $T && gofmt -l $(git ls-files '*.go'); go build ./... && go vet ./... && go test ./...`
Expected: no gofmt output; all packages `ok`.

- [ ] **Step 5: Diff against the vendored copy to confirm parity**

Run:

```bash
cd $T && norm() { sed -E 's/^package .*//; s#"github.com/amber-store/transport-iroh/[a-z]+"##; s/\b(protocol|wantsync|relaymode|server)\.([A-Z])/\2/g' "$1"; }
diff <(norm server/server.go) <(norm ~/jobs-build/jobs-iroh/amberiroh/server.go) | grep '^[<>]' | grep -v '^[<>]\s*$'
```

Expected: only the package doc comment, the `context`/`iroh` imports, the `RefGuard` doc comment wording, and the `Serve`/`serveConn` functions (all upstream-only by design). Anything else is a porting mistake; fix it before committing. Repeat for `protocol/protocol.go` vs `protocol.go` (expect only the doc comment) and `wantsync/wantsync.go` vs `wantsync.go` (expect only the doc comment and the import line).

- [ ] **Step 6: Commit**

```bash
cd $T && git add server/server.go server/server_gc_test.go && git commit -m "server: TPin pin-asserts, access and pin hooks, push ref guard

Ported from jobs-iroh's amberiroh copy (ca7670c, def46cc). All hooks
are no-ops when unset; *gc.Collector satisfies RefGuard directly.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01GAJmL5jzzyWvFiikdwFCEn"
```

---

### Task 6: go-iroh v0.1.0

**Files:**
- Modify: `$T/go.mod`, `$T/go.sum`

- [ ] **Step 1: Bump**

Run: `cd $T && go get github.com/tmc/go-iroh@v0.1.0 && go mod tidy && grep -n go-iroh go.mod`
Expected: `github.com/tmc/go-iroh v0.1.0` in the direct require block.

- [ ] **Step 2: Verify**

Run: `cd $T && go build ./... && go vet ./... && go test ./...`
Expected: all `ok`.

- [ ] **Step 3: Commit**

```bash
cd $T && git add go.mod go.sum && git commit -m "deps: go-iroh v0.1.0

jobs-iroh already requires v0.1.0; once it imports this module, MVS
selects v0.1.0 regardless, so test against it here.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01GAJmL5jzzyWvFiikdwFCEn"
```

---

### Task 7: facade package `amberiroh`

**Files:**
- Create: `$T/amberiroh/amberiroh.go`
- Create: `$T/amberiroh/amberiroh_test.go`
- Modify: `$T/README.md:70-71`

**Interfaces:**
- Consumes: everything exported from `protocol`, `wantsync`, `server`, `relaymode`, including Task 1's `DataEndpointRec`/`TPin` and Task 5's `RefGuard`.
- Produces: the import path `github.com/amber-store/transport-iroh/amberiroh` with the identifiers listed below; Task 9 depends on all of them.

- [ ] **Step 1: Write the failing test**

Create `amberiroh/amberiroh_test.go`:

```go
package amberiroh_test

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"

	"github.com/amber-store/transport-iroh/amberiroh"
	"github.com/amber-store/transport-iroh/protocol"
)

// Every wrapper must exist with the upstream signature; a missing one is a
// compile error here rather than in a consumer.
var _ = []any{
	amberiroh.WriteMsg, amberiroh.ReadMsg, amberiroh.RemoteFromMsg,
	amberiroh.NewPackReader, amberiroh.SendPack, amberiroh.SendPackRecords,
	amberiroh.Send, amberiroh.Receive, amberiroh.Wants,
	amberiroh.New, amberiroh.FromFlag,
}

func TestFacadeMsgRoundTrip(t *testing.T) {
	in := amberiroh.Msg{Type: amberiroh.TAccept, Token: []byte{1},
		DataEndpoints: []amberiroh.DataEndpointRec{{ID: bytes.Repeat([]byte{7}, 32), Addrs: []string{"ip:127.0.0.1:4001"}}}}
	var buf bytes.Buffer
	if err := amberiroh.WriteMsg(&buf, in); err != nil {
		t.Fatal(err)
	}
	out, err := amberiroh.ReadMsg(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch:\n in %+v\nout %+v", in, out)
	}
}

func TestFacadeServerHooks(t *testing.T) {
	srv := amberiroh.New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil)
	srv.SetDataPorts([]uint16{4001})
	srv.SetDataEndpoints(func() []amberiroh.DataEndpointRec { return nil })
	srv.SetOnAccess(func(string) {})
	srv.SetOnPin(func(string) {})
	var g amberiroh.RefGuard
	srv.SetRefGuard(g)
	_ = srv.HandleStream
}

func TestFacadeIdentities(t *testing.T) {
	if !errors.Is(amberiroh.ErrProtocol, protocol.ErrProtocol) {
		t.Fatal("ErrProtocol must be the same error value as protocol.ErrProtocol")
	}
	if amberiroh.ALPN != protocol.ALPN || amberiroh.TPin != protocol.TPin || amberiroh.CodeInternal != protocol.CodeInternal {
		t.Fatal("constants must mirror protocol's values")
	}
	var m amberiroh.Msg = protocol.Msg{Type: protocol.TOK}
	if m.Type != amberiroh.TOK {
		t.Fatal("Msg must alias protocol.Msg")
	}
	re := amberiroh.RemoteFromMsg(amberiroh.Msg{Type: amberiroh.TErr, Code: amberiroh.CodeBadRequest, Text: "x"})
	if re == nil || re.Code != protocol.CodeBadRequest {
		t.Fatalf("RemoteFromMsg: %+v", re)
	}
}
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd $T && go test ./amberiroh/ 2>&1 | head -3`
Expected: `no Go files` / package not found.

- [ ] **Step 3: Write the facade**

Create `amberiroh/amberiroh.go`:

```go
// Package amberiroh is the single-import surface of transport-iroh: it
// re-exports the protocol, wantsync, server and relaymode packages so a
// consumer can write amberiroh.Msg, amberiroh.Receive, amberiroh.New and
// amberiroh.FromFlag without importing four paths. The four packages remain
// the implementation; nothing lives here but type aliases, re-declared
// constants and thin wrappers. Every exported identifier added to those
// packages must be mirrored here — amberiroh_test.go exists so a missing
// mirror fails to compile in this repo rather than in a consumer.
package amberiroh

import (
	"io"
	"iter"
	"log/slog"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/core/refstore"
	"github.com/amber-store/transport-iroh/protocol"
	"github.com/amber-store/transport-iroh/relaymode"
	"github.com/amber-store/transport-iroh/server"
	"github.com/amber-store/transport-iroh/wantsync"
	"github.com/tmc/go-iroh/relay"
)

// Wire parameters (protocol). ALPN is a wire constant: renaming it breaks
// every peer.
const (
	ALPN      = protocol.ALPN
	MaxFrame  = protocol.MaxFrame
	ChunkSize = protocol.ChunkSize
)

// Frame types (protocol).
const (
	TPush    = protocol.TPush
	TPull    = protocol.TPull
	TRefList = protocol.TRefList
	TRef     = protocol.TRef
	TRefs    = protocol.TRefs
	TWants   = protocol.TWants
	TData    = protocol.TData
	TDataEnd = protocol.TDataEnd
	TOK      = protocol.TOK
	TErr     = protocol.TErr
	TAttach  = protocol.TAttach
	TAccept  = protocol.TAccept
	TPin     = protocol.TPin
)

// Error codes carried in TErr frames (protocol).
const (
	CodeCASMismatch = protocol.CodeCASMismatch
	CodeUnknownRef  = protocol.CodeUnknownRef
	CodeBadRequest  = protocol.CodeBadRequest
	CodeInternal    = protocol.CodeInternal
)

// ErrProtocol is protocol.ErrProtocol; errors.Is matches under either name.
var ErrProtocol = protocol.ErrProtocol

// Types. Aliases carry their methods, so *Server has HandleStream, Serve
// and the Set* hooks, and *RemoteError implements error.
type (
	Msg             = protocol.Msg
	RefInfo         = protocol.RefInfo
	RemoteError     = protocol.RemoteError
	DataEndpointRec = protocol.DataEndpointRec
	Stats           = wantsync.Stats
	Progress        = wantsync.Progress
	Server          = server.Server
	RefGuard        = server.RefGuard
)

// WriteMsg is protocol.WriteMsg.
func WriteMsg(w io.Writer, m Msg) error { return protocol.WriteMsg(w, m) }

// ReadMsg is protocol.ReadMsg.
func ReadMsg(r io.Reader) (Msg, error) { return protocol.ReadMsg(r) }

// RemoteFromMsg is protocol.RemoteFromMsg.
func RemoteFromMsg(m Msg) *RemoteError { return protocol.RemoteFromMsg(m) }

// NewPackReader is protocol.NewPackReader.
func NewPackReader(r io.Reader) io.Reader { return protocol.NewPackReader(r) }

// SendPack is protocol.SendPack.
func SendPack(w io.Writer, objs iter.Seq2[fstree.Object, error]) error {
	return protocol.SendPack(w, objs)
}

// SendPackRecords is protocol.SendPackRecords.
func SendPackRecords(w io.Writer, recs iter.Seq2[[]byte, error]) error {
	return protocol.SendPackRecords(w, recs)
}

// Send is wantsync.Send.
func Send(rw io.ReadWriter, st *packstore.Store, prog Progress) error {
	return wantsync.Send(rw, st, prog)
}

// Receive is wantsync.Receive.
func Receive(channels []io.ReadWriter, st *packstore.Store, root key.Key, jobs int, prog Progress) (Stats, error) {
	return wantsync.Receive(channels, st, root, jobs, prog)
}

// Wants is wantsync.Wants.
func Wants(st *packstore.Store, frontier []key.Key, jobs int) ([]key.Key, error) {
	return wantsync.Wants(st, frontier, jobs)
}

// New is server.New.
func New(log *slog.Logger, objects *packstore.Store, refs *refstore.Store) *Server {
	return server.New(log, objects, refs)
}

// FromFlag is relaymode.FromFlag.
func FromFlag(url string) (relay.Mode, error) { return relaymode.FromFlag(url) }
```

- [ ] **Step 4: Run the tests**

Run: `cd $T && gofmt -l amberiroh; go vet ./amberiroh/ && go test ./amberiroh/`
Expected: no gofmt output; `ok`.

- [ ] **Step 5: Document it in the README**

In `README.md`, replace lines 70–71

```
- Module: `github.com/amber-store/transport-iroh` (fetching the
  private core module needs `GOPRIVATE=github.com/jobs-build/*`).
```

with

```
- Module: `github.com/amber-store/transport-iroh`. Library consumers can
  import the single facade package `amberiroh`, which re-exports
  `protocol`, `wantsync`, `server` and `relaymode`; the CLIs use the
  four packages directly.
```

- [ ] **Step 6: Commit**

```bash
cd $T && git add amberiroh README.md && git commit -m "amberiroh: single-import facade over protocol, wantsync, server, relaymode

Lets jobs-iroh import one path in place of its vendored copy. Aliases,
re-declared constants and thin wrappers only; the test pins the surface.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01GAJmL5jzzyWvFiikdwFCEn"
```

---

### Task 8: PR, merge, release v0.2.0

**Files:** none new (the plan file itself is committed here).

- [ ] **Step 1: Commit the plan and verify the branch**

```bash
cd $T && git add docs/superpowers/plans/2026-09-07-devendor-jobs-iroh.md && git commit -m "docs: implementation plan for de-vendoring jobs-iroh

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01GAJmL5jzzyWvFiikdwFCEn" ; gofmt -l $(git ls-files '*.go'); go build ./... && go vet ./... && go test ./... && git status --short
```

Expected: nothing from gofmt, all `ok`, clean tree.

- [ ] **Step 2: Push and open the PR**

```bash
cd $T && git push -u origin backport-parity && gh pr create --repo amber-store/transport-iroh --title "Parity with jobs-iroh's vendored copy, single-import facade" --body "$(cat <<'EOF'
Ports the seven commits jobs-iroh made to its vendored `amberiroh` copy since 2026-07-27 back into `protocol`, `wantsync` and `server`, with their tests: `DataEndpoints`/`Names` wire fields and `TPin` (additive, compat-tested), full server-side stream retirement (`CancelRead`), the 10s attach window, throughput-weighted want dealing, and the `SetOnAccess`/`SetOnPin`/`SetRefGuard` GC hooks. Bumps go-iroh to v0.1.0, the version jobs-iroh already selects. Adds an `amberiroh` facade package re-exporting the four packages so jobs-iroh can import one path and delete its copy.

Design: `docs/superpowers/specs/2026-09-07-devendor-jobs-iroh-design.md`. The ALPN and all existing behavior of the `amber`/`amber-serve` CLIs are unchanged.

Verified with `gofmt -l`, `go build ./...`, `go vet ./...`, `go test ./...`.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01GAJmL5jzzyWvFiikdwFCEn
EOF
)"
```

- [ ] **Step 3: Merge and tag**

```bash
cd $T && N=$(gh pr list --repo amber-store/transport-iroh --head backport-parity --json number --jq '.[0].number') && gh pr merge "$N" --repo amber-store/transport-iroh --merge --delete-branch && git fetch -q --prune origin && git checkout -q main && git reset -q --hard origin/main && gh release create v0.2.0 --target main --repo amber-store/transport-iroh --title "v0.2.0" --notes "$(cat <<'EOF'
## What's Changed
* Parity with the changes jobs-iroh made to its vendored copy since July: `DataEndpoints` records on `TAccept`/`TRef` so sharding clients can hole-punch to data endpoints (additive CBOR field 16; old peers ignore it), a 10s attach gather window, full server-side stream retirement with `CancelRead` (fixes pooled clients starving after exactly 100 attaches), and throughput-weighted want dealing in `wantsync.Receive`.
* GC hooks on `server.Server`: `SetOnAccess`, `SetOnPin`, `SetRefGuard`, and a `TPin` frame (type 13, `Names` field 17). All no-ops when unset; `*gc.Collector` from amber-store-core satisfies `RefGuard`.
* New `amberiroh` package: a single-import facade re-exporting `protocol`, `wantsync`, `server` and `relaymode`.
* go-iroh bumped to v0.1.0.

The wire ALPN `amber-store-iroh/1` and the `amber`/`amber-serve` CLIs are unchanged.

**Full Changelog**: https://github.com/amber-store/transport-iroh/compare/v0.1.0...v0.2.0
EOF
)" && git fetch -q --tags && git rev-parse --short v0.2.0
```

- [ ] **Step 4: Confirm the module resolves**

Run: `cd /private/tmp && GOFLAGS=-mod=mod go list -m github.com/amber-store/transport-iroh@v0.2.0`
Expected: `github.com/amber-store/transport-iroh v0.2.0`.

---

## Phase 2: jobs-iroh

### Task 9: Replace the vendored package with the dependency

**Files:**
- Delete: `$J/amberiroh/` (12 files)
- Modify: `$J/go.mod`, `$J/go.sum`
- Modify (import path only): `$J/amberclient/client.go`, `$J/amberclient/pool.go`, `$J/amberclient/pool_internal_test.go`, `$J/amberclient/shard.go`, `$J/amberclient/shardtarget.go`, `$J/amberclient/shardtarget_internal_test.go`, `$J/registryd/pins.go`, `$J/registryd/pins_test.go`, `$J/registryd/sync.go`, `$J/runnerd/sync.go`, `$J/serve/announce.go`, `$J/serve/serve.go`, `$J/serve/serve_test.go`

**Interfaces:**
- Consumes: `github.com/amber-store/transport-iroh/amberiroh` v0.2.0 (Task 7/8), which exposes every identifier the 83 call sites use.

- [ ] **Step 1: Branch and remove the copy**

```bash
cd $J && git status --short && git checkout -b devendor-amberiroh && git rm -r -q amberiroh && ls amberiroh 2>&1 | head -1
```

Expected: empty status before branching; `ls` reports no such directory.

- [ ] **Step 2: Rewrite the import path**

```bash
cd $J && grep -rl '"github.com/jobs-build/jobs-iroh/amberiroh"' --include='*.go' . | xargs sed -i 's#"github.com/jobs-build/jobs-iroh/amberiroh"#"github.com/amber-store/transport-iroh/amberiroh"#' && grep -rn 'jobs-iroh/amberiroh"' --include='*.go' . | wc -l
```

Expected: `0`.

- [ ] **Step 3: Add the dependency and tidy**

```bash
cd $J && go get github.com/amber-store/transport-iroh@v0.2.0 && go mod tidy && grep -n 'transport-iroh\|amber-store/core\|go-iroh' go.mod
```

Expected: `github.com/amber-store/transport-iroh v0.2.0` and `github.com/amber-store/core v0.0.4` in the direct block; `github.com/tmc/go-iroh v0.1.0` unchanged.

- [ ] **Step 4: gofmt and verify**

```bash
cd $J && f=$(gofmt -l $(git ls-files '*.go')); [ -n "$f" ] && gofmt -w $f; gofmt -l $(git ls-files '*.go'); go build ./... && go vet ./... && CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go vet ./... && go test ./... 2>&1 | tail -40
```

Expected: no gofmt output after the rewrite; build and both vets clean; every package `ok` (the amberclient e2e tests drive the real server through the facade).

- [ ] **Step 5: Commit**

```bash
cd $J && git add -A && git commit -m "Import transport-iroh's amberiroh facade; drop the vendored copy

Everything the copy grew since 2026-07-27 is in transport-iroh v0.2.0,
so the 83 call sites change only in import path. Behavior, wire format
and ALPNs are unchanged.

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01GAJmL5jzzyWvFiikdwFCEn"
```

---

### Task 10: CLAUDE.md, PR, merge

**Files:**
- Modify: `$J/CLAUDE.md:126, 271`

- [ ] **Step 1: Rewrite the package-table row**

Replace the whole line 126 (it begins with ``| `amberiroh/` | Store sync over iroh QUIC — **vendored** ``) with:

```
| `amberiroh` (external) | Store sync over iroh QUIC, imported from **`github.com/amber-store/transport-iroh/amberiroh`** — the facade over transport-iroh's `protocol`, `wantsync`, `server` and `relaymode`. This repo carried a vendored copy from 2026-07-27 to 2026-09-07; it is gone and must not come back: protocol or server changes land upstream first, then the pin here is bumped. Wire protocol (length-prefixed CBOR frames, amberpack payloads chunked into `TData`), the have/want transfer loop, and the `Server` that `serve/` mounts on `jobs-runner-amber/1.0` + `jobs-amber-admin/1.0` through `HandleStream` — the router owns dispatch. `ALPN` (`amber-store-iroh/1`) is a **wire constant**: renaming it breaks every peer. TAccept/TRef advertise per-endpoint `DataEndpoints` records (identity + candidates); their presence signals the 10s attach gather window. TPin pin-asserts + OnAccess/OnPin/RefGuard hooks for the GC tracker/collector. |
```

- [ ] **Step 2: Fix the GC gotcha**

On line 271, replace `` `amberiroh.handlePush` `` with `` transport-iroh's `server.handlePush` `` so the sentence reads:

```
  guard (`amber.PutRef` + transport-iroh's `server.handlePush` — a new PUT path MUST take
```

Check: `grep -n 'amberiroh' CLAUDE.md` shows only lines 126 and 127 (the `amberclient/` row's "over `amberiroh`" is still accurate).

- [ ] **Step 3: Commit, push, PR**

```bash
cd $J && git add CLAUDE.md && git commit -m "docs: amberiroh is a dependency now, not a vendored package

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01GAJmL5jzzyWvFiikdwFCEn" && git push -u origin devendor-amberiroh && gh pr create --repo jobs-build/jobs-iroh --title "Import transport-iroh's amberiroh facade; drop the vendored copy" --body "$(cat <<'EOF'
Deletes `amberiroh/` and imports `github.com/amber-store/transport-iroh/amberiroh` v0.2.0 instead. Everything the vendored copy grew since 2026-07-27 — `DataEndpoints` advertising, server-side stream retirement, throughput-weighted want dealing, `TPin` and the GC hooks — was ported upstream first (amber-store/transport-iroh v0.2.0), so the 83 call sites change only in import path. Behavior, wire format and ALPNs are unchanged. CLAUDE.md now describes the dependency and the rule that protocol/server changes go upstream first.

Verified with `gofmt -l`, `go build ./...`, `go vet ./...`, `go test ./...`, and the Linux cross-vet.

🤖 Generated with [Claude Code](https://claude.com/claude-code)

https://claude.ai/code/session_01GAJmL5jzzyWvFiikdwFCEn
EOF
)"
```

- [ ] **Step 4: Merge and return to main**

```bash
cd $J && N=$(gh pr list --repo jobs-build/jobs-iroh --head devendor-amberiroh --json number --jq '.[0].number') && gh pr merge "$N" --repo jobs-build/jobs-iroh --merge --delete-branch && git checkout -q main && git pull -q --ff-only && git fetch -q --prune && git log --oneline -1
```

Expected: `Merge pull request #<N> from jobs-build/devendor-amberiroh`.

---

### Task 11: Release jobs-iroh v0.32.0 with images

**Files:**
- Modify: `$J/version/version.go`, `$J/CHANGELOG.md`

- [ ] **Step 1: Bump the version and add the changelog entry**

Edit `version/version.go`: `const Version = "0.31.1"` → `const Version = "0.32.0"`.

Insert at the top of `CHANGELOG.md`, directly after the `# Changelog` heading and its blank line:

```
## v0.32.0 — 2026-09-07

- **`amberiroh/` de-vendored.** The store-sync transport is imported from
  `github.com/amber-store/transport-iroh/amberiroh` (v0.2.0) again instead
  of the copy this repo carried since 2026-07-27. Everything that copy grew
  — `DataEndpoints` advertising, server-side stream retirement,
  throughput-weighted want dealing, TPin and the GC hooks — was ported
  upstream first, so behavior, wire format and ALPNs are unchanged. Future
  protocol or server changes land upstream and arrive here as a version
  bump.

```

- [ ] **Step 2: Release commit, tag, push, GitHub release**

```bash
cd $J && go build ./... && git add version/version.go CHANGELOG.md && git commit -m "Release v0.32.0: amberiroh de-vendored onto transport-iroh v0.2.0

Co-Authored-By: Claude Fable 5.1 <noreply@anthropic.com>
Claude-Session: https://claude.ai/code/session_01GAJmL5jzzyWvFiikdwFCEn" && git tag v0.32.0 && git push origin main && git push origin v0.32.0 && gh release create v0.32.0 --verify-tag --repo jobs-build/jobs-iroh --title "v0.32.0 — amberiroh de-vendored onto transport-iroh v0.2.0" --notes "$(cat <<'EOF'
The store-sync transport is a dependency again. `amberiroh/`, vendored from amber-store-iroh on 2026-07-27, is deleted; jobs-iroh imports `github.com/amber-store/transport-iroh/amberiroh` v0.2.0 instead. Everything the vendored copy grew — `DataEndpoints` advertising, server-side stream retirement, throughput-weighted want dealing, `TPin` and the GC hooks — was ported upstream first (amber-store/transport-iroh v0.2.0).

## Compatibility

No functional change — no wire, API, or ALPN changes. Any mix of old and new servers, runners, registries and clients interoperates as before.
EOF
)"
```

- [ ] **Step 3: Build and push the images**

Precondition: `git status --porcelain` is empty and `git describe --tags --exact-match` prints `v0.32.0`.

```bash
cd $J && bash scripts/release-images.sh 0.32.0 2>&1 | grep -vE '^#[0-9]+ |^ => |DONE|CACHED|transferring|exporting|resolve |^$'
```

Expected: three `== pushing dmilhdef/...:v0.32.0 (+latest)` blocks each followed by two `Platform:` lines, then `== done: v0.32.0`. (Run outside the sandbox: the keychain credential helper is not reachable from inside it.)

- [ ] **Step 4: Verify the images**

```bash
for img in jobs-iroh-server jobs-iroh-runner jobs-registry; do for tag in v0.32.0 latest; do out=$(docker buildx imagetools inspect dmilhdef/$img:$tag 2>&1); echo "$img:$tag platforms=$(echo "$out" | grep -c 'Platform:') unknown=$(echo "$out" | grep -c 'unknown/unknown') manifest=$(echo "$out" | grep -m1 'Digest:' | awk '{print substr($2,8,12)}')"; done; done
```

Expected: every line `platforms=2 unknown=0`, and each image's `latest` manifest digest equals its `v0.32.0` digest. Confirm `cd $J && git status --porcelain` is still empty (the script's trap removed the binaries).
