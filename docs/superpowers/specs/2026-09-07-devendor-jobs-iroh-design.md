# De-vendoring jobs-iroh: transport-iroh parity and facade

**Date:** 2026-09-07
**Repos:** `amber-store/transport-iroh` (phase 1), `jobs-build/jobs-iroh` (phase 2)

## Background

On 2026-07-27 jobs-iroh vendored transport-iroh's `protocol`, `wantsync`,
`server` and `relaymode` packages into a single package `amberiroh` and
dropped the module dependency (jobs-iroh commit `22368aa`). Since then all
development happened on the vendored copy: seven commits, about 280 changed
lines plus roughly 300 lines of tests. transport-iroh received no functional
change in that period; its only commits are the two module moves.

The vendored copy's changes fall into three groups:

- **Transport fixes.** A `DataEndpoints` wire field so sharding clients can
  hole-punch to data endpoints instead of direct-dialing UDP ports, with the
  server's attach gather window raised from 5s to 10s. A `closeStream`
  helper that sends STOP_SENDING on every server-side close path, fixing a
  field bug where a pooled client starved after exactly 100 attaches (the
  quic-go initial stream budget was never returned). Throughput-weighted
  want dealing (`dealWants`) so one relayed shard no longer paces every
  round.
- **GC hooks.** A `TPin` message with a `Names` field, and `SetOnAccess`,
  `SetOnPin`, `SetRefGuard` on the server. All are no-ops when unset;
  `*gc.Collector` from amber-store-core satisfies `RefGuard` directly.
- **jobs-iroh-only edits.** Removing `Serve`, `serveConn` and `SendPack`
  (jobs-iroh's `serve` package owns its accept loop) and the package docs.

jobs-iroh reaches `amberiroh` from four packages (amberclient, serve,
registryd, runnerd) at 83 call sites using 29 distinct exported
identifiers. assimilate and testing, the two downstream consumers of
jobs-iroh, never reference it.

## Goal

transport-iroh is again the single authoritative implementation of the
amber sync protocol. jobs-iroh deletes `amberiroh/` and imports
transport-iroh, and cannot drift from it again.

## Non-goals

- Porting jobs-iroh's `amberclient` (connection pool, punch-capable shard
  dials, reserve-first sharding) into the `amber` CLI. The CLI keeps
  direct-dialing `DataPorts`.
- Wiring the GC hooks into `amber-serve`. It runs no collector.
- Rewriting jobs-iroh's dated design docs, plans and research notes that
  describe the vendored copy. They are history.
- Changing the wire ALPN `amber-store-iroh/1`.

## Phase 1: transport-iroh parity and facade (release v0.2.0)

One PR on branch `backport-parity`, three parts.

### 1a. Parity

Port the vendored changes into the existing packages, by diff against
`jobs-iroh/amberiroh` at commit `b37082d`, not by hand.

`protocol`:
- `type DataEndpointRec struct { ID []byte; Addrs []string }` with the
  vendored CBOR tags (`0`, `1`).
- `Msg.DataEndpoints []DataEndpointRec` at CBOR key 16 and
  `Msg.Names []string` at key 17, both `omitempty`.
- `TPin = 13`.

`server`:
- `closeStream(rw io.Closer)`: `Close` then `CancelRead(0)` when the stream
  supports it. Used on every close path: `transfers.drop`, `HandleStream`
  (early error, unknown token, deferred close), and the extras released by
  `shardChannels` and `handlePull`.
- `attachWait` default 5s → 10s.
- Fields `dataEndpoints func() []DataEndpointRec`, `onAccess`, `onPin
  func(string)`, `guard RefGuard`, with setters `SetDataEndpoints`,
  `SetOnAccess`, `SetOnPin`, `SetRefGuard`. All "call before Serve".
- `type RefGuard interface { PrepareRef(root key.Key) (commit, abort
  func(), err error) }`.
- `TAccept` and sharded `TRef` carry `DataEndpoints` when the snapshot is
  set.
- `handlePin`: for each name, `refs.Get`; `ErrNotFound` is skipped, any
  other error fails the request with `CodeInternal`; existing names fire
  `onPin`; answer `TOK`.
- `handlePush`: when `guard` is set, bracket `refs.Put` with
  `PrepareRef(root)` (`abort` on Put failure, `commit` on success). After a
  successful put, and after `handlePull` resolves a ref, fire `onAccess`.
- `Serve`, `serveConn` and `protocol.SendPack` stay.

`wantsync`:
- `dealWants(wants []key.Key, weights []int64) [][]key.Key`: proportional
  greedy dealing by embedded key length, weights floored to 1, ties to the
  lower index, single-channel fast path.
- `Receive` samples each channel's wire bytes per round (`crs[i].n` deltas)
  and deals the next round with `dealWants` instead of `shardWants`.
  `shardWants` stays: `loop_test.go` still exercises it directly.

Tests, all internal (`package protocol` etc.), copied from the vendored
files:
- `protocol/protocol_test.go`: `TestMsgDataEndpointsRoundTrip`,
  `TestMsgDataEndpointsCompat` (uses the existing `encMode`).
- `wantsync/wantsync_test.go`: `TestDealWantsSingleChannel`,
  `TestDealWantsCoversAllKeysOnce`, `TestDealWantsProportionalToWeights`,
  `TestDealWantsZeroWeightsFallBackToEven`.
- `server/server_test.go`: `TestAttachWaitDefaultCoversPunching`,
  `TestShardChannelsAdvertisesDataEndpoints`, plus the small edits the
  vendored copy made to existing tests.
- `server/server_gc_test.go` (new): `TestPinAssert`,
  `TestPinWithoutHookStillOK`, `TestPullFiresOnAccess`, with the
  `recordingGuard` helper.
- `protocol/pack_test.go` and `wantsync/loop_test.go`: apply the vendored
  copy's small edits where they are not artifacts of the single-package
  layout.

### 1b. go-iroh bump

Bump `github.com/tmc/go-iroh` from the July pseudo-version to `v0.1.0`,
the version jobs-iroh uses. Once jobs-iroh imports transport-iroh, minimum
version selection picks v0.1.0 regardless, so upstream must be tested on
it. A throwaway check on 2026-09-07 built and passed all packages on
v0.1.0.

### 1c. Facade package `amberiroh`

`github.com/amber-store/transport-iroh/amberiroh` re-exports the complete
exported surface of the four packages so a consumer can import one path.
jobs-iroh's call sites then change only in import path.

- Types as aliases: `Msg`, `RefInfo`, `RemoteError`, `DataEndpointRec`
  (protocol); `Stats`, `Progress` (wantsync); `Server`, `RefGuard`
  (server). Methods travel with the alias.
- Constants re-declared with the same values: `ALPN`, `MaxFrame`,
  `ChunkSize`; `TPush`, `TPull`, `TRefList`, `TRef`, `TRefs`, `TWants`,
  `TData`, `TDataEnd`, `TOK`, `TErr`, `TAttach`, `TAccept`, `TPin`;
  `CodeCASMismatch`, `CodeUnknownRef`, `CodeBadRequest`, `CodeInternal`.
- `var ErrProtocol = protocol.ErrProtocol`, so `errors.Is` matches across
  the facade and the underlying package.
- Wrapper functions with the same signatures: `WriteMsg`, `ReadMsg`,
  `RemoteFromMsg`, `NewPackReader`, `SendPack`, `SendPackRecords`
  (protocol); `Send`, `Receive`, `Wants` (wantsync); `New` (server);
  `FromFlag` (relaymode).
- The package doc states that it is a re-export umbrella, that the four
  packages remain the implementation, and that new exported API must be
  mirrored here.
- `amberiroh/amberiroh_test.go`: an external test (`package
  amberiroh_test`) that round-trips a `Msg` with `DataEndpoints` through
  `WriteMsg`/`ReadMsg`, constructs a `Server` with `New` and calls
  `SetDataEndpoints`, and checks `errors.Is(ErrProtocol,
  protocol.ErrProtocol)`. Its purpose is to make a missing re-export a
  compile failure in this repo rather than in a consumer.

### 1d. Release

Merge, tag `v0.2.0`, GitHub release. Notes call out the additive wire
fields, the new server hooks, the go-iroh bump, and the facade.

## Phase 2: jobs-iroh de-vendoring (release v0.32.0)

One PR on branch `devendor-amberiroh`.

- `git rm -r amberiroh/`.
- `go get github.com/amber-store/transport-iroh@v0.2.0`, `go mod tidy`.
- In the 13 importing files (amberclient, registryd, runnerd, serve), replace
  `"github.com/jobs-build/jobs-iroh/amberiroh"` with
  `"github.com/amber-store/transport-iroh/amberiroh"`. The `amberiroh.`
  qualifiers stay. gofmt afterwards; the new path sorts differently.
- CLAUDE.md: rewrite the `amberiroh/` row of the package table to describe
  the dependency on transport-iroh's facade, remove the "this copy is
  authoritative and the two can drift silently" sentence, and change the GC
  gotcha that names `amberiroh.handlePush` to name transport-iroh's
  `server.handlePush`. The `amberclient/` row is unchanged.
- Verification: `gofmt -l`, `go build ./...`, `go vet ./...`, `go test
  ./...`, and `CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go vet ./...`. The
  amberclient e2e tests (`pool_e2e_internal_test.go`,
  `starve_e2e_internal_test.go`) drive the real server and cover the swap
  end to end.
- Release per jobs-iroh's CLAUDE.md: bump `version/version.go` to 0.32.0,
  CHANGELOG entry, `Release v0.32.0: ...` commit, tag, GitHub release,
  `scripts/release-images.sh 0.32.0` from the clean tag checkout, verify
  each image shows two platforms and no `unknown/unknown` rows.

assimilate and testing are untouched; their jobs-iroh pins stay at v0.31.1.

## Compatibility and risk

- Wire changes are additive. `TestMsgDataEndpointsCompat` shows a peer
  decoding with the old `Msg` shape ignores the new fields; an old server
  answers `TPin` with `TErr`/`CodeBadRequest`, which clients already treat
  as "pin unsupported".
- jobs-iroh serves the same `Server` on its own ALPNs through
  `HandleStream`; dispatch does not move.
- The one real risk is a silent behavioral difference between the vendored
  copy and the port. Mitigations: port by diff; keep the vendored tests
  verbatim; run jobs-iroh's whole suite on the result before tagging
  anything.
- Rollback: jobs-iroh can pin the previous tag; transport-iroh v0.2.0 is
  additive over v0.1.0.

## Order of operations

1. Phase 1 PR → merge → `v0.2.0`.
2. Phase 2 PR pinned to `v0.2.0` → merge → `v0.32.0` + images.
