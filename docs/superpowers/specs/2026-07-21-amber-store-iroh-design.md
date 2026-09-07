# Amber-Store Iroh — P2P Distributed Amber-Store

**Date:** 2026-07-21
**Status:** Approved design

## Purpose

A peer-to-peer distributed layer over
[amber-store-core](https://github.com/amber-store/core): a
server that hosts an amber store reachable over
[iroh](https://iroh.computer) QUIC (via `github.com/tmc/go-iroh`), and a
client CLI that owns a local copy of the store, imports directories into it,
and pushes/pulls refs (with their reachable objects) to/from the server.

Decisions fixed during brainstorming:

- **Sync protocol:** have/want negotiation — only objects the receiver is
  missing cross the wire.
- **Access:** open — anyone who knows the server's endpoint ID can push and
  pull. No authentication or authorization.
- **Ref updates:** compare-and-swap on push, with `--force` escape hatch.
- **Binaries:** two — `cmd/amber-serve` and `cmd/amber` (like irohese's
  server/client split).
- **Core dependency:** plain `require github.com/amber-store/core`
  fetched from GitHub (the core repo must be pushed; version pinned by
  tag or pseudo-version).
- **Client scope:** full local toolkit — the local copy is fully usable
  offline.
- **Protocol shape:** one bidirectional QUIC stream per operation,
  deterministic-CBOR message frames, amberpack payloads inline on the same
  stream.

## Architecture

```
cmd/amber-serve        cmd/amber (client)
      │                     │
      ├── protocol ─────────┤        (wire message types + CBOR codec)
      ├── sync ─────────────┤        (want-loop: server & client halves)
      │                     │
  amber-store-core: packstore, refstore, reference, fstree,
                    ingest, amberpack, tarexport, tarextract
      │                     │
  go-iroh: endpoint bind, pkarr publish/resolve, QUIC streams
```

### `cmd/amber-serve`

- Owns a store directory (`--store` / `$AMBER_STORE`), conventional layout:
  `packstore/` for objects, `refs/` for the refstore. Store directories are single-owner; only the server
  process opens this one.
- Persists its iroh identity hex-encoded in a key file (`--key`, default
  `server.key`, generated on first run — same pattern as irohese). Deleting
  the file changes the server's endpoint ID.
- Binds an iroh endpoint with ALPN `amber-store-iroh/1`, relays enabled
  (nearest built-in relay preferred via a bounded latency probe; `--relay
  URL` overrides), and logs its endpoint ID on startup.
- Advertises direct addresses so peers can dial without the relay:
  `--advertise-addr ip[:port]` verbatim when given, otherwise the
  machine's unicast interface addresses on the bound port — excluding
  wildcard, loopback, link-local, down interfaces, and container bridges
  (docker/cni/flannel/veth/…), whose unreachable addresses cost
  connecting peers handshake budget. Published to n0's pkarr relay with a
  filter that keeps direct addresses (the library default strips them),
  and advertised on the local link over mDNS for same-LAN clients.
- One goroutine per connection; one operation per accepted stream. No actor
  system (irohese's goakt usage was an experiment there; it adds nothing
  here).
- Ref updates are serialized per ref name (per-name lock) so
  compare-and-swap is race-free under concurrent pushes. Object writes rely
  on packstore's parallel, deduplicating writers.
- Every committed push is logged with the ref name, client endpoint ID,
  objects offered (whole tree) vs actually transferred, wire bytes,
  duration, and throughput.
- Shutdown on SIGINT/SIGTERM: stop accepting and close open connections
  immediately — interrupted transfers are resumable by design — then wait
  a bounded grace for handler cleanup.

### `cmd/amber` (client)

- Owns its own local store directory in the same layout.
- Local commands reuse amber-store-core packages directly and work
  offline: `import` (ingest), `ls`, `export`, `restore`, `ref
  list|get|set|rm` — mirroring the amber-store-core CLI.
- Network commands (`push`, `pull`, `refs`) take `--server <endpoint-id>`,
  resolve it via mDNS, pkarr, and DNS — the union of every resolver's
  candidates is dialed, so the relay stays available as fallback when a
  direct candidate is unreachable — and dial with an **ephemeral** identity — access is open, so the client
  needs no stable key file. `--addr host:port` (hostnames resolve;
  repeatable) skips discovery and relays entirely; `--relay URL` pins the
  fallback relay.

### Remote-tracking refs

To give push CAS git-like semantics, the client records the last-seen server
value of each ref in its own refstore under a reserved namespace:

```
remotes/<server-endpoint-id>/<name>
```

- `push NAME` sends the tracking ref's key as `expectedOld` (absent if no
  tracking ref exists — meaning "create new").
- On CAS rejection the client reports the server's current key and suggests
  `pull` first or `--force` (which omits the precondition).
- Successful push and pull both update the tracking ref.
- The `remotes/` prefix is reserved: `ref list` hides it, and `ref set`
  refuses to write under it.

Local and remote ref names map name-for-name; there is no refspec
translation.

## Wire protocol

One bidirectional QUIC stream per operation. Frames are length-prefixed
deterministic-CBOR messages (encoded with `cborx` conventions; determinism
is not load-bearing on the wire but keeps one encoding style everywhere).
Amberpack bytes are embedded as a sized binary payload following the frame
that announces them. As in irohese: the initiator writes first (a QUIC
stream is invisible to the acceptor until data flows), and the responder
FIN-closes only after the exchange completes.

### Operations

**`ref-list`**
Request `{op: "ref-list"}` → response: array of `{name, key, created,
user}` from the server's refstore (tracking-namespace concept doesn't exist
server-side; everything is listed).

**`push`**
Request `{op: "push", name, root, expectedOld?}`.

1. Server checks the CAS precondition against its refstore. Mismatch →
   immediate `{error: {code: "cas-mismatch", current}}`; the operation
   ends. (`--force` pushes omit `expectedOld` entirely, skipping the check.)
2. **Server-driven want loop**, top-down from `root`. The server keeps a
   frontier of keys, initially `{root}`. For each frontier key:
   - If the store has the object **and** fstree's completeness walk
     confirms the whole subtree is present → prune (local reads only).
   - Otherwise → add to the want-list.

   Mere key presence is not enough to prune: an interrupted previous push
   stores parents before children, so a present parent may sit above
   missing descendants. The completeness check is what makes resuming
   correct.
3. Server sends `{wants: [key, ...]}`. If empty → done: server re-checks
   CAS under the per-name lock, commits the ref, replies `{ok, key}`.
4. Client reads the wanted objects from its local store and streams them
   as one amberpack payload. Server verifies each received object against its key and writes it straight into the packstore (parallel verified writes; durability via synced writes), decodes the received tree objects, and
   their children become the next frontier. Loop to 3.

Round trips are O(tree depth) ≈ O(log n); only missing objects cross the
wire. Interrupted pushes are safe: received objects are fsynced and
deduplicated, so rerunning resumes — previously transferred complete
subtrees prune out.

**`pull`**
Request `{op: "pull", name}` → server responds with the ref record (name,
key, and signature fields carried opaquely per the `reference` package).
Then the same want loop runs with roles swapped: the client walks top-down
from the received root against its local store (same
presence + completeness pruning rule), sends `{wants: [...]}`, and the
server responds with amberpack payloads until the client sends an empty
want-list. The client then sets the local ref and the remote-tracking ref.
The local ref update is unconditional — the client store is single-owner,
so there is no local race.

### Sharded transfers

One endpoint's UDP socket loop caps throughput well below a fast link,
so a transfer can spread across parallel connections on separate
sockets. The client requests it with `dataConns` in the push/pull
request; a sharding-aware server replies with a transfer `token` (in
`TAccept` for pushes, inside the ref frame for pulls) plus the UDP ports
of its dedicated data endpoints. The client opens each extra connection
on its own endpoint — targeting the advertised ports on the address the
control connection reached, falling back to the control candidates —
and attaches its stream with `TAttach{token}`; the server routes
attaching streams to the in-progress transfer and proceeds leniently
with whatever attached within a bounded wait.

Each round's wants are dealt round-robin across the channels; a channel
whose shard is empty gets no frame that round (an empty want list would
terminate its sender), and the final empty want list goes to every
channel. Compatibility is by omission: an old server ignores `dataConns`
and opens with wants directly (the client replays the consumed frame and
stays single-channel); an old client never sets `dataConns` and sees the
pre-sharding exchange byte-for-byte.

### Errors

Any failure is reported as an `{error: {code, message, ...}}` frame followed
by stream close. Both sides run under contexts wired to signal handling and
per-operation deadlines. Error codes: `cas-mismatch`, `unknown-ref`,
`bad-request`, `internal`.

## CLI surface

```sh
# server
amber-serve --store ./srv-store --key server.key     # prints endpoint ID

# client — local (offline) commands, mirroring amber-store-core
amber --store ./st import [--ref NAME] ./dir         # ingest; prints root key
amber --store ./st ls KEY[/PATH]                     # ref:NAME[@PATH] also works
amber --store ./st export ref:NAME -o tree.tar
amber --store ./st restore ref:NAME ./dest
amber --store ./st ref list|get|set|rm ...

# client — network commands
amber --store ./st push --server ID NAME [--force] [--conns N]
amber --store ./st pull --server ID NAME [--conns N]
amber refs --server ID                               # list remote refs
```

`--store` defaults from `$AMBER_STORE`. Ingest tuning flags (`--jobs`,
chunker parameters) carry over from the amber-store-core CLI where they
apply to `import`.

## Error handling (user-facing)

- CAS rejection: print the remote's current key; suggest `pull` or
  `--force`. Non-zero exit.
- Push of a nonexistent local ref, pull of an unknown remote ref,
  unresolvable/unreachable server, mid-transfer disconnect: one-line error,
  non-zero exit.
- Interrupted transfers require no repair; rerun the command.

## Testing

- **Unit — protocol codec:** round-trip encode/decode of every message
  type, including error frames and payload framing.
- **Unit — want loop:** both halves against temp-dir stores. Must cover
  the partial-subtree case: parent object present, children missing →
  completeness check forces descent instead of pruning.
- **End-to-end (offline):** bind a real server endpoint and client
  endpoint in-process and connect **by direct address** — pass
  `ep.Addr()` straight to the client, no pkarr/DNS — so tests never touch
  n0's public infrastructure. Scenarios:
  - import → push → pull into a second store → restore → byte-compare
    trees;
  - push to an existing ref with stale `expectedOld` → cas-mismatch;
  - interrupt a push mid-loop, re-push, verify completion and that
    already-sent objects are not re-sent (assert via want-list contents).
- Run with `nix develop -c go test ./...`, as in the sibling repos.

## Non-goals

- Authentication, authorization, signed-ref verification (signature fields
  are carried opaquely, never checked).
- Refspec mapping between local and remote names.
- Multi-server remotes configuration; `--server` is passed per invocation.
- Garbage collection / ref deletion propagation.
