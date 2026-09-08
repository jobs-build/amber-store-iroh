# Amber-Store Iroh

A peer-to-peer distributed layer over
[amber-store-core](https://github.com/amber-store/core):
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
amber --store ./st pull --server ENDPOINT_ID snap  # both show a progress bar (--no-progress to disable)
amber refs --server ENDPOINT_ID                    # list remote refs
amber --store ./st ls ref:snap                     # local commands work offline
amber --store ./st restore ref:snap ./dest
```

`--addr host:port` (repeatable, hostnames allowed) dials the server
directly, skipping discovery and relays — useful on a LAN and used by the
offline e2e tests. Without it, clients dial the union of all resolver
candidates (mDNS on the local link, pkarr, DNS), keeping the relay as
fallback; the server advertises its interface addresses both ways
(skipping down interfaces and container bridges — every unreachable
advertised address costs connecting peers handshake budget), so same-LAN
transfers go direct rather than through a relay. `--advertise-addr
ip[:port]` overrides auto-detection; `--relay URL` pins the fallback
relay on either side.

Throughput notes:

- Records travel disk-to-wire verbatim (already zstd-compressed in the
  packstore; the sender never decompresses or re-encodes). The progress
  bar shows both content and wire rates.
- On Linux, raise the kernel UDP buffers or QUIC throughput suffers and
  quic-go prints a receive-buffer warning:
  `sysctl -w net.core.rmem_max=8388608 net.core.wmem_max=8388608`
- One go-iroh endpoint receives about 350 MB/s over loopback with
  go-iroh v0.2.0 (Apple M4 Pro; measured 2026-09-08) and the full
  107 MB/s line rate of a 1 Gbit WAN path at 43 ms, and neither
  parallel streams on one connection nor parallel client sockets into
  one endpoint raise that ceiling — only more *receiving* endpoints do.
  Sharded transfers, `--conns N` on push/pull (default 4) paired with
  the server's dedicated data endpoints (`--data-endpoints`, default 3),
  therefore pay off only on links faster than one endpoint's ceiling;
  on a 1 Gbit path a single connection is enough. Old peers
  interoperate — the transfer just stays single-connection.

## Development

```sh
direnv allow          # or: nix develop
nix develop -c go build ./...
nix develop -c go test ./...
```

The wire protocol (CBOR frames, have/want rounds, chunked amberpack
payloads) is specified in
[`docs/superpowers/specs/2026-07-21-amber-store-iroh-design.md`](docs/superpowers/specs/2026-07-21-amber-store-iroh-design.md).

- Module: `github.com/amber-store/transport-iroh`. Library consumers can
  import the single facade package `amberiroh`, which re-exports
  `protocol`, `wantsync`, `server` and `relaymode`; the CLIs use the
  four packages directly.
- Go: 1.26+
