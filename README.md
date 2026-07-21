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

`--addr host:port` (repeatable, hostnames allowed) dials the server
directly, skipping discovery and relays — useful on a LAN and used by the
offline e2e tests. Without it, clients resolve the server via mDNS on the
local link first, then pkarr/DNS; the server advertises its interface
addresses both ways, so same-LAN transfers go direct rather than through
a relay. `--relay URL` pins the fallback relay on either side.

Throughput note: the current go-iroh transport tops out around 16 MB/s
per connection even on loopback (parallel streams do not lift it); direct
paths hit that ceiling, relayed paths are far slower.

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
