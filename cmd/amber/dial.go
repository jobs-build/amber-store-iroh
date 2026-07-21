package main

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"time"

	"github.com/fables-for-robots/amber-store-iroh/protocol"
	"github.com/fables-for-robots/amber-store-iroh/relaymode"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/iroh/mdns"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/urfave/cli/v2"
)

// serverFlags returns the flags shared by every network command.
func serverFlags(server *string, addrs *cli.StringSlice, relayURL *string) []cli.Flag {
	return []cli.Flag{
		&cli.StringFlag{
			Name:        "server",
			Usage:       "endpoint ID of the server (printed in its startup log)",
			Required:    true,
			Destination: server,
		},
		&cli.StringSliceFlag{
			Name:        "addr",
			Usage:       "direct server address host:port or ip:port (repeatable; skips discovery and relays)",
			Destination: addrs,
		},
		&cli.StringFlag{
			Name:        "relay",
			Usage:       "relay server URL to use as the fallback path (default: the built-in relay map)",
			Destination: relayURL,
		},
	}
}

// trackingRef is the local refstore name recording the last-seen
// server-side value of ref name on the given server.
func trackingRef(serverID string, name string) string {
	return trackingPrefix + serverID + "/" + name
}

// parseDirectAddrs turns --addr values into socket addresses. Each value
// is host:port where host is an IP literal or a hostname; hostnames may
// resolve to several addresses and all of them become dial candidates.
func parseDirectAddrs(ctx context.Context, addrs []string) ([]netip.AddrPort, error) {
	var out []netip.AddrPort
	for _, s := range addrs {
		if ap, err := netip.ParseAddrPort(s); err == nil {
			out = append(out, ap)
			continue
		}
		host, portStr, err := net.SplitHostPort(s)
		if err != nil {
			return nil, fmt.Errorf("parse --addr %q: %w", s, err)
		}
		port, err := strconv.ParseUint(portStr, 10, 16)
		if err != nil {
			return nil, fmt.Errorf("parse --addr %q: bad port: %w", s, err)
		}
		ips, err := net.DefaultResolver.LookupNetIP(ctx, "ip", host)
		if err != nil {
			return nil, fmt.Errorf("resolve --addr host %q: %w", host, err)
		}
		for _, ip := range ips {
			out = append(out, netip.AddrPortFrom(ip.Unmap(), uint16(port)))
		}
	}
	return out, nil
}

// dialServer connects to the server with an ephemeral client identity
// (access is open, so no stable key is needed). With direct addresses it
// dials straight at them — no discovery, no relays — which is also how
// the offline end-to-end tests connect. Without them it resolves the
// endpoint ID via pkarr and DNS like the irohese client.
func dialServer(ctx context.Context, serverID string, directAddrs []string, relayURL string) (*iroh.Conn, func(), error) {
	id, err := irohkey.ParseEndpointID(serverID)
	if err != nil {
		return nil, nil, fmt.Errorf("parse endpoint id: %w", err)
	}

	if len(directAddrs) > 0 {
		aps, err := parseDirectAddrs(ctx, directAddrs)
		if err != nil {
			return nil, nil, err
		}
		ep, err := iroh.Bind(ctx)
		if err != nil {
			return nil, nil, fmt.Errorf("bind: %w", err)
		}
		cands := make([]netaddr.TransportAddr, len(aps))
		for i, ap := range aps {
			cands[i] = netaddr.IPAddr{Addr: ap}
		}
		conn, err := raceConnect(ctx, ep, id, cands)
		if err != nil {
			ep.Shutdown(ctx)
			return nil, nil, fmt.Errorf("connect: %w", err)
		}
		return conn, func() { conn.Close(); ep.Shutdown(context.Background()) }, nil
	}

	sk, err := irohkey.GenerateSecretKey()
	if err != nil {
		return nil, nil, fmt.Errorf("generate key: %w", err)
	}

	pkarrResolver, err := iroh.N0PkarrResolver(nil)
	if err != nil {
		return nil, nil, fmt.Errorf("pkarr resolver: %w", err)
	}
	var services iroh.AddressLookupServices
	// mDNS first: on the server's LAN it yields direct addresses even
	// when the pkarr record is stale or unreachable. Passive (resolve
	// only), and a short lookup timeout so off-LAN dials fall through
	// to pkarr/DNS quickly.
	disc := mdns.New(irohkey.EndpointID(sk.Public()), mdns.WithPassive(true), mdns.WithLookupTimeout(time.Second))
	// Start is the listen loop itself — it blocks until ctx ends, so it
	// runs on its own goroutine for the lifetime of the command.
	go func() { _ = disc.Start(ctx) }()
	services.AddResolver(disc)
	services.AddResolver(pkarrResolver)
	services.AddResolver(iroh.N0DNSAddressLookup(nil))

	relayMode, err := relaymode.FromFlag(relayURL)
	if err != nil {
		return nil, nil, err
	}
	ep, err := iroh.Bind(
		ctx,
		iroh.WithSecretKey(sk),
		iroh.WithAddressLookup(&services),
		iroh.WithRelayMode(relayMode),
	)
	if err != nil {
		return nil, nil, fmt.Errorf("bind: %w", err)
	}

	// Connect does no discovery on its own: it only dials addresses
	// already present in the EndpointAddr, so resolve first. One
	// resolver's answer can be a partial view — mDNS yields direct
	// addresses with no relay, and any single record may list candidates
	// this network can't reach — so union every resolver's candidates:
	// the relay then always remains available as fallback when a direct
	// candidate turns out to be dead.
	addr := netaddr.NewEndpointAddr(id)
	resolved := false
	var lastErr error
	for item, err := range services.Resolve(ctx, id) {
		if err != nil {
			lastErr = err
			continue
		}
		addr = addr.WithAddrs(item.Addr().Addrs()...)
		resolved = true
	}
	if !resolved {
		ep.Shutdown(ctx)
		if lastErr != nil {
			return nil, nil, fmt.Errorf("no address found for endpoint %s (last resolver error: %v)", id, lastErr)
		}
		return nil, nil, fmt.Errorf("no address found for endpoint %s", id)
	}

	conn, err := raceConnect(ctx, ep, id, addr.Addrs())
	if err != nil {
		ep.Shutdown(ctx)
		return nil, nil, fmt.Errorf("connect: %w", err)
	}
	return conn, func() { conn.Close(); ep.Shutdown(context.Background()) }, nil
}

// raceConnect dials every candidate address concurrently and returns the
// first connection to complete. go-iroh's own multi-candidate connect
// walks a sorted candidate list with a per-candidate budget, so a few
// unreachable addresses (which sort low: container-bridge 10.x/172.x
// before LAN 192.168.x) exhaust the handshake window before a live one is
// tried; a live candidate answers in milliseconds when dialed directly.
// Losing attempts are canceled; late winners are closed.
func raceConnect(ctx context.Context, ep *iroh.Endpoint, id irohkey.EndpointID, cands []netaddr.TransportAddr) (*iroh.Conn, error) {
	if len(cands) == 0 {
		return nil, fmt.Errorf("no candidate addresses for endpoint %s", id)
	}
	type result struct {
		conn *iroh.Conn
		err  error
	}
	results := make(chan result, len(cands))
	cancels := make([]context.CancelFunc, len(cands))
	for i, ta := range cands {
		actx, cancel := context.WithCancel(ctx)
		cancels[i] = cancel
		go func(ta netaddr.TransportAddr) {
			conn, err := ep.Connect(actx, netaddr.NewEndpointAddr(id, ta), protocol.ALPN)
			results <- result{conn, err}
		}(ta)
	}
	var errs []error
	for range cands {
		r := <-results
		if r.err != nil {
			errs = append(errs, r.err)
			continue
		}
		// Winner: stop the losers and close any that already made it.
		for _, cancel := range cancels {
			cancel()
		}
		remaining := len(cands) - len(errs) - 1
		go func(n int) {
			for ; n > 0; n-- {
				if late := <-results; late.conn != nil {
					late.conn.Close()
				}
			}
		}(remaining)
		return r.conn, nil
	}
	for _, cancel := range cancels {
		cancel()
	}
	return nil, errors.Join(errs...)
}
