package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/netip"
	"strconv"
	"sync"
	"time"

	"github.com/amber-store/transport-iroh/protocol"
	"github.com/amber-store/transport-iroh/relaymode"
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

// serverConn is an established control connection plus the ability to
// open extra connections to the same server over the same endpoint and
// candidate set — sharded transfers race the same candidates again.
type serverConn struct {
	conn  *iroh.Conn
	ep    *iroh.Endpoint
	id    irohkey.EndpointID
	cands []netaddr.TransportAddr

	mu       sync.Mutex
	extraEPs []*iroh.Endpoint
}

// Extra opens one more connection to the same server on its own
// endpoint — an endpoint's single UDP socket loop caps throughput, so
// sharded connections must not share one. When the server advertised
// dedicated data ports, the connection goes to port ports[i%len] on the
// address the control connection actually reached (spreading load across
// the server's sockets); otherwise it races the control candidates.
// Extra endpoints are torn down by Close.
func (s *serverConn) Extra(ctx context.Context, i int, ports []uint16) (*iroh.Conn, error) {
	ep, err := iroh.Bind(ctx)
	if err != nil {
		return nil, err
	}
	cands := s.cands
	if len(ports) > 0 {
		if ip, ok := s.remoteIP(); ok {
			cands = []netaddr.TransportAddr{netaddr.IPAddr{Addr: netip.AddrPortFrom(ip, ports[i%len(ports)])}}
		}
	}
	conn, err := raceConnect(ctx, ep, s.id, cands)
	if err != nil {
		// The dedicated port may be filtered; fall back to the
		// candidates that reached the control stream.
		if len(ports) > 0 {
			conn, err = raceConnect(ctx, ep, s.id, s.cands)
		}
		if err != nil {
			ep.Shutdown(context.Background())
			return nil, err
		}
	}
	s.mu.Lock()
	s.extraEPs = append(s.extraEPs, ep)
	s.mu.Unlock()
	return conn, nil
}

// remoteIP is the address the control connection actually reached the
// server at.
func (s *serverConn) remoteIP() (netip.Addr, bool) {
	ua, ok := s.conn.RemoteAddr().(*net.UDPAddr)
	if !ok {
		return netip.Addr{}, false
	}
	ip, ok := netip.AddrFromSlice(ua.IP)
	if !ok {
		return netip.Addr{}, false
	}
	return ip.Unmap(), true
}

// Close tears down the control connection and every endpoint.
func (s *serverConn) Close() {
	s.conn.Close()
	s.ep.Shutdown(context.Background())
	s.mu.Lock()
	extras := s.extraEPs
	s.extraEPs = nil
	s.mu.Unlock()
	for _, ep := range extras {
		ep.Shutdown(context.Background())
	}
}

// dialServer connects to the server with an ephemeral client identity
// (access is open, so no stable key is needed). With direct addresses it
// dials straight at them — no discovery, no relays — which is also how
// the offline end-to-end tests connect. Without them it resolves the
// endpoint ID via pkarr and DNS like the irohese client.
func dialServer(ctx context.Context, serverID string, directAddrs []string, relayURL string) (*serverConn, error) {
	id, err := irohkey.ParseEndpointID(serverID)
	if err != nil {
		return nil, fmt.Errorf("parse endpoint id: %w", err)
	}

	if len(directAddrs) > 0 {
		aps, err := parseDirectAddrs(ctx, directAddrs)
		if err != nil {
			return nil, err
		}
		ep, err := iroh.Bind(ctx)
		if err != nil {
			return nil, fmt.Errorf("bind: %w", err)
		}
		cands := make([]netaddr.TransportAddr, len(aps))
		for i, ap := range aps {
			cands[i] = netaddr.IPAddr{Addr: ap}
		}
		conn, err := raceConnect(ctx, ep, id, cands)
		if err != nil {
			ep.Shutdown(ctx)
			return nil, fmt.Errorf("connect: %w", err)
		}
		return &serverConn{conn: conn, ep: ep, id: id, cands: cands}, nil
	}

	sk, err := irohkey.GenerateSecretKey()
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}

	pkarrResolver, err := iroh.N0PkarrResolver(nil)
	if err != nil {
		return nil, fmt.Errorf("pkarr resolver: %w", err)
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
		return nil, err
	}
	ep, err := iroh.Bind(
		ctx,
		iroh.WithSecretKey(sk),
		iroh.WithAddressLookup(&services),
		iroh.WithRelayMode(relayMode),
	)
	if err != nil {
		return nil, fmt.Errorf("bind: %w", err)
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
			return nil, fmt.Errorf("no address found for endpoint %s (last resolver error: %v)", id, lastErr)
		}
		return nil, fmt.Errorf("no address found for endpoint %s", id)
	}

	cands := addr.Addrs()
	conn, err := raceConnect(ctx, ep, id, cands)
	if err != nil {
		ep.Shutdown(ctx)
		return nil, fmt.Errorf("connect: %w", err)
	}
	return &serverConn{conn: conn, ep: ep, id: id, cands: cands}, nil
}

// attachExtras opens up to n extra connections, attaches each to the
// transfer token, and returns the attached streams with a closer.
// Failures reduce parallelism instead of failing the transfer — the
// server's gather is lenient about missing attaches.
func attachExtras(ctx context.Context, sc *serverConn, token []byte, ports []uint16, n int) ([]io.ReadWriter, func()) {
	var streams []io.ReadWriter
	var closers []func()
	for i := 0; i < n; i++ {
		conn, err := sc.Extra(ctx, i, ports)
		if err != nil {
			break
		}
		stream, err := conn.OpenStreamConn(ctx)
		if err != nil {
			conn.Close()
			break
		}
		if err := protocol.WriteMsg(stream, protocol.Msg{Type: protocol.TAttach, Token: token}); err != nil {
			stream.Close()
			conn.Close()
			break
		}
		streams = append(streams, stream)
		closers = append(closers, func() { stream.Close(); conn.Close() })
	}
	return streams, func() {
		for _, c := range closers {
			c()
		}
	}
}

// connsFlag clamps the --conns value to a sane range.
func connsFlag(n int) int {
	if n < 1 {
		return 1
	}
	if n > 16 {
		return 16
	}
	return n
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
