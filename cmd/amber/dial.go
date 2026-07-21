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
