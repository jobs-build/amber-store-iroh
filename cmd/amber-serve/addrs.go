package main

import (
	"net"
	"net/netip"

	"github.com/tmc/go-iroh/netaddr"
)

// publishableAddrs is the pkarr publisher's address filter: everything
// except unusable IP candidates. The publisher's default (RelayOnlyFilter)
// would strip the direct addresses we advertise on purpose; publishing
// unfiltered would leak the raw wildcard bind address that rides along in
// the endpoint's address set and must not be advertised.
func publishableAddrs(addrs []netaddr.TransportAddr) []netaddr.TransportAddr {
	out := make([]netaddr.TransportAddr, 0, len(addrs))
	for _, a := range addrs {
		if ip, ok := a.(netaddr.IPAddr); ok && (!ip.Addr.IsValid() || ip.Addr.Addr().IsUnspecified()) {
			continue
		}
		out = append(out, a)
	}
	return out
}

// directAddrPorts pairs the machine's unicast interface addresses with the
// endpoint's bound UDP port, yielding the direct addresses worth
// advertising. The default wildcard bind address is not a usable dial
// candidate and is dropped from published records, which would leave peers
// with only a relay path; these are the candidates that let them dial
// direct. Loopback, link-local, and unspecified addresses are excluded,
// duplicates removed, order preserved.
func directAddrPorts(addrs []net.Addr, port uint16) []netip.AddrPort {
	var out []netip.AddrPort
	seen := make(map[netip.Addr]bool, len(addrs))
	for _, a := range addrs {
		n, ok := a.(*net.IPNet)
		if !ok {
			continue
		}
		ip, ok := netip.AddrFromSlice(n.IP)
		if !ok {
			continue
		}
		ip = ip.Unmap()
		if seen[ip] || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() || !ip.IsValid() || ip.IsUnspecified() {
			continue
		}
		seen[ip] = true
		out = append(out, netip.AddrPortFrom(ip, port))
	}
	return out
}
