package main

import (
	"net"
	"net/netip"
)

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
