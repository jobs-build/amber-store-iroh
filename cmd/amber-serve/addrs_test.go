package main

import (
	"net"
	"net/netip"
	"slices"
	"testing"

	"github.com/tmc/go-iroh/netaddr"
)

func ipNet(s string) *net.IPNet {
	ip, n, err := net.ParseCIDR(s)
	if err != nil {
		panic(err)
	}
	n.IP = ip
	return n
}

func TestDirectAddrPorts(t *testing.T) {
	in := []net.Addr{
		ipNet("192.168.1.50/24"),                      // LAN IPv4: keep
		ipNet("10.0.0.7/8"),                           // private IPv4: keep
		ipNet("2001:db8::1234/64"),                    // global IPv6: keep
		ipNet("127.0.0.1/8"),                          // loopback: drop
		ipNet("::1/128"),                              // loopback: drop
		ipNet("fe80::1/64"),                           // link-local: drop
		ipNet("169.254.10.1/16"),                      // link-local IPv4: drop
		ipNet("0.0.0.0/0"),                            // unspecified: drop
		&net.TCPAddr{IP: net.ParseIP("192.168.1.60")}, // not *net.IPNet: drop
		ipNet("192.168.1.50/24"),                      // duplicate: dedupe
	}
	got := directAddrPorts(in, 4242)
	want := []netip.AddrPort{
		netip.MustParseAddrPort("192.168.1.50:4242"),
		netip.MustParseAddrPort("10.0.0.7:4242"),
		netip.MustParseAddrPort("[2001:db8::1234]:4242"),
	}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}

func TestPublishableAddrsDropsWildcard(t *testing.T) {
	relay := netaddr.RelayAddr{}
	keep := netaddr.IPAddr{Addr: netip.MustParseAddrPort("192.168.1.9:1")}
	in := []netaddr.TransportAddr{
		relay,
		keep,
		netaddr.IPAddr{Addr: netip.MustParseAddrPort("[::]:1")},    // wildcard: drop
		netaddr.IPAddr{Addr: netip.MustParseAddrPort("0.0.0.0:1")}, // wildcard: drop
		netaddr.IPAddr{}, // invalid: drop
	}
	got := publishableAddrs(in)
	if len(got) != 2 || got[0] != relay || got[1] != keep {
		t.Fatalf("got %v", got)
	}
}

func TestDirectAddrPortsUnmapsIPv4InIPv6(t *testing.T) {
	got := directAddrPorts([]net.Addr{ipNet("::ffff:192.168.1.9/96")}, 1)
	want := []netip.AddrPort{netip.MustParseAddrPort("192.168.1.9:1")}
	if !slices.Equal(got, want) {
		t.Fatalf("got %v, want %v", got, want)
	}
}
