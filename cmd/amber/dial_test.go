package main

import (
	"context"
	"net/netip"
	"testing"
)

func TestParseDirectAddrsLiterals(t *testing.T) {
	got, err := parseDirectAddrs(context.Background(), []string{"192.168.1.9:4242", "[2001:db8::7]:99"})
	if err != nil {
		t.Fatal(err)
	}
	want := []netip.AddrPort{
		netip.MustParseAddrPort("192.168.1.9:4242"),
		netip.MustParseAddrPort("[2001:db8::7]:99"),
	}
	if len(got) != len(want) {
		t.Fatalf("got %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("got %v, want %v", got, want)
		}
	}
}

func TestParseDirectAddrsHostname(t *testing.T) {
	got, err := parseDirectAddrs(context.Background(), []string{"localhost:4242"})
	if err != nil {
		t.Fatal(err)
	}
	if len(got) == 0 {
		t.Fatal("localhost resolved to no addresses")
	}
	for _, ap := range got {
		if ap.Port() != 4242 {
			t.Fatalf("port lost in resolution: %v", ap)
		}
		if !ap.Addr().IsLoopback() {
			t.Fatalf("localhost resolved to non-loopback %v", ap)
		}
	}
}

func TestParseDirectAddrsErrors(t *testing.T) {
	for _, bad := range []string{"no-port", "host.invalid.:x", ":"} {
		if _, err := parseDirectAddrs(context.Background(), []string{bad}); err == nil {
			t.Fatalf("%q: want error, got nil", bad)
		}
	}
}
