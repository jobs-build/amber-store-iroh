package amberiroh_test

import (
	"bytes"
	"errors"
	"io"
	"log/slog"
	"reflect"
	"testing"

	"github.com/amber-store/transport-iroh/amberiroh"
	"github.com/amber-store/transport-iroh/protocol"
)

// Every wrapper must exist with the upstream signature; a missing one is a
// compile error here rather than in a consumer.
var _ = []any{
	amberiroh.WriteMsg, amberiroh.ReadMsg, amberiroh.RemoteFromMsg,
	amberiroh.NewPackReader, amberiroh.SendPack, amberiroh.SendPackRecords,
	amberiroh.Send, amberiroh.Receive, amberiroh.Wants,
	amberiroh.New, amberiroh.FromFlag,
}

func TestFacadeMsgRoundTrip(t *testing.T) {
	in := amberiroh.Msg{Type: amberiroh.TAccept, Token: []byte{1},
		DataEndpoints: []amberiroh.DataEndpointRec{{ID: bytes.Repeat([]byte{7}, 32), Addrs: []string{"ip:127.0.0.1:4001"}}}}
	var buf bytes.Buffer
	if err := amberiroh.WriteMsg(&buf, in); err != nil {
		t.Fatal(err)
	}
	out, err := amberiroh.ReadMsg(&buf)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Fatalf("round trip mismatch:\n in %+v\nout %+v", in, out)
	}
}

func TestFacadeServerHooks(t *testing.T) {
	srv := amberiroh.New(slog.New(slog.NewTextHandler(io.Discard, nil)), nil, nil)
	srv.SetDataPorts([]uint16{4001})
	srv.SetDataEndpoints(func() []amberiroh.DataEndpointRec { return nil })
	srv.SetOnAccess(func(string) {})
	srv.SetOnPin(func(string) {})
	var g amberiroh.RefGuard
	srv.SetRefGuard(g)
	_ = srv.HandleStream
}

func TestFacadeIdentities(t *testing.T) {
	if !errors.Is(amberiroh.ErrProtocol, protocol.ErrProtocol) {
		t.Fatal("ErrProtocol must be the same error value as protocol.ErrProtocol")
	}
	if amberiroh.ALPN != protocol.ALPN || amberiroh.TPin != protocol.TPin || amberiroh.CodeInternal != protocol.CodeInternal {
		t.Fatal("constants must mirror protocol's values")
	}
	var m amberiroh.Msg = protocol.Msg{Type: protocol.TOK}
	if m.Type != amberiroh.TOK {
		t.Fatal("Msg must alias protocol.Msg")
	}
	re := amberiroh.RemoteFromMsg(amberiroh.Msg{Type: amberiroh.TErr, Code: amberiroh.CodeBadRequest, Text: "x"})
	if re == nil || re.Code != protocol.CodeBadRequest {
		t.Fatalf("RemoteFromMsg: %+v", re)
	}
}
