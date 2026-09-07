// Package amberiroh is the single-import surface of transport-iroh: it
// re-exports the protocol, wantsync, server and relaymode packages so a
// consumer can write amberiroh.Msg, amberiroh.Receive, amberiroh.New and
// amberiroh.FromFlag without importing four paths. The four packages remain
// the implementation; nothing lives here but type aliases, re-declared
// constants and thin wrappers. Every exported identifier added to those
// packages must be mirrored here — amberiroh_test.go exists so a missing
// mirror fails to compile in this repo rather than in a consumer.
package amberiroh

import (
	"io"
	"iter"
	"log/slog"

	"github.com/amber-store/core/fstree"
	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/core/refstore"
	"github.com/amber-store/transport-iroh/protocol"
	"github.com/amber-store/transport-iroh/relaymode"
	"github.com/amber-store/transport-iroh/server"
	"github.com/amber-store/transport-iroh/wantsync"
	"github.com/tmc/go-iroh/relay"
)

// Wire parameters (protocol). ALPN is a wire constant: renaming it breaks
// every peer.
const (
	ALPN      = protocol.ALPN
	MaxFrame  = protocol.MaxFrame
	ChunkSize = protocol.ChunkSize
)

// Frame types (protocol).
const (
	TPush    = protocol.TPush
	TPull    = protocol.TPull
	TRefList = protocol.TRefList
	TRef     = protocol.TRef
	TRefs    = protocol.TRefs
	TWants   = protocol.TWants
	TData    = protocol.TData
	TDataEnd = protocol.TDataEnd
	TOK      = protocol.TOK
	TErr     = protocol.TErr
	TAttach  = protocol.TAttach
	TAccept  = protocol.TAccept
	TPin     = protocol.TPin
)

// Error codes carried in TErr frames (protocol).
const (
	CodeCASMismatch = protocol.CodeCASMismatch
	CodeUnknownRef  = protocol.CodeUnknownRef
	CodeBadRequest  = protocol.CodeBadRequest
	CodeInternal    = protocol.CodeInternal
)

// ErrProtocol is protocol.ErrProtocol; errors.Is matches under either name.
var ErrProtocol = protocol.ErrProtocol

// Types. Aliases carry their methods, so *Server has HandleStream, Serve
// and the Set* hooks, and *RemoteError implements error.
type (
	Msg             = protocol.Msg
	RefInfo         = protocol.RefInfo
	RemoteError     = protocol.RemoteError
	DataEndpointRec = protocol.DataEndpointRec
	Stats           = wantsync.Stats
	Progress        = wantsync.Progress
	Server          = server.Server
	RefGuard        = server.RefGuard
)

// WriteMsg is protocol.WriteMsg.
func WriteMsg(w io.Writer, m Msg) error { return protocol.WriteMsg(w, m) }

// ReadMsg is protocol.ReadMsg.
func ReadMsg(r io.Reader) (Msg, error) { return protocol.ReadMsg(r) }

// RemoteFromMsg is protocol.RemoteFromMsg.
func RemoteFromMsg(m Msg) *RemoteError { return protocol.RemoteFromMsg(m) }

// NewPackReader is protocol.NewPackReader.
func NewPackReader(r io.Reader) io.Reader { return protocol.NewPackReader(r) }

// SendPack is protocol.SendPack.
func SendPack(w io.Writer, objs iter.Seq2[fstree.Object, error]) error {
	return protocol.SendPack(w, objs)
}

// SendPackRecords is protocol.SendPackRecords.
func SendPackRecords(w io.Writer, recs iter.Seq2[[]byte, error]) error {
	return protocol.SendPackRecords(w, recs)
}

// Send is wantsync.Send.
func Send(rw io.ReadWriter, st *packstore.Store, prog Progress) error {
	return wantsync.Send(rw, st, prog)
}

// Receive is wantsync.Receive.
func Receive(channels []io.ReadWriter, st *packstore.Store, root key.Key, jobs int, prog Progress) (Stats, error) {
	return wantsync.Receive(channels, st, root, jobs, prog)
}

// Wants is wantsync.Wants.
func Wants(st *packstore.Store, frontier []key.Key, jobs int) ([]key.Key, error) {
	return wantsync.Wants(st, frontier, jobs)
}

// New is server.New.
func New(log *slog.Logger, objects *packstore.Store, refs *refstore.Store) *Server {
	return server.New(log, objects, refs)
}

// FromFlag is relaymode.FromFlag.
func FromFlag(url string) (relay.Mode, error) { return relaymode.FromFlag(url) }
