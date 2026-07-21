// Package server implements the amber-store-iroh server: it owns a store
// directory and answers push/pull/ref-list operations, one per stream.
package server

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"sync"
	"time"

	"github.com/fables-for-robots/amber-store-core/key"
	"github.com/fables-for-robots/amber-store-core/packstore"
	"github.com/fables-for-robots/amber-store-core/reference"
	"github.com/fables-for-robots/amber-store-core/refstore"
	"github.com/fables-for-robots/amber-store-iroh/protocol"
	"github.com/fables-for-robots/amber-store-iroh/wantsync"
)

// Server answers amber-store-iroh operations against a single store.
// Access is open by design: any peer that can connect may push and pull.
type Server struct {
	log     *slog.Logger
	objects *packstore.Store
	refs    *refstore.Store
	jobs    int // completeness-walk parallelism; 0 = GOMAXPROCS

	mu       sync.Mutex
	refLocks map[string]*sync.Mutex
}

// New wires a Server over an open packstore and refstore. The caller keeps
// ownership of both and closes them after the server stops.
func New(log *slog.Logger, objects *packstore.Store, refs *refstore.Store) *Server {
	return &Server{log: log, objects: objects, refs: refs, refLocks: map[string]*sync.Mutex{}}
}

// lockRef serializes ref commits per name so compare-and-swap is
// race-free under concurrent pushes. Entries are never removed; the map
// is bounded by the number of distinct ref names ever pushed.
func (s *Server) lockRef(name string) (unlock func()) {
	s.mu.Lock()
	l := s.refLocks[name]
	if l == nil {
		l = &sync.Mutex{}
		s.refLocks[name] = l
	}
	s.mu.Unlock()
	l.Lock()
	return l.Unlock
}

// HandleStream serves one operation on one stream and closes it. The
// stream is FIN-closed after the final response frame; the connection
// stays up for further streams.
func (s *Server) HandleStream(rw io.ReadWriteCloser) {
	defer rw.Close()
	m, err := protocol.ReadMsg(rw)
	if err != nil {
		s.log.Error("read request", "error", err)
		return
	}
	switch m.Type {
	case protocol.TRefList:
		err = s.handleRefList(rw)
	case protocol.TPush:
		err = s.handlePush(rw, m)
	case protocol.TPull:
		err = s.handlePull(rw, m)
	default:
		err = s.fail(rw, protocol.CodeBadRequest, fmt.Errorf("unknown operation %d", m.Type))
	}
	if err != nil {
		s.log.Error("operation failed", "op", m.Type, "name", m.Name, "error", err)
	}
}

// fail sends a TErr frame (best effort) and returns err for logging.
func (s *Server) fail(w io.Writer, code string, err error) error {
	_ = protocol.WriteMsg(w, protocol.Msg{Type: protocol.TErr, Code: code, Text: err.Error()})
	return err
}

func (s *Server) handleRefList(rw io.ReadWriter) error {
	records, err := s.refs.All()
	if err != nil {
		return s.fail(rw, protocol.CodeInternal, err)
	}
	infos := make([]protocol.RefInfo, 0, len(records))
	for _, r := range records {
		rec, err := reference.Decode(r.Data)
		if err != nil {
			return s.fail(rw, protocol.CodeInternal, fmt.Errorf("reference %q: %w", r.Name, err))
		}
		infos = append(infos, protocol.RefInfo{Name: r.Name, Key: rec.Key, CreatedAt: rec.CreatedAt, User: rec.User})
	}
	return protocol.WriteMsg(rw, protocol.Msg{Type: protocol.TRefs, Refs: infos})
}

func (s *Server) handlePush(rw io.ReadWriter, m protocol.Msg) error {
	if err := reference.ValidateName(m.Name); err != nil {
		return s.fail(rw, protocol.CodeBadRequest, err)
	}
	root, err := key.Parse(m.Root)
	if err != nil {
		return s.fail(rw, protocol.CodeBadRequest, fmt.Errorf("root key: %w", err))
	}
	// Early precondition check: reject before any transfer happens. The
	// authoritative re-check happens under the per-name lock at commit.
	if m.CAS {
		if err := s.checkCAS(rw, m); err != nil {
			return err
		}
	}
	if err := wantsync.Receive(rw, s.objects, root, s.jobs); err != nil {
		return err
	}
	unlock := s.lockRef(m.Name)
	defer unlock()
	if m.CAS {
		if err := s.checkCAS(rw, m); err != nil {
			return err
		}
	}
	rec := reference.Reference{Name: m.Name, Key: root[:], CreatedAt: time.Now().UnixNano()}
	raw, err := rec.Encode()
	if err != nil {
		return s.fail(rw, protocol.CodeInternal, err)
	}
	if err := s.refs.Put(m.Name, raw); err != nil {
		return s.fail(rw, protocol.CodeInternal, err)
	}
	return protocol.WriteMsg(rw, protocol.Msg{Type: protocol.TOK, Key: root[:]})
}

// checkCAS verifies the push precondition: ExpectedOld must equal the
// ref's current key (nil meaning "ref must not exist"). On mismatch it
// sends the cas-mismatch frame carrying the current key and returns an
// error that ends the operation.
func (s *Server) checkCAS(rw io.ReadWriter, m protocol.Msg) error {
	var current []byte
	raw, err := s.refs.Get(m.Name)
	switch {
	case err == nil:
		rec, decErr := reference.Decode(raw)
		if decErr != nil {
			return s.fail(rw, protocol.CodeInternal, decErr)
		}
		current = rec.Key
	case errors.Is(err, refstore.ErrNotFound):
		// current stays nil: the ref does not exist.
	default:
		return s.fail(rw, protocol.CodeInternal, err)
	}
	if bytes.Equal(current, m.ExpectedOld) {
		return nil
	}
	_ = protocol.WriteMsg(rw, protocol.Msg{
		Type:    protocol.TErr,
		Code:    protocol.CodeCASMismatch,
		Text:    fmt.Sprintf("remote ref %q changed", m.Name),
		Current: current,
	})
	return fmt.Errorf("cas mismatch on %q", m.Name)
}

func (s *Server) handlePull(rw io.ReadWriter, m protocol.Msg) error {
	raw, err := s.refs.Get(m.Name)
	if errors.Is(err, refstore.ErrNotFound) {
		return s.fail(rw, protocol.CodeUnknownRef, fmt.Errorf("ref %q not found", m.Name))
	}
	if err != nil {
		return s.fail(rw, protocol.CodeInternal, err)
	}
	if err := protocol.WriteMsg(rw, protocol.Msg{Type: protocol.TRef, Record: raw}); err != nil {
		return err
	}
	return wantsync.Send(rw, s.objects)
}
