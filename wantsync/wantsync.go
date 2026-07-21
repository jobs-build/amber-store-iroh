// Package wantsync implements both halves of the have/want object-transfer
// loop: the receiver announces which keys it is missing below a root, the
// sender answers each round with an amberpack of exactly those objects.
package wantsync

import (
	"errors"
	"fmt"
	"io"

	"github.com/fables-for-robots/amber-store-core/amberpack"
	"github.com/fables-for-robots/amber-store-core/fstree"
	"github.com/fables-for-robots/amber-store-core/key"
	"github.com/fables-for-robots/amber-store-core/packstore"
	"github.com/fables-for-robots/amber-store-iroh/protocol"
)

// maxWantsPerRound caps how many keys one TWants frame may carry. A very
// wide frontier would otherwise encode past protocol.MaxFrame, which is a
// permanent failure: re-running recomputes the same oversized round. The
// remainder is carried into the next round instead.
const maxWantsPerRound = 32 << 10

// splitWants divides wants into the keys to request this round and the
// remainder to defer to the next one.
func splitWants(wants []key.Key, max int) (send, carry []key.Key) {
	if len(wants) <= max {
		return wants, nil
	}
	return wants[:max], wants[max:]
}

// Wants partitions frontier into the keys that must be transferred. A key
// is pruned only when its object is present AND the whole subtree below it
// is complete — presence alone is not enough, because an interrupted
// transfer stores parents before their children. A present-but-incomplete
// key is re-requested whole; its redundant bytes are trivial and the
// receiver re-walks its children from the fresh copy.
func Wants(st *packstore.Store, frontier []key.Key, jobs int) ([]key.Key, error) {
	var wants []key.Key
	seen := make(map[key.Key]bool, len(frontier))
	for _, k := range frontier {
		if seen[k] {
			continue
		}
		seen[k] = true
		ok, err := st.Has(k)
		if err != nil {
			return nil, err
		}
		if !ok {
			wants = append(wants, k)
			continue
		}
		err = fstree.CheckComplete(k, st.Get, st.Has, jobs)
		switch {
		case err == nil: // complete subtree: prune
		case isMissing(err):
			wants = append(wants, k)
		default:
			return nil, err
		}
	}
	return wants, nil
}

// isMissing reports whether a CheckComplete failure means "object absent"
// (leaves surface as *fstree.MissingObjectError, interior nodes as the
// store's not-found error) rather than a real store fault.
func isMissing(err error) bool {
	var m *fstree.MissingObjectError
	return errors.As(err, &m) || errors.Is(err, packstore.ErrNotFound)
}

// encodeKeys flattens keys for a TWants frame.
func encodeKeys(keys []key.Key) [][]byte {
	out := make([][]byte, len(keys))
	for i, k := range keys {
		kk := k
		out[i] = kk[:]
	}
	return out
}

// decodeKeys parses and validates TWants keys.
func decodeKeys(bs [][]byte) ([]key.Key, error) {
	out := make([]key.Key, len(bs))
	for i, b := range bs {
		k, err := key.Parse(b)
		if err != nil {
			return nil, err
		}
		out[i] = k
	}
	return out, nil
}

// Stats summarizes one Receive run for transfer accounting.
type Stats struct {
	Rounds    int   // want rounds sent, including the final empty one
	Requested int   // keys requested across all rounds
	Received  int   // objects delivered in packs
	Bytes     int64 // wire bytes read: pack frames and payloads
}

// countingReader counts the bytes read through it.
type countingReader struct {
	r io.Reader
	n int64
}

func (c *countingReader) Read(p []byte) (int, error) {
	n, err := c.r.Read(p)
	c.n += int64(n)
	return n, err
}

// Receive runs the receiving half of the want loop over rw: rounds of
// TWants → amberpack until nothing below root is missing. The final,
// empty TWants tells the sender the loop is over. Received objects are
// verified against their keys before being stored — the peer is untrusted.
func Receive(rw io.ReadWriter, st *packstore.Store, root key.Key, jobs int) (Stats, error) {
	var stats Stats
	cr := &countingReader{r: rw}
	frontier := []key.Key{root}
	for {
		wants, err := Wants(st, frontier, jobs)
		if err != nil {
			return stats, err
		}
		wants, carry := splitWants(wants, maxWantsPerRound)
		if err := protocol.WriteMsg(rw, protocol.Msg{Type: protocol.TWants, Keys: encodeKeys(wants)}); err != nil {
			return stats, err
		}
		stats.Rounds++
		stats.Requested += len(wants)
		if len(wants) == 0 {
			stats.Bytes = cr.n
			return stats, nil
		}
		var next []key.Key
		received := make(map[key.Key]bool, len(wants))
		packSrc := protocol.NewPackReader(cr)
		tracked := &errTrackingReader{Reader: packSrc}
		pr := amberpack.NewReader(tracked)
		seq := func(yield func(packstore.Object, error) bool) {
			for o, err := range pr.All() {
				if err != nil {
					yield(packstore.Object{}, err)
					return
				}
				kids, err := fstree.ChildKeys(o.Key, o.Bytes)
				if err != nil {
					yield(packstore.Object{}, err)
					return
				}
				received[o.Key] = true
				next = append(next, kids...)
				if !yield(packstore.Object{Key: o.Key, Data: o.Bytes}, nil) {
					return
				}
			}
		}
		if _, err := st.WriteParallel(seq, packstore.WriteOpts{Verify: true}); err != nil {
			// amberpack reports stream corruption with fmt.Errorf("%w: ...: %v",
			// ErrMalformed, rawErr) — the %v loses the type of rawErr, so a
			// *protocol.RemoteError carried inside the pack (a TErr frame the
			// sender wrote after a local failure) is not reachable via
			// errors.As on err. tracked recorded the untouched error from
			// packSrc's Read before amberpack wrapped it; prefer it when set.
			if terr := tracked.err; terr != nil {
				return stats, terr
			}
			return stats, err
		}
		// amberpack's decoder stops at its own end marker; drain through
		// our TDataEnd frame so the stream is positioned for the next round.
		if _, err := io.Copy(io.Discard, packSrc); err != nil {
			return stats, err
		}
		stats.Received += len(received)
		if err := checkDelivered(received, wants); err != nil {
			return stats, err
		}
		// Carried-over wants rejoin the frontier; Wants dedupes them
		// against the children just discovered and re-verifies presence.
		frontier = append(next, carry...)
	}
}

// checkDelivered fails when the sender did not deliver every key this
// round asked for. The frontier only advances to children of received
// objects, so an omitted want would otherwise vanish silently and let an
// incomplete tree end the loop as success. Receipt is judged by what
// arrived in the pack, not by store presence: a key requested because it
// was present but incomplete already satisfies Has, so presence would let
// a sender skip exactly the resume rounds. An honest sender always resends
// every requested key.
func checkDelivered(received map[key.Key]bool, wants []key.Key) error {
	missing := 0
	var example key.Key
	for _, k := range wants {
		if !received[k] {
			if missing == 0 {
				example = k
			}
			missing++
		}
	}
	if missing > 0 {
		return fmt.Errorf("sender omitted %d of %d requested objects (e.g. %s)", missing, len(wants), example)
	}
	return nil
}

// Send runs the sending half: answer each TWants round with a pack of
// exactly the requested objects, until an empty TWants ends the loop. A
// local read failure is reported to the peer as a TErr frame and returned.
func Send(rw io.ReadWriter, st *packstore.Store) error {
	for {
		m, err := protocol.ReadMsg(rw)
		if err != nil {
			return err
		}
		switch m.Type {
		case protocol.TErr:
			return protocol.RemoteFromMsg(m)
		case protocol.TWants:
		default:
			return fmt.Errorf("%w: type %d, want TWants", protocol.ErrProtocol, m.Type)
		}
		if len(m.Keys) == 0 {
			return nil
		}
		keys, err := decodeKeys(m.Keys)
		if err != nil {
			return err
		}
		keys = dedupeKeys(keys)
		st.SortByLocation(keys)
		seq := func(yield func(fstree.Object, error) bool) {
			for _, k := range keys {
				data, err := st.Get(k)
				if err != nil {
					yield(fstree.Object{}, fmt.Errorf("object %s: %w", k, err))
					return
				}
				if !yield(fstree.Object{Key: k, Bytes: data}, nil) {
					return
				}
			}
		}
		if err := protocol.SendPack(rw, seq); err != nil {
			// Best effort: tell the peer why the pack stopped short.
			_ = protocol.WriteMsg(rw, protocol.Msg{Type: protocol.TErr, Code: protocol.CodeInternal, Text: err.Error()})
			return err
		}
	}
}

// dedupeKeys drops repeats, keeping first-seen order: a peer that asks
// for one key many times must not get its bytes many times.
func dedupeKeys(keys []key.Key) []key.Key {
	seen := make(map[key.Key]bool, len(keys))
	out := keys[:0]
	for _, k := range keys {
		if seen[k] {
			continue
		}
		seen[k] = true
		out = append(out, k)
	}
	return out
}

// errTrackingReader remembers the last non-EOF error a Read returned,
// unmodified. amberpack.Reader.All folds read errors into its own
// ErrMalformed-wrapped message with %v instead of %w, which erases the
// type of errors like *protocol.RemoteError; Receive consults the tracked
// error to recover it.
type errTrackingReader struct {
	io.Reader
	err error
}

func (t *errTrackingReader) Read(p []byte) (int, error) {
	n, err := t.Reader.Read(p)
	if err != nil && err != io.EOF {
		t.err = err
	}
	return n, err
}
