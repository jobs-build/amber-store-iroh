// Package wantsync implements both halves of the have/want object-transfer
// loop: the receiver announces which keys it is missing below a root, the
// sender answers each round with an amberpack of exactly those objects.
package wantsync

import (
	"errors"

	"github.com/fables-for-robots/amber-store-core/fstree"
	"github.com/fables-for-robots/amber-store-core/key"
	"github.com/fables-for-robots/amber-store-core/packstore"
)

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
