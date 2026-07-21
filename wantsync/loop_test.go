package wantsync

import (
	"errors"
	"io"
	"strings"
	"sync"
	"testing"

	"github.com/fables-for-robots/amber-store-core/fstree"
	"github.com/fables-for-robots/amber-store-core/key"
	"github.com/fables-for-robots/amber-store-core/packstore"
	"github.com/fables-for-robots/amber-store-iroh/protocol"
)

// duplex joins one side's reader with its writer.
type duplex struct {
	io.Reader
	io.Writer
}

// pipePair returns two connected in-memory duplex streams.
func pipePair() (duplex, duplex) {
	ar, aw := io.Pipe()
	br, bw := io.Pipe()
	return duplex{ar, bw}, duplex{br, aw}
}

// runLoop drives Send on src and Receive on dest concurrently.
func runLoop(t *testing.T, src, dest *packstore.Store, root key.Key) (stats Stats, sendErr, recvErr error) {
	t.Helper()
	a, b := pipePair()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sendErr = Send(a, src, nil)
		// Unblock the peer if the sender bailed early.
		if c, ok := a.Writer.(io.Closer); ok && sendErr != nil {
			c.Close()
		}
	}()
	stats, recvErr = Receive(b, dest, root, 0, nil)
	wg.Wait()
	return stats, sendErr, recvErr
}

func TestLoopSyncsIntoEmptyStore(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)
	_, sendErr, recvErr := runLoop(t, src, dest, root)
	if sendErr != nil || recvErr != nil {
		t.Fatalf("send=%v recv=%v", sendErr, recvErr)
	}
	if err := fstree.CheckComplete(root, dest.Get, dest.Has, 0); err != nil {
		t.Fatalf("dest incomplete after sync: %v", err)
	}
}

func TestLoopIsIdempotent(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)
	if _, se, re := runLoop(t, src, dest, root); se != nil || re != nil {
		t.Fatalf("first sync: send=%v recv=%v", se, re)
	}
	if _, se, re := runLoop(t, src, dest, root); se != nil || re != nil {
		t.Fatalf("second sync: send=%v recv=%v", se, re)
	}
	if err := fstree.CheckComplete(root, dest.Get, dest.Has, 0); err != nil {
		t.Fatal(err)
	}
}

// TestLoopResumesPartialTransfer plants only the root object in dest —
// the on-disk state an interrupted push leaves behind — and verifies the
// loop completes the tree.
func TestLoopResumesPartialTransfer(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)
	rootBytes, err := src.Get(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := dest.Put(root, rootBytes); err != nil {
		t.Fatal(err)
	}
	if _, se, re := runLoop(t, src, dest, root); se != nil || re != nil {
		t.Fatalf("send=%v recv=%v", se, re)
	}
	if err := fstree.CheckComplete(root, dest.Get, dest.Has, 0); err != nil {
		t.Fatalf("dest incomplete after resume: %v", err)
	}
}

// TestLoopSenderOmitsWantedObject drives Receive against a sender that
// answers every round with a well-formed but empty pack. The frontier
// advances only through received objects, so without delivery
// verification the loop would terminate as success over an empty store.
func TestLoopSenderOmitsWantedObject(t *testing.T) {
	_, root := buildTree(t)
	dest := openStore(t)
	err := receiveFromEmptyPackSender(t, dest, root)
	if err == nil {
		t.Fatal("undelivered wants must fail the loop, not succeed")
	}
	if !strings.Contains(err.Error(), "omitted 1 of 1") {
		t.Fatalf("error must name the missing wants: %v", err)
	}
}

// TestLoopSenderOmitsIncompleteWantedObject is the resume-path variant: the
// root is already present but incomplete, so it is requested again even
// though the store would answer Has for it. Delivery must be judged by what
// the pack actually carried, or a sender could skip exactly these rounds
// and still have the loop report success over an incomplete tree.
func TestLoopSenderOmitsIncompleteWantedObject(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)
	rootBytes, err := src.Get(root)
	if err != nil {
		t.Fatal(err)
	}
	if err := dest.Put(root, rootBytes); err != nil {
		t.Fatal(err)
	}
	err = receiveFromEmptyPackSender(t, dest, root)
	if err == nil {
		t.Fatal("a present-but-incomplete want left undelivered must fail the loop")
	}
	if !strings.Contains(err.Error(), "omitted 1 of 1") {
		t.Fatalf("error must name the missing wants: %v", err)
	}
}

// receiveFromEmptyPackSender runs Receive against a sender that answers
// every want round with a well-formed but empty pack.
func receiveFromEmptyPackSender(t *testing.T, dest *packstore.Store, root key.Key) error {
	t.Helper()
	a, b := pipePair()
	go func() {
		for {
			m, err := protocol.ReadMsg(a)
			if err != nil || m.Type != protocol.TWants || len(m.Keys) == 0 {
				if c, ok := a.Writer.(io.Closer); ok {
					c.Close()
				}
				return
			}
			empty := func(yield func(fstree.Object, error) bool) {}
			if err := protocol.SendPack(a, empty); err != nil {
				return
			}
		}
	}()
	_, err := Receive(b, dest, root, 0, nil)
	return err
}

// recordingProgress sums observer callbacks; safe for the loop's
// single-threaded use.
type recordingProgress struct {
	reqObjs, xferObjs   int
	reqBytes, xferBytes int64
}

func (r *recordingProgress) Requested(objects int, bytes int64) {
	r.reqObjs += objects
	r.reqBytes += bytes
}

func (r *recordingProgress) Transferred(objects int, bytes int64) {
	r.xferObjs += objects
	r.xferBytes += bytes
}

// TestLoopReportsProgress drives a fresh sync with observers on both
// halves: each side must see every object of the tree requested and
// transferred, and requested bytes (derived from key lengths) must match
// the bytes actually moved.
func TestLoopReportsProgress(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)
	total, err := fstree.ReachableKeys(root, src.Get)
	if err != nil {
		t.Fatal(err)
	}

	var sendRec, recvRec recordingProgress
	a, b := pipePair()
	var wg sync.WaitGroup
	wg.Add(1)
	var sendErr error
	go func() {
		defer wg.Done()
		sendErr = Send(a, src, &sendRec)
	}()
	_, recvErr := Receive(b, dest, root, 0, &recvRec)
	wg.Wait()
	if sendErr != nil || recvErr != nil {
		t.Fatalf("send=%v recv=%v", sendErr, recvErr)
	}

	for name, rec := range map[string]*recordingProgress{"send": &sendRec, "recv": &recvRec} {
		if rec.reqObjs != len(total) || rec.xferObjs != len(total) {
			t.Fatalf("%s: requested=%d transferred=%d objects, want both %d", name, rec.reqObjs, rec.xferObjs, len(total))
		}
		// Key lengths are logical sizes: subtree footprints for
		// directory types, so requested bytes bound transferred
		// payload bytes from above.
		if rec.xferBytes == 0 || rec.reqBytes < rec.xferBytes {
			t.Fatalf("%s: requested %d bytes must be >= transferred %d", name, rec.reqBytes, rec.xferBytes)
		}
	}
}

// TestReceiveStats checks the transfer accounting a fresh sync and an
// idempotent re-sync report: a fresh sync requests and receives exactly
// the tree's objects and counts wire bytes; a re-sync moves nothing and
// ends after the single empty want round.
func TestReceiveStats(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)
	total, err := fstree.ReachableKeys(root, src.Get)
	if err != nil {
		t.Fatal(err)
	}

	stats, se, re := runLoop(t, src, dest, root)
	if se != nil || re != nil {
		t.Fatalf("send=%v recv=%v", se, re)
	}
	if stats.Received != len(total) || stats.Requested != len(total) {
		t.Fatalf("fresh sync: requested=%d received=%d, want both %d", stats.Requested, stats.Received, len(total))
	}
	if stats.Bytes == 0 {
		t.Fatal("fresh sync must count wire bytes")
	}
	if stats.Rounds < 2 {
		t.Fatalf("fresh sync of a multi-level tree took %d rounds", stats.Rounds)
	}

	stats, se, re = runLoop(t, src, dest, root)
	if se != nil || re != nil {
		t.Fatalf("re-sync: send=%v recv=%v", se, re)
	}
	if stats.Received != 0 || stats.Requested != 0 || stats.Bytes != 0 {
		t.Fatalf("re-sync must transfer nothing: %+v", stats)
	}
	if stats.Rounds != 1 {
		t.Fatalf("re-sync must end after the empty round, took %d", stats.Rounds)
	}
}

// TestLoopSenderMissingObject syncs from a sender that lacks the tree:
// the sender must report a remote error and the receiver must fail, not
// hang or succeed.
func TestLoopSenderMissingObject(t *testing.T) {
	_, root := buildTree(t)
	emptySrc := openStore(t)
	dest := openStore(t)
	_, sendErr, recvErr := runLoop(t, emptySrc, dest, root)
	if !errors.Is(sendErr, packstore.ErrNotFound) {
		t.Fatalf("sender error: %v", sendErr)
	}
	var re *protocol.RemoteError
	if !errors.As(recvErr, &re) || re.Code != protocol.CodeInternal {
		t.Fatalf("receiver error: %v", recvErr)
	}
}
