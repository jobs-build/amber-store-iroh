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
func runLoop(t *testing.T, src, dest *packstore.Store, root key.Key) (sendErr, recvErr error) {
	t.Helper()
	a, b := pipePair()
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		sendErr = Send(a, src)
		// Unblock the peer if the sender bailed early.
		if c, ok := a.Writer.(io.Closer); ok && sendErr != nil {
			c.Close()
		}
	}()
	recvErr = Receive(b, dest, root, 0)
	wg.Wait()
	return sendErr, recvErr
}

func TestLoopSyncsIntoEmptyStore(t *testing.T) {
	src, root := buildTree(t)
	dest := openStore(t)
	sendErr, recvErr := runLoop(t, src, dest, root)
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
	if se, re := runLoop(t, src, dest, root); se != nil || re != nil {
		t.Fatalf("first sync: send=%v recv=%v", se, re)
	}
	if se, re := runLoop(t, src, dest, root); se != nil || re != nil {
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
	if se, re := runLoop(t, src, dest, root); se != nil || re != nil {
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
	err := Receive(b, dest, root, 0)
	if err == nil {
		t.Fatal("undelivered wants must fail the loop, not succeed")
	}
	if !strings.Contains(err.Error(), "omitted 1 of 1") {
		t.Fatalf("error must name the missing wants: %v", err)
	}
}

// TestLoopSenderMissingObject syncs from a sender that lacks the tree:
// the sender must report a remote error and the receiver must fail, not
// hang or succeed.
func TestLoopSenderMissingObject(t *testing.T) {
	_, root := buildTree(t)
	emptySrc := openStore(t)
	dest := openStore(t)
	sendErr, recvErr := runLoop(t, emptySrc, dest, root)
	if !errors.Is(sendErr, packstore.ErrNotFound) {
		t.Fatalf("sender error: %v", sendErr)
	}
	var re *protocol.RemoteError
	if !errors.As(recvErr, &re) || re.Code != protocol.CodeInternal {
		t.Fatalf("receiver error: %v", recvErr)
	}
}
