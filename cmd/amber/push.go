package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/core/packstore"
	"github.com/amber-store/core/reference"
	"github.com/amber-store/core/refstore"
	"github.com/jobs-build/amber-store-iroh/protocol"
	"github.com/jobs-build/amber-store-iroh/wantsync"
	"github.com/urfave/cli/v2"
)

func pushCommand() *cli.Command {
	var (
		server     string
		addrs      cli.StringSlice
		relayURL   string
		force      bool
		noProgress bool
		conns      int
	)
	flags := append(serverFlags(&server, &addrs, &relayURL),
		&cli.BoolFlag{
			Name:        "force",
			Usage:       "overwrite the remote ref even if it changed since the last pull/push",
			Destination: &force,
		},
		&cli.BoolFlag{
			Name:        "no-progress",
			Usage:       "disable the progress bar",
			Destination: &noProgress,
		},
		&cli.IntFlag{
			Name:        "conns",
			Value:       4,
			Usage:       "parallel connections for the transfer (1-16; servers without support fall back to 1)",
			Destination: &conns,
		},
	)
	return &cli.Command{
		Name:      "push",
		Usage:     "push local ref NAME (and every missing object below it) to the server",
		ArgsUsage: "NAME",
		Flags:     flags,
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("push requires exactly one NAME argument, got %d", c.NArg())
			}
			return runPush(c, server, addrs.Value(), relayURL, force, noProgress, connsFlag(conns), c.Args().First())
		},
	}
}

func runPush(c *cli.Context, server string, addrs []string, relayURL string, force bool, noProgress bool, conns int, name string) error {
	if strings.HasPrefix(name, trackingPrefix) {
		return fmt.Errorf("ref %q: the %q namespace is reserved for remote-tracking refs", name, trackingPrefix)
	}
	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	defer closeStore(objects, refs)

	root, _, err := resolveSpec(refs, "ref:"+name)
	if err != nil {
		return err
	}

	// The tracking ref records what this store last saw on the server;
	// it is the compare-and-swap expectation. Absent tracking ref means
	// "the server must not have this ref yet".
	var expectedOld []byte
	tname := trackingRef(server, name)
	if raw, err := refs.Get(tname); err == nil {
		rec, err := reference.Decode(raw)
		if err != nil {
			return fmt.Errorf("tracking ref %q: %w", tname, err)
		}
		expectedOld = rec.Key
	} else if !errors.Is(err, refstore.ErrNotFound) {
		return err
	}

	sc, err := dialServer(c.Context, server, addrs, relayURL)
	if err != nil {
		return err
	}
	defer sc.Close()
	stream, err := sc.conn.OpenStreamConn(c.Context)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	// The root key's logical length is the tree's whole footprint — the
	// transfer's upper bound, known before the first round. LIFO: cancel
	// stops the render goroutine, then the Wait drains it, so every
	// return path tears the bar down before printing.
	var xfer *XferProgress
	var pwg sync.WaitGroup
	start := time.Now()
	pctx, pcancel := context.WithCancel(c.Context)
	defer pwg.Wait()
	defer pcancel()
	if !noProgress {
		xfer = NewXferProgress("push", int64(root.Length()))
		isTTY := isTerminal(os.Stderr)
		pwg.Go(func() { xfer.Run(pctx, os.Stderr, start, isTTY) })
	}

	// A QUIC stream is invisible to the server until data flows, so the
	// request goes out before any read.
	req := protocol.Msg{Type: protocol.TPush, Name: name, Root: root[:], CAS: !force, ExpectedOld: expectedOld, DataConns: conns - 1}
	if err := protocol.WriteMsg(stream, req); err != nil {
		return err
	}
	if err := runSenders(c.Context, sc, stream, objects, xfer, conns); err != nil {
		return pushError(name, err)
	}
	xfer.Finish()
	m, err := protocol.ReadMsg(stream)
	if err != nil {
		return err
	}
	switch m.Type {
	case protocol.TOK:
	case protocol.TErr:
		return pushError(name, protocol.RemoteFromMsg(m))
	default:
		return fmt.Errorf("%w: type %d, want TOK", protocol.ErrProtocol, m.Type)
	}
	if err := stream.Close(); err != nil {
		return err
	}

	// Record the new server-side value for the next push's CAS check.
	trec := reference.Reference{Name: tname, Key: root[:], CreatedAt: time.Now().UnixNano()}
	raw, err := trec.Encode()
	if err != nil {
		return err
	}
	if err := refs.Put(tname, raw); err != nil {
		return fmt.Errorf("push succeeded but recording tracking ref failed: %w", err)
	}

	// Tear down the bar before printing, so the summary and result are
	// not repainted over.
	pcancel()
	pwg.Wait()
	if xfer != nil {
		fmt.Fprintln(os.Stderr, xfer.summary(time.Since(start)))
	}
	fmt.Fprintf(c.App.Writer, "%s -> %s\n", name, root)
	return nil
}

// runSenders drives the transfer's sending side. With one connection it
// is exactly the plain Send loop. With more, the first server frame
// decides: TAccept means a sharding-aware server — attach the extra
// connections and run one Send loop per channel; a TWants means an old
// server that ignored the request's DataConns — replay the consumed
// frame in front of the stream and fall back to a single channel; a
// TErr (e.g. cas-mismatch) surfaces as the usual remote error.
func runSenders(ctx context.Context, sc *serverConn, ctrl io.ReadWriter, objects *packstore.Store, xfer *XferProgress, conns int) error {
	if conns <= 1 {
		return wantsync.Send(ctrl, objects, xfer)
	}
	first, err := protocol.ReadMsg(ctrl)
	if err != nil {
		return err
	}
	switch first.Type {
	case protocol.TErr:
		return protocol.RemoteFromMsg(first)
	case protocol.TWants:
		var replay bytes.Buffer
		if err := protocol.WriteMsg(&replay, first); err != nil {
			return err
		}
		fallback := struct {
			io.Reader
			io.Writer
		}{io.MultiReader(&replay, ctrl), ctrl}
		return wantsync.Send(fallback, objects, xfer)
	case protocol.TAccept:
	default:
		return fmt.Errorf("%w: type %d, want TAccept or TWants", protocol.ErrProtocol, first.Type)
	}

	streams, closeStreams := attachExtras(ctx, sc, first.Token, first.DataPorts, conns-1)
	defer closeStreams()
	channels := append([]io.ReadWriter{ctrl}, streams...)
	errs := make([]error, len(channels))
	var wg sync.WaitGroup
	for i, ch := range channels {
		wg.Add(1)
		go func(i int, ch io.ReadWriter) {
			defer wg.Done()
			errs[i] = wantsync.Send(ch, objects, xfer)
		}(i, ch)
	}
	wg.Wait()
	return errors.Join(errs...)
}

// pushError rewrites a cas-mismatch into an actionable message.
func pushError(name string, err error) error {
	var re *protocol.RemoteError
	if !errors.As(err, &re) || re.Code != protocol.CodeCASMismatch {
		return err
	}
	current := "absent"
	if len(re.Current) > 0 {
		if k, kerr := key.Parse(re.Current); kerr == nil {
			current = k.String()
		} else {
			current = hex.EncodeToString(re.Current)
		}
	}
	return fmt.Errorf("remote ref %q changed (now %s): pull first, or push --force", name, current)
}
