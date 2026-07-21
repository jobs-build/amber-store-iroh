package main

import (
	"encoding/hex"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/fables-for-robots/amber-store-core/key"
	"github.com/fables-for-robots/amber-store-core/reference"
	"github.com/fables-for-robots/amber-store-core/refstore"
	"github.com/fables-for-robots/amber-store-iroh/protocol"
	"github.com/fables-for-robots/amber-store-iroh/wantsync"
	"github.com/urfave/cli/v2"
)

func pushCommand() *cli.Command {
	var (
		server   string
		addrs    cli.StringSlice
		relayURL string
		force    bool
	)
	flags := append(serverFlags(&server, &addrs, &relayURL),
		&cli.BoolFlag{
			Name:        "force",
			Usage:       "overwrite the remote ref even if it changed since the last pull/push",
			Destination: &force,
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
			return runPush(c, server, addrs.Value(), relayURL, force, c.Args().First())
		},
	}
}

func runPush(c *cli.Context, server string, addrs []string, relayURL string, force bool, name string) error {
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

	conn, closeConn, err := dialServer(c.Context, server, addrs, relayURL)
	if err != nil {
		return err
	}
	defer closeConn()
	stream, err := conn.OpenStreamConn(c.Context)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	// A QUIC stream is invisible to the server until data flows, so the
	// request goes out before any read.
	req := protocol.Msg{Type: protocol.TPush, Name: name, Root: root[:], CAS: !force, ExpectedOld: expectedOld}
	if err := protocol.WriteMsg(stream, req); err != nil {
		return err
	}
	if err := wantsync.Send(stream, objects); err != nil {
		return pushError(name, err)
	}
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
	fmt.Fprintf(c.App.Writer, "%s -> %s\n", name, root)
	return nil
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
