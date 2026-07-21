package main

import (
	"fmt"
	"strings"

	"github.com/fables-for-robots/amber-store-core/key"
	"github.com/fables-for-robots/amber-store-core/reference"
	"github.com/fables-for-robots/amber-store-iroh/protocol"
	"github.com/fables-for-robots/amber-store-iroh/wantsync"
	"github.com/urfave/cli/v2"
)

func pullCommand() *cli.Command {
	var (
		server string
		addrs  cli.StringSlice
	)
	return &cli.Command{
		Name:      "pull",
		Usage:     "fetch ref NAME (and every missing object below it) from the server and set the local ref",
		ArgsUsage: "NAME",
		Flags:     serverFlags(&server, &addrs),
		Action: func(c *cli.Context) error {
			if c.NArg() != 1 {
				return fmt.Errorf("pull requires exactly one NAME argument, got %d", c.NArg())
			}
			return runPull(c, server, addrs.Value(), c.Args().First())
		},
	}
}

func runPull(c *cli.Context, server string, addrs []string, name string) error {
	if err := reference.ValidateName(name); err != nil {
		return err
	}
	if strings.HasPrefix(name, trackingPrefix) {
		return fmt.Errorf("ref %q: the %q namespace is reserved for remote-tracking refs", name, trackingPrefix)
	}
	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	defer closeStore(objects, refs)

	conn, closeConn, err := dialServer(c.Context, server, addrs)
	if err != nil {
		return err
	}
	defer closeConn()
	stream, err := conn.OpenStreamConn(c.Context)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}

	if err := protocol.WriteMsg(stream, protocol.Msg{Type: protocol.TPull, Name: name}); err != nil {
		return err
	}
	m, err := protocol.ReadMsg(stream)
	if err != nil {
		return err
	}
	switch m.Type {
	case protocol.TRef:
	case protocol.TErr:
		return protocol.RemoteFromMsg(m)
	default:
		return fmt.Errorf("%w: type %d, want TRef", protocol.ErrProtocol, m.Type)
	}
	rec, err := reference.Decode(m.Record)
	if err != nil {
		return fmt.Errorf("server ref record: %w", err)
	}
	root, err := key.Parse(rec.Key)
	if err != nil {
		return fmt.Errorf("server ref record key: %w", err)
	}

	if err := wantsync.Receive(stream, objects, root, 0); err != nil {
		return err
	}
	if err := stream.Close(); err != nil {
		return err
	}

	// Store the server's record verbatim under both the local name and
	// the tracking name: verbatim keeps opaque signature fields intact,
	// and the tracking copy is the next push's CAS expectation. The
	// local update is unconditional — the store is single-owner.
	if err := refs.Put(name, m.Record); err != nil {
		return err
	}
	if err := refs.Put(trackingRef(server, name), m.Record); err != nil {
		return fmt.Errorf("pull succeeded but recording tracking ref failed: %w", err)
	}
	fmt.Fprintf(c.App.Writer, "%s <- %s\n", name, root)
	return nil
}
