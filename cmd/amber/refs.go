package main

import (
	"fmt"
	"time"

	"github.com/amber-store/core/key"
	"github.com/amber-store/transport-iroh/protocol"
	"github.com/urfave/cli/v2"
)

func refsCommand() *cli.Command {
	var (
		server   string
		addrs    cli.StringSlice
		relayURL string
	)
	return &cli.Command{
		Name:  "refs",
		Usage: "list the references on the server: name, key, creation time, creator",
		Flags: serverFlags(&server, &addrs, &relayURL),
		Action: func(c *cli.Context) error {
			if c.NArg() != 0 {
				return fmt.Errorf("refs takes no arguments, got %d", c.NArg())
			}
			return runRefs(c, server, addrs.Value(), relayURL)
		},
	}
}

// runRefs lists remote refs; it needs no local store.
func runRefs(c *cli.Context, server string, addrs []string, relayURL string) error {
	sc, err := dialServer(c.Context, server, addrs, relayURL)
	if err != nil {
		return err
	}
	defer sc.Close()
	stream, err := sc.conn.OpenStreamConn(c.Context)
	if err != nil {
		return fmt.Errorf("open stream: %w", err)
	}
	if err := protocol.WriteMsg(stream, protocol.Msg{Type: protocol.TRefList}); err != nil {
		return err
	}
	m, err := protocol.ReadMsg(stream)
	if err != nil {
		return err
	}
	switch m.Type {
	case protocol.TRefs:
	case protocol.TErr:
		return protocol.RemoteFromMsg(m)
	default:
		return fmt.Errorf("%w: type %d, want TRefs", protocol.ErrProtocol, m.Type)
	}
	if err := stream.Close(); err != nil {
		return err
	}
	for _, r := range m.Refs {
		k, err := key.Parse(r.Key)
		if err != nil {
			return fmt.Errorf("reference %q: %w", r.Name, err)
		}
		line := fmt.Sprintf("%s %s %s", r.Name, k, time.Unix(0, r.CreatedAt).UTC().Format(time.RFC3339))
		if r.User != "" {
			line += " " + r.User
		}
		if _, err := fmt.Fprintln(c.App.Writer, line); err != nil {
			return err
		}
	}
	return nil
}
