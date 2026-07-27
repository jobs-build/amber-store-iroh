package main

import (
	"fmt"
	"strings"
	"time"

	"github.com/jobs-build/amber-store-core/key"
	"github.com/jobs-build/amber-store-core/reference"
	"github.com/urfave/cli/v2"
)

// trackingPrefix is the reserved local-refstore namespace that records the
// last-seen server-side value of each ref, keyed by server endpoint ID:
// remotes/<endpoint-id>/<name>. Hidden from listings; ref set refuses it.
const trackingPrefix = "remotes/"

func refCommand() *cli.Command {
	return &cli.Command{
		Name:  "ref",
		Usage: "manage references: named pointers to root keys",
		Subcommands: []*cli.Command{
			{
				Name:   "list",
				Usage:  "list every reference: name, key, creation time, creator",
				Action: runRefList,
			},
			{
				Name:      "get",
				Usage:     "print the key a reference points at",
				ArgsUsage: "NAME",
				Action:    runRefGet,
			},
			{
				Name:      "set",
				Usage:     "create or overwrite reference NAME pointing at KEY",
				ArgsUsage: "NAME KEY",
				Action:    runRefSet,
			},
			{
				Name:      "rm",
				Usage:     "delete reference NAME",
				ArgsUsage: "NAME",
				Action:    runRefRm,
			},
		},
	}
}

func runRefList(c *cli.Context) error {
	if c.NArg() != 0 {
		return fmt.Errorf("ref list takes no arguments, got %d", c.NArg())
	}
	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	defer closeStore(objects, refs)
	records, err := refs.All()
	if err != nil {
		return err
	}
	for _, r := range records {
		if strings.HasPrefix(r.Name, trackingPrefix) {
			continue
		}
		rec, err := reference.Decode(r.Data)
		if err != nil {
			return fmt.Errorf("reference %q: %w", r.Name, err)
		}
		k, err := key.Parse(rec.Key)
		if err != nil {
			return fmt.Errorf("reference %q: stored key: %w", r.Name, err)
		}
		line := fmt.Sprintf("%s %s %s", rec.Name, k, time.Unix(0, rec.CreatedAt).UTC().Format(time.RFC3339))
		if rec.User != "" {
			line += " " + rec.User
		}
		if _, err := fmt.Fprintln(c.App.Writer, line); err != nil {
			return err
		}
	}
	return nil
}

func runRefGet(c *cli.Context) error {
	if c.NArg() != 1 {
		return fmt.Errorf("ref get requires exactly one NAME argument, got %d", c.NArg())
	}
	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	defer closeStore(objects, refs)
	k, _, err := resolveSpec(refs, "ref:"+c.Args().First())
	if err != nil {
		return err
	}
	_, err = fmt.Fprintln(c.App.Writer, k.String())
	return err
}

func runRefSet(c *cli.Context) error {
	if c.NArg() != 2 {
		return fmt.Errorf("ref set requires NAME KEY arguments, got %d", c.NArg())
	}
	name := c.Args().Get(0)
	if strings.HasPrefix(name, trackingPrefix) {
		return fmt.Errorf("ref %q: the %q namespace is reserved for remote-tracking refs", name, trackingPrefix)
	}
	k, err := parseHexKey(c.Args().Get(1))
	if err != nil {
		return err
	}
	rec := reference.Reference{
		Name:      name,
		Key:       k[:],
		CreatedAt: time.Now().UnixNano(),
	}
	raw, err := rec.Encode()
	if err != nil {
		return err
	}
	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	if err := refs.Put(name, raw); err != nil {
		closeStore(objects, refs)
		return err
	}
	return closeStore(objects, refs)
}

func runRefRm(c *cli.Context) error {
	if c.NArg() != 1 {
		return fmt.Errorf("ref rm requires exactly one NAME argument, got %d", c.NArg())
	}
	objects, refs, err := openStore(c)
	if err != nil {
		return err
	}
	if err := refs.Delete(c.Args().First()); err != nil {
		closeStore(objects, refs)
		return err
	}
	return closeStore(objects, refs)
}
