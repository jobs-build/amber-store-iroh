// Command amber is the amber-store-iroh client: a full local
// content-addressed store plus push/pull/refs against an amber-serve
// server reached over iroh QUIC.
package main

import (
	"fmt"
	"os"

	"github.com/urfave/cli/v2"
)

func main() {
	if err := newApp().Run(os.Args); err != nil {
		fmt.Fprintf(os.Stderr, "amber: %v\n", err)
		os.Exit(1)
	}
}

// newApp creates the CLI application. Local commands operate on the store
// directory named by --store / $AMBER_STORE; network commands also take
// --server.
func newApp() *cli.App {
	return &cli.App{
		Name:  "amber",
		Usage: "p2p distributed content-addressed filesystem tree store",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "store",
				Usage:   "store directory (layout: <dir>/packstore, <dir>/refs)",
				EnvVars: []string{"AMBER_STORE"},
			},
		},
		Commands: []*cli.Command{
			importCommand(),
			lsCommand(),
			exportCommand(),
			restoreCommand(),
			refCommand(),
			// Task 8 appends: pushCommand(), pullCommand(), refsCommand()
		},
	}
}
