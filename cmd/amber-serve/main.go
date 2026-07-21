// Command amber-serve hosts an amber store over iroh QUIC. Access is
// open: any peer that knows the endpoint ID may push and pull.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"github.com/fables-for-robots/amber-store-core/packstore"
	"github.com/fables-for-robots/amber-store-core/refstore"
	"github.com/fables-for-robots/amber-store-iroh/protocol"
	"github.com/fables-for-robots/amber-store-iroh/server"
	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/iroh"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/relay"
	"github.com/urfave/cli/v2"
)

const shutdownGrace = 10 * time.Second

// loadOrCreateSecretKey reads the hex-encoded secret key from path,
// generating and persisting a fresh one on first run. Deleting the file
// changes the server's endpoint ID.
func loadOrCreateSecretKey(path string) (irohkey.SecretKey, error) {
	b, err := os.ReadFile(path)
	if err == nil {
		sk, err := irohkey.ParseSecretKey(strings.TrimSpace(string(b)))
		if err != nil {
			return irohkey.SecretKey{}, fmt.Errorf("parse key file %s: %w", path, err)
		}
		return sk, nil
	}
	if !errors.Is(err, os.ErrNotExist) {
		return irohkey.SecretKey{}, fmt.Errorf("read key file: %w", err)
	}
	sk, err := irohkey.GenerateSecretKey()
	if err != nil {
		return irohkey.SecretKey{}, err
	}
	seed := sk.Bytes()
	if err := os.WriteFile(path, []byte(hex.EncodeToString(seed[:])+"\n"), 0o600); err != nil {
		return irohkey.SecretKey{}, fmt.Errorf("write key file: %w", err)
	}
	return sk, nil
}

func main() {
	log := slog.New(slog.NewTextHandler(os.Stdout, nil))

	app := &cli.App{
		Name:  "amber-serve",
		Usage: "host an amber store over iroh QUIC",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "store",
				Usage:   "store directory (layout: <dir>/packstore, <dir>/refs)",
				EnvVars: []string{"AMBER_STORE"},
			},
			&cli.StringFlag{
				Name:  "key",
				Value: "server.key",
				Usage: "path to the secret key file (generated on first run)",
			},
		},
		Action: func(c *cli.Context) error {
			dir := c.String("store")
			if dir == "" {
				return fmt.Errorf("no store directory: set --store or $AMBER_STORE")
			}
			objects, err := packstore.Open(filepath.Join(dir, "packstore"), packstore.WithSync(true))
			if err != nil {
				return err
			}
			defer objects.Close()
			refs, err := refstore.Open(filepath.Join(dir, "refs"), true)
			if err != nil {
				return err
			}
			defer refs.Close()

			sk, err := loadOrCreateSecretKey(c.String("key"))
			if err != nil {
				return fmt.Errorf("load secret key: %w", err)
			}

			ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
			defer cancel()

			ep, err := iroh.Bind(
				ctx,
				iroh.WithSecretKey(sk),
				iroh.WithALPNs(protocol.ALPN),
				iroh.WithRelayMode(relay.ModeDefault()),
			)
			if err != nil {
				return fmt.Errorf("bind: %w", err)
			}
			defer ep.Shutdown(context.Background())

			if err := ep.Online(ctx); err != nil {
				return fmt.Errorf("connect to relay: %w", err)
			}

			// The default wildcard bind address is not a dialable
			// candidate and is dropped from published records, which
			// would leave clients relay-only. Advertise the machine's
			// real interface addresses on the bound port so peers can
			// dial direct.
			ifaceAddrs, err := net.InterfaceAddrs()
			if err != nil {
				return fmt.Errorf("interface addresses: %w", err)
			}
			direct := directAddrPorts(ifaceAddrs, ep.LocalAddr().Port())
			advertised := ep.Addr()
			for _, ap := range direct {
				ep.AddExternalAddr(ap)
				advertised = advertised.WithIP(ap)
			}

			// Publish the relay and direct addresses so clients can
			// resolve the endpoint ID over the internet; re-published
			// in the background every 5 minutes.
			pub, err := iroh.N0PkarrPublisher(sk, nil)
			if err != nil {
				return fmt.Errorf("pkarr publisher: %w", err)
			}
			defer pub.Close()
			pub.Publish(dns.NewEndpointData(advertised.Addrs()...))

			log.Info("server started", "id", ep.ID())
			log.Info("server listening", "addr", advertised)

			srv := server.New(log, objects, refs)
			err = srv.Serve(ctx, ep, shutdownGrace)
			log.Info("server stopped")
			return err
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Error("run", "error", err)
		os.Exit(1)
	}
}
