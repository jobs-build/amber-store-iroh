// Command amber-serve hosts an amber store over iroh QUIC. Access is
// open: any peer that knows the endpoint ID may push and pull.
package main

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"net/netip"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/amber-store/core/packstore"
	"github.com/amber-store/core/refstore"
	"github.com/jobs-build/amber-store-iroh/protocol"
	"github.com/jobs-build/amber-store-iroh/relaymode"
	"github.com/jobs-build/amber-store-iroh/server"
	"github.com/tmc/go-iroh/dns"
	"github.com/tmc/go-iroh/iroh"
	"github.com/tmc/go-iroh/iroh/mdns"
	irohkey "github.com/tmc/go-iroh/key"
	"github.com/tmc/go-iroh/netaddr"
	"github.com/tmc/go-iroh/relay"
	"github.com/urfave/cli/v2"
)

const shutdownGrace = 10 * time.Second

// serverRelayMode picks the relay fallback: an explicit --relay URL wins;
// otherwise the built-in map is reordered to prefer the lowest-latency
// relay (the stock selection can land on a far-away region). Probing is
// bounded and best-effort — on failure the default map is used as-is.
func serverRelayMode(ctx context.Context, flag string, log *slog.Logger) (relay.Mode, error) {
	if flag != "" {
		return relaymode.FromFlag(flag)
	}
	probeCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	m, err := relay.DefaultMap().PreferNearest(probeCtx, relay.HTTPConnectProber(nil))
	if err != nil {
		log.Warn("relay latency probe failed; using default relay map", "error", err)
		return relay.ModeDefault(), nil
	}
	if urls := m.URLs(); len(urls) > 0 {
		log.Info("preferred relay", "url", urls[0])
	}
	return relay.ModeCustom(m), nil
}

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
			&cli.StringFlag{
				Name:  "relay",
				Usage: "relay server URL to use as the fallback path (default: nearest of the built-in relays)",
			},
			&cli.StringSliceFlag{
				Name:  "advertise-addr",
				Usage: "direct address to advertise, ip or ip:port (repeatable; replaces interface auto-detection)",
			},
			&cli.IntFlag{
				Name:  "data-endpoints",
				Value: 3,
				Usage: "extra UDP endpoints for sharded transfers (0 disables; one socket caps well below fast links)",
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

			relayMode, err := serverRelayMode(ctx, c.String("relay"), log)
			if err != nil {
				return err
			}

			ep, err := iroh.Bind(
				ctx,
				iroh.WithSecretKey(sk),
				iroh.WithALPNs(protocol.ALPN),
				iroh.WithRelayMode(relayMode),
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
			// would leave clients relay-only. Advertise real reachable
			// addresses so peers can dial direct: the operator's
			// --advertise-addr list verbatim, or else the machine's
			// interface addresses on the bound port.
			var direct []netip.AddrPort
			if vals := c.StringSlice("advertise-addr"); len(vals) > 0 {
				direct, err = parseAdvertiseAddrs(vals, ep.LocalAddr().Port())
				if err != nil {
					return err
				}
			} else {
				ifaces, err := localIfaceAddrs()
				if err != nil {
					return fmt.Errorf("interface addresses: %w", err)
				}
				direct = advertisedAddrPorts(ifaces, ep.LocalAddr().Port())
			}
			advertised := ep.Addr()
			directTransport := make([]netaddr.TransportAddr, 0, len(direct))
			for _, ap := range direct {
				ep.AddExternalAddr(ap)
				advertised = advertised.WithIP(ap)
				directTransport = append(directTransport, netaddr.IPAddr{Addr: ap})
			}

			// Advertise the direct addresses on the local link too, so
			// same-LAN clients resolve them over mDNS even when pkarr
			// is stale or unreachable. Start is the listen loop itself,
			// not a launcher — it blocks until ctx ends, so it gets its
			// own goroutine; Publish is safe before Start.
			disc := mdns.New(ep.ID())
			disc.Publish(dns.NewEndpointData(directTransport...))
			go func() {
				if err := disc.Start(ctx); err != nil && ctx.Err() == nil {
					log.Warn("mdns listener stopped", "error", err)
				}
			}()

			// Publish the relay and direct addresses so clients can
			// resolve the endpoint ID over the internet; re-published
			// in the background every 5 minutes.
			pub, err := iroh.N0PkarrPublisher(sk, &iroh.PkarrPublisherConfig{
				AddrFilter: publishableAddrs,
			})
			if err != nil {
				return fmt.Errorf("pkarr publisher: %w", err)
			}
			defer pub.Close()
			pub.Publish(dns.NewEndpointData(advertised.Addrs()...))

			log.Info("server started", "id", ep.ID())
			log.Info("server listening", "addr", advertised)

			srv := server.New(log, objects, refs)

			// Extra data endpoints give sharded transfers separate UDP
			// sockets — one socket's loop caps well below a fast link.
			// Direct-path only (no relay, not published): clients learn
			// the ports in-band and fall back to the control candidates.
			nData := c.Int("data-endpoints")
			if nData < 0 {
				nData = 0
			}
			if nData > 15 {
				nData = 15
			}
			var dataPorts []uint16
			var dataWG sync.WaitGroup
			for i := 0; i < nData; i++ {
				dep, err := iroh.Bind(ctx, iroh.WithSecretKey(sk), iroh.WithALPNs(protocol.ALPN))
				if err != nil {
					return fmt.Errorf("bind data endpoint: %w", err)
				}
				defer dep.Shutdown(context.Background())
				dataPorts = append(dataPorts, dep.LocalAddr().Port())
				dataWG.Add(1)
				go func(dep *iroh.Endpoint) {
					defer dataWG.Done()
					_ = srv.Serve(ctx, dep, shutdownGrace)
				}(dep)
			}
			srv.SetDataPorts(dataPorts)
			if len(dataPorts) > 0 {
				log.Info("data endpoints", "ports", dataPorts)
			}

			err = srv.Serve(ctx, ep, shutdownGrace)
			dataWG.Wait()
			log.Info("server stopped")
			return err
		},
	}

	if err := app.Run(os.Args); err != nil {
		log.Error("run", "error", err)
		os.Exit(1)
	}
}
