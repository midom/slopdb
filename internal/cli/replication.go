package cli

import (
	"context"
	"crypto/tls"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"slopdb/db"
	"slopdb/replication"
	"slopdb/storage"
)

type replicationFlags struct {
	recoveryMinimum                     string
	recoverTo, recoverySources          string
	recoveryFenced                      bool
	config                              replication.Config
	tokenEnv, cert, key, ca, serverName string
}

func (f *replicationFlags) register(flags *flag.FlagSet) {
	flags.BoolVar(&f.config.NonVoting, "raft-non-voting", false, "permanent non-voting replica; disable automatic and manual promotion")
	flags.BoolVar(&f.config.LogsOnly, "raft-logs-only", false, "retain Raft logs and recovery snapshots without SQL tables; vote but never lead")
	flags.Func("raft-read-barrier", "default SELECT policy: previous, next, or committed (default: normal reads)", func(s string) error { f.config.ReadBarrier = db.ReadBarrier(s); return nil })
	flags.Func("raft-durability", "replication commit mode: sync or async (default: inherit, sync for a new cluster)", func(s string) error { f.config.Durability = replication.Durability(s); return nil })
	flags.DurationVar(&f.config.DurabilityInterval, "raft-durability-interval", 0, "async WAL group-flush interval (default 100ms)")
	flags.DurationVar(&f.config.MaxDurabilityLag, "raft-max-durability-lag", 0, "reject new writes when the oldest unstable prefix exceeds this age (default max(1s, 10 intervals))")
	flags.DurationVar(&f.config.ReplicaReadMaxStaleness, "raft-replica-read-max-staleness", 0, "allow read-only SQL on replicas within this time bound (default 0: leader only)")
	flags.StringVar(&f.recoveryMinimum, "raft-recovery-minimum-token", "", "JSON durability token that offline recovery must preserve")
	flags.StringVar(&f.recoverTo, "raft-recover-to", "", "offline disaster recovery: export the latest certified prefix to a new standalone seed")
	flags.StringVar(&f.recoverySources, "raft-recovery-sources", "", "comma-separated additional offline voter node directories; positional path is the first source")
	flags.BoolVar(&f.recoveryFenced, "raft-recovery-fenced", false, "assert that every original cluster member has been stopped or externally fenced before offline recovery")
	flags.StringVar(&f.config.ID, "raft-id", "", "persistent unique Raft node ID")
	flags.StringVar(&f.config.Listen, "raft-listen", "", "Raft endpoint: tcp://host:port or unix:///absolute/path")
	flags.StringVar(&f.config.Advertise, "raft-advertise", "", "Raft endpoint advertised to peers (default: bound endpoint)")
	flags.StringVar(&f.config.ControlListen, "raft-control", "", "authenticated clone/membership/status endpoint: tcp://host:port or unix:///path")
	flags.StringVar(&f.config.ControlAdvertise, "raft-control-advertise", "", "control endpoint advertised to peers (default: bound endpoint)")
	flags.StringVar(&f.tokenEnv, "raft-token-env", "", "environment variable holding the shared cluster token (at least 16 bytes)")
	flags.BoolVar(&f.config.Bootstrap, "raft-bootstrap", false, "bootstrap the first node of a new Raft cluster")
	flags.StringVar(&f.config.Join, "raft-join", "", "clone from and join this leader control endpoint as a learner")
	flags.StringVar(&f.config.SeedDirectory, "raft-seed", "", "standalone database to copy when bootstrapping a new node directory")
	flags.BoolVar(&f.config.AutoPromote, "raft-auto-promote", true, "promote a joining learner after durable WAL catch-up")
	flags.StringVar(&f.cert, "raft-tls-cert", "", "PEM node TLS certificate (server and client use)")
	flags.StringVar(&f.key, "raft-tls-key", "", "PEM node TLS private key")
	flags.StringVar(&f.ca, "raft-tls-ca", "", "PEM cluster CA: verify peers and require verified client certificates")
	flags.StringVar(&f.serverName, "raft-tls-server-name", "", "TLS peer DNS name override, including for Unix sockets")
}
func (f *replicationFlags) enabled() bool {
	return f.config.NonVoting || f.config.LogsOnly || f.config.ReadBarrier != db.ReadBarrierDefault || f.config.ReplicaReadMaxStaleness != 0 || f.config.Durability != "" || f.config.DurabilityInterval != 0 || f.config.MaxDurabilityLag != 0 || f.config.ID != "" || f.config.Listen != "" || f.config.ControlListen != "" || f.config.Advertise != "" || f.config.ControlAdvertise != "" || f.config.Bootstrap || f.config.Join != "" || f.config.SeedDirectory != "" || f.tokenEnv != "" || f.cert != "" || f.key != "" || f.ca != "" || f.serverName != ""
}
func (f *replicationFlags) open(directory string, options *storage.Options) (*replication.Node, error) {
	token, ok := os.LookupEnv(f.tokenEnv)
	if f.tokenEnv == "" || !ok {
		return nil, fmt.Errorf("--raft-token-env must name a set environment variable")
	}
	f.config.Token = token
	server, err := loadServerTLS("raft", f.cert, f.key, f.ca, false)
	if err != nil {
		return nil, err
	}
	f.config.TLSConfig = server
	if server != nil {
		client := &tls.Config{Certificates: server.Certificates, MinVersion: tls.VersionTLS12, ServerName: f.serverName}
		if server.ClientCAs != nil {
			client.RootCAs = server.ClientCAs.Clone()
		}
		f.config.ClientTLSConfig = client
	} else if f.serverName != "" {
		return nil, fmt.Errorf("--raft-tls-server-name requires a node TLS certificate/key")
	}
	f.config.Directory = directory
	f.config.StorageOptions = options
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	return replication.Open(ctx, f.config)
}
func runReplicationNode(n *replication.Node) error {
	fmt.Fprintf(os.Stderr, "Raft node %s listening on %s; control %s\n", n.Status().ID, n.Address(), n.ControlAddress())
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	select {
	case <-ctx.Done():
		return nil
	case <-n.Done():
		return n.Err()
	}
}

func (f *replicationFlags) recover(directory string) error {
	token, found := os.LookupEnv(f.tokenEnv)
	if !found || f.tokenEnv == "" {
		return fmt.Errorf("--raft-token-env must name a set environment variable")
	}
	sources := []string{directory}
	if f.recoverySources != "" {
		for _, source := range strings.Split(f.recoverySources, ",") {
			sources = append(sources, strings.TrimSpace(source))
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	var minimum *replication.DurabilityToken
	if f.recoveryMinimum != "" {
		minimum = &replication.DurabilityToken{}
		if err := json.Unmarshal([]byte(f.recoveryMinimum), minimum); err != nil {
			return err
		}
	}
	result, err := replication.RecoverStable(ctx, replication.RecoveryConfig{Sources: sources, Destination: f.recoverTo, Token: token, Fenced: f.recoveryFenced, Minimum: minimum})
	if err != nil {
		return err
	}
	return json.NewEncoder(os.Stdout).Encode(result)
}
