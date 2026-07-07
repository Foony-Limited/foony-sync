// foony-sync is the customer-run Database Sync agent: it connects to the
// customer's own Postgres (credentials never leave their infrastructure),
// captures row changes over logical replication, recomputes registered live
// queries, and publishes the resulting docs to foony's reserved db: channels.
//
// Configuration is the environment plus one local file, matching the docker
// run in the dashboard:
//
//	FOONY_SYNC_KEY     the source credential from the dashboard (appSlug.keyId:secret)
//	DATABASE_URL       the Postgres DSN (needs REPLICATION + SELECT on watched tables)
//	FOONY_SYNC_CONFIG  path to the definitions file (default ./foony-sync.json)
//	FOONY_URL          optional data-plane override (default https://realtime.foony.io)
//
// The definitions file holds the live queries and never leaves this machine;
// the dashboard only ever sees name-and-tables summaries from heartbeats.
package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"syscall"

	"github.com/Foony-Limited/foony-sync/internal/agent"
)

// version is stamped by the build (-ldflags "-X main.version=...").
var version = "dev"

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))
	agentKey := os.Getenv("FOONY_SYNC_KEY")
	databaseURL := os.Getenv("DATABASE_URL")
	if agentKey == "" || databaseURL == "" {
		logger.Error("FOONY_SYNC_KEY and DATABASE_URL are required")
		os.Exit(2)
	}
	foonyURL := os.Getenv("FOONY_URL")
	if foonyURL == "" {
		foonyURL = "https://realtime.foony.io"
	}
	configPath := os.Getenv("FOONY_SYNC_CONFIG")
	if configPath == "" {
		configPath = "foony-sync.json"
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logger.Info("foony-sync starting", "version", version, "foony", foonyURL, "config", configPath)
	if err := agent.Run(ctx, agent.Config{
		FoonyURL:    foonyURL,
		AgentKey:    agentKey,
		DatabaseURL: databaseURL,
		ConfigPath:  configPath,
		Version:     version,
		Logger:      logger,
	}); err != nil && ctx.Err() == nil {
		logger.Error("foony-sync exited", "error", err.Error())
		os.Exit(1)
	}
}
