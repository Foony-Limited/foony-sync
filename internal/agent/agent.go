// Package agent is foony-sync: the customer-run Database Sync agent. It holds
// the customer's database credentials AND its live-query definitions locally
// (neither ever reaches foony), captures row changes over logical
// replication, recomputes the queries, and publishes the resulting docs to
// the app's reserved db: channels. The only inbound traffic it acts on is a
// warm request naming a channel. See https://foony.io/docs/database-sync.
package agent

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/Foony-Limited/foony-sync/internal/cdc"
	"github.com/Foony-Limited/foony-sync/internal/engine"
	"github.com/Foony-Limited/foony-sync/internal/spec"
)

// Config is everything foony-sync needs: where foony is, the source
// credential, the customer database, and the local definitions file.
type Config struct {
	// FoonyURL is the realtime data-plane base URL (https://realtime.foony.io).
	FoonyURL string
	// AgentKey is the source credential from the dashboard (appSlug.keyId:secret).
	AgentKey string
	// DatabaseURL is the customer Postgres DSN. The agent adds
	// replication=database itself for the WAL connection.
	DatabaseURL string
	// ConfigPath is the local definitions file (foony-sync.json).
	ConfigPath string
	// Version is the agent build version, reported on heartbeats.
	Version string
	Logger  *slog.Logger
}

// Run starts the agent and blocks until ctx ends or startup fails in a way
// retrying cannot fix (bad config, bad credentials, unreachable database).
func Run(ctx context.Context, cfg Config) error {
	logger := cfg.Logger
	if logger == nil {
		logger = slog.Default()
	}
	fileConfig, err := LoadConfig(cfg.ConfigPath)
	if err != nil {
		return err
	}
	resolved := fileConfig.resolved()
	client, err := NewFoonyClient(cfg.FoonyURL, cfg.AgentKey)
	if err != nil {
		return err
	}

	executor, err := engine.NewExecutor(ctx, cfg.DatabaseURL, resolved.statementTimeout, maxDocBytes)
	if err != nil {
		return err
	}
	defer executor.Close()
	if err := executor.Ping(ctx); err != nil {
		return fmt.Errorf("dbsync: customer database unreachable: %w", err)
	}
	checkReplicaIdentities(ctx, executor.Pool(), fileConfig.Queries, logger)

	syncEngine := engine.New(logger, executor, client, fileConfig.Queries)
	// The liveness poll doubles as the startup credential check: a 4xx means
	// a bad or deleted key and is fatal; network trouble retries.
	liveDocs, complete, err := liveDocsWithRetry(ctx, logger, client)
	if err != nil {
		return err
	}
	syncEngine.SyncLive(liveDocs, complete)
	logger.Info("connected to foony",
		"queries", len(fileConfig.Queries),
		"liveDocs", len(liveDocs))
	syncEngine.Start(ctx)

	runner := &agentRunner{
		cfg:       cfg,
		logger:    logger,
		client:    client,
		executor:  executor,
		engine:    syncEngine,
		settings:  resolved,
		summaries: querySummaries(fileConfig.Queries),
		slotName:  slotNameFor(client.KeyID()),
	}

	go runWarmListener(ctx, logger, cfg.FoonyURL, client, func(kind, channel string) {
		if kind == "touch" {
			syncEngine.Touch(channel)
			return
		}
		syncEngine.Warm(channel)
	})
	go runner.runLivenessPolls(ctx)
	go runner.runHeartbeats(ctx)
	runner.runReader(ctx)
	return ctx.Err()
}

// livenessPollInterval is how often the agent asks foony which db: channels
// have subscribers. It bounds two windows: how long an abandoned doc keeps
// recomputing, and how long a warm shed by the rate limiter waits before the
// poll picks the channel up anyway.
const livenessPollInterval = 5 * time.Minute

// runLivenessPolls keeps the engine's live set matched to actual subscriber
// interest. Poll failures keep the current set: when foony is unreachable the
// safe direction is to keep publishing.
func (runner *agentRunner) runLivenessPolls(ctx context.Context) {
	ticker := time.NewTicker(livenessPollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		channels, complete, err := runner.client.LiveDocs(ctx)
		if err != nil {
			if ctx.Err() == nil {
				runner.logger.Warn("liveness poll failed", "error", err.Error())
			}
			continue
		}
		runner.engine.SyncLive(channels, complete)
	}
}

// agentRunner holds the moving parts the background loops share.
type agentRunner struct {
	cfg       Config
	logger    *slog.Logger
	client    *FoonyClient
	executor  *engine.Executor
	engine    *engine.Engine
	settings  settings
	summaries []spec.QuerySummary
	slotName  string

	mutex sync.Mutex
	// state is what the next heartbeat reports.
	state       string
	lastError   string
	lastEventAt *time.Time
	// detached is set once the WAL safety valve fires; the reader stays down
	// (warm requests and the invalidate API keep working) until restart.
	detached bool
	// stopReader asks the reader loop to shut its replication stream (the
	// safety valve firing).
	stopReader chan struct{}
}

// runReader keeps the replication stream alive. Each decoded row change feeds
// the engine; the WAL is acked immediately by the reader's standby loop, so a
// slow refetch can never balloon WAL retention on the customer database. The
// flip side is that a crash between the ack and the recompute loses the event
// from the slot; the startup liveness poll heals that window by marking every
// live doc dirty (see Engine.SyncLive).
func (runner *agentRunner) runReader(ctx context.Context) {
	runner.mutex.Lock()
	runner.stopReader = make(chan struct{}, 1)
	runner.state = "connected"
	runner.mutex.Unlock()
	for ctx.Err() == nil {
		if runner.isDetached() {
			<-ctx.Done()
			return
		}
		tables := runner.engine.WatchedTables()
		readerCtx, cancel := context.WithCancel(ctx)
		go func() {
			select {
			case <-readerCtx.Done():
			case <-runner.stopReader:
				cancel()
			}
		}()
		err := cdc.Run(readerCtx, cdc.Config{
			DatabaseURL:     replicationURL(runner.cfg.DatabaseURL),
			SlotName:        runner.slotName,
			PublicationName: runner.slotName,
			AutoCreateSlot:  true,
			AutoCreatePub:   true,
			Tables:          tables,
			Logger:          runner.logger,
		}, func(ctx context.Context, event cdc.Event) error {
			now := time.Now()
			runner.mutex.Lock()
			runner.lastEventAt = &now
			runner.mutex.Unlock()
			runner.engine.HandleEvent(engine.Event{
				Table:   event.FullTableName(),
				OldData: event.OldData,
				NewData: event.NewData,
			})
			return nil
		})
		cancel()
		if ctx.Err() != nil {
			return
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			runner.setError(err)
			runner.logger.Error("replication stream ended, restarting", "error", err.Error())
			select {
			case <-ctx.Done():
				return
			case <-time.After(5 * time.Second):
			}
		}
	}
}

// runHeartbeats reports health on the configured cadence and runs the WAL
// safety valve: if the slot retains more WAL than the cap, the agent drops
// its own slot rather than fill the customer's disk, and reports the source
// detached.
func (runner *agentRunner) runHeartbeats(ctx context.Context) {
	ticker := time.NewTicker(heartbeatInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
		}
		slotLag := runner.measureSlotLag(ctx)
		if cap := runner.settings.walRetentionCapBytes; cap > 0 && slotLag > cap && !runner.isDetached() {
			runner.detach(ctx, slotLag)
			slotLag = 0
		}
		runner.mutex.Lock()
		heartbeat := Heartbeat{
			AgentVersion: runner.cfg.Version,
			State:        runner.state,
			SlotLagBytes: slotLag,
			DirtyDepth:   runner.engine.DirtyDepth(),
			LastEventAt:  runner.lastEventAt,
			LastError:    runner.lastError,
			Queries:      runner.summaries,
		}
		runner.mutex.Unlock()
		if err := runner.client.SendHeartbeat(ctx, heartbeat); err != nil && ctx.Err() == nil {
			runner.logger.Warn("heartbeat failed", "error", err.Error())
		}
	}
}

// measureSlotLag reads how much WAL the slot retains on the customer
// database, through the regular query pool.
func (runner *agentRunner) measureSlotLag(ctx context.Context) int64 {
	var lag *int64
	err := runner.executor.Pool().QueryRow(ctx,
		`SELECT pg_wal_lsn_diff(pg_current_wal_lsn(), restart_lsn)::bigint
		 FROM pg_replication_slots WHERE slot_name = $1`,
		runner.slotName,
	).Scan(&lag)
	if err != nil || lag == nil {
		return 0
	}
	return *lag
}

// detach is the safety valve: stop replicating and drop the slot so the
// customer database stops retaining WAL for us. Warm requests keep working,
// so subscribers still get docs computed on demand; only change-driven
// freshness is lost until the agent restarts.
func (runner *agentRunner) detach(ctx context.Context, slotLag int64) {
	runner.logger.Error("WAL retention cap exceeded; dropping the replication slot to protect the database",
		"slotLagBytes", slotLag,
		"capBytes", runner.settings.walRetentionCapBytes)
	runner.mutex.Lock()
	runner.detached = true
	runner.state = "detached"
	runner.lastError = fmt.Sprintf("WAL retention exceeded %d bytes; the replication slot was dropped. Restart the agent to re-attach.", runner.settings.walRetentionCapBytes)
	stop := runner.stopReader
	runner.mutex.Unlock()
	// Stop the reader first so the slot is droppable, then drop it.
	select {
	case stop <- struct{}{}:
	default:
	}
	time.Sleep(2 * time.Second)
	if _, err := runner.executor.Pool().Exec(ctx, "SELECT pg_drop_replication_slot($1)", runner.slotName); err != nil {
		runner.logger.Error("dropping the replication slot failed; drop it manually", "slot", runner.slotName, "error", err.Error())
	}
}

func (runner *agentRunner) isDetached() bool {
	runner.mutex.Lock()
	defer runner.mutex.Unlock()
	return runner.detached
}

func (runner *agentRunner) setError(err error) {
	runner.mutex.Lock()
	runner.state = "degraded"
	runner.lastError = err.Error()
	runner.mutex.Unlock()
}

// liveDocsWithRetry fetches the startup liveness poll, retrying transient
// failures: foony being briefly unreachable must not crash-loop a customer's
// deployment, but a 4xx (bad or deleted credential) is fatal.
func liveDocsWithRetry(ctx context.Context, logger *slog.Logger, client *FoonyClient) ([]string, bool, error) {
	backoff := time.Second
	for {
		channels, complete, err := client.LiveDocs(ctx)
		if err == nil {
			return channels, complete, nil
		}
		if strings.Contains(err.Error(), "HTTP 4") {
			return nil, false, err
		}
		logger.Warn("fetching live docs failed, retrying", "error", err.Error(), "backoff", backoff.String())
		select {
		case <-ctx.Done():
			return nil, false, ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

// slotNameFor derives a Postgres-safe replication slot and publication name
// from the agent key's public id (stable per source; key ids are base64url,
// which slot names cannot carry, hence the hex digest).
func slotNameFor(keyID string) string {
	digest := sha256.Sum256([]byte(keyID))
	return "foony_sync_" + hex.EncodeToString(digest[:8])
}

// replicationURL adds the replication=database parameter the WAL connection
// needs, preserving any existing query parameters.
func replicationURL(databaseURL string) string {
	if strings.Contains(databaseURL, "replication=") {
		return databaseURL
	}
	separator := "?"
	if strings.Contains(databaseURL, "?") {
		separator = "&"
	}
	return databaseURL + separator + "replication=database"
}
