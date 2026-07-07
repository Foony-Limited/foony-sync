// Derived from github.com/emoss08/gtc (MIT). See LICENSE in this directory.
//
// Modifications from the upstream Reader:
//   - Replaced the ports.WALEventHandler/Sink interface with a plain Handler
//     callback and a single Run() entry point so callers don't need FX.
//   - Public configuration is grouped in Config, and sensible defaults are
//     filled in by Run() so callers can pass a partly-zero value.
//   - Removed Stop() in favour of cancelling the context the caller passes in.

package cdc

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Config configures the WAL reader.
type Config struct {
	// DatabaseURL is the Postgres connection string. It MUST include
	// "?replication=database" so pgconn opens a replication connection.
	DatabaseURL string

	// SlotName is the logical-replication slot name to read from.
	SlotName string

	// PublicationName is the publication name to subscribe to.
	PublicationName string

	// StandbyTimeout is how often we send a standby status (ack) message.
	StandbyTimeout time.Duration

	// ReconnectBackoff is the initial backoff between reconnect attempts.
	ReconnectBackoff time.Duration

	// MaxReconnectBackoff caps the exponential backoff.
	MaxReconnectBackoff time.Duration

	// AutoCreateSlot creates the replication slot on startup if missing.
	AutoCreateSlot bool

	// AutoCreatePub creates (or reconciles) the publication on startup
	// when it does not match the desired Tables list. When Tables is empty
	// the publication is created `FOR ALL TABLES`, otherwise it is created
	// `FOR TABLE t1, t2, ...`.
	AutoCreatePub bool

	// Tables is the list of unqualified `public.<table>` names that should
	// be included in the publication. When nil/empty, the publication is
	// `FOR ALL TABLES`. When non-empty, the reader will reconcile the
	// publication's table list on startup (creating, narrowing, broadening,
	// or altering as needed). Tables that do not yet exist in Postgres are
	// dropped from the list with a warning so missing schema doesn't crash
	// the reader. They'll be picked up the next time the cdc pod restarts
	// after the migration lands.
	Tables []string

	// SlotRetryInterval is how often to retry StartReplication if the slot
	// is currently busy (e.g. another reader is still draining).
	SlotRetryInterval time.Duration

	// SlotRetryTimeout caps how long we'll keep retrying StartReplication.
	SlotRetryTimeout time.Duration

	// OnStatus, when set, is told how each reader cycle went: the error when
	// a connect, setup, or streaming attempt fails, and nil once WAL streaming
	// starts. The retry loop never returns these errors (it backs off and
	// tries again), so without this hook a failure like a missing publication
	// stays log-only forever. The agent uses it to put replication health on
	// heartbeats.
	OnStatus func(err error)

	// Logger is used for structured logging. If nil, slog.Default is used.
	Logger *slog.Logger
}

func (c *Config) applyDefaults() {
	if c.StandbyTimeout == 0 {
		c.StandbyTimeout = 10 * time.Second
	}
	if c.ReconnectBackoff == 0 {
		c.ReconnectBackoff = time.Second
	}
	if c.MaxReconnectBackoff == 0 {
		c.MaxReconnectBackoff = 30 * time.Second
	}
	if c.SlotRetryInterval == 0 {
		c.SlotRetryInterval = 2 * time.Second
	}
	if c.SlotRetryTimeout == 0 {
		c.SlotRetryTimeout = 30 * time.Second
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// Run starts a WAL reader loop, calling handler for every decoded event. It
// blocks until ctx is cancelled or an unrecoverable error occurs. The loop
// auto-reconnects with exponential backoff on transient failures.
func Run(ctx context.Context, cfg Config, handler Handler) error {
	cfg.applyDefaults()
	if cfg.DatabaseURL == "" {
		return errors.New("cdc: DatabaseURL is required")
	}
	if cfg.SlotName == "" {
		return errors.New("cdc: SlotName is required")
	}
	if cfg.PublicationName == "" {
		return errors.New("cdc: PublicationName is required")
	}

	r := &reader{
		cfg:    cfg,
		logger: cfg.Logger.With(slog.String("component", "wal_reader")),
	}
	return r.start(ctx, handler)
}

type reader struct {
	cfg       Config
	logger    *slog.Logger
	conn      *pgconn.PgConn
	decoder   *decoder
	clientLSN atomic.Uint64
}

func (r *reader) start(ctx context.Context, handler Handler) error {
	r.logger.Info("starting WAL reader",
		slog.String("slot_name", r.cfg.SlotName),
		slog.String("publication", r.cfg.PublicationName),
	)

	backoff := r.cfg.ReconnectBackoff

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if err := r.connect(ctx); err != nil {
			r.logger.Error("connection failed", slog.String("error", err.Error()))
			r.reportStatus(ctx, err)
			r.waitWithBackoff(ctx, backoff)
			backoff = minDuration(backoff*2, r.cfg.MaxReconnectBackoff)
			continue
		}

		if err := r.setupReplication(ctx); err != nil {
			r.logger.Error("replication setup failed", slog.String("error", err.Error()))
			r.reportStatus(ctx, err)
			r.closeConnection(ctx)
			r.waitWithBackoff(ctx, backoff)
			backoff = minDuration(backoff*2, r.cfg.MaxReconnectBackoff)
			continue
		}

		backoff = r.cfg.ReconnectBackoff

		r.logger.Info("WAL streaming started, listening for changes")
		r.reportStatus(ctx, nil)
		if err := r.streamLoop(ctx, handler); err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			r.logger.Error("stream loop failed, will reconnect",
				slog.String("error", err.Error()),
				slog.Duration("backoff", backoff),
			)
			r.reportStatus(ctx, fmt.Errorf("replication stream: %w", err))
			r.closeConnection(ctx)
			r.waitWithBackoff(ctx, backoff)
			backoff = minDuration(backoff*2, r.cfg.MaxReconnectBackoff)
			continue
		}
		return nil
	}
}

// reportStatus forwards a cycle outcome to OnStatus. Failures caused by the
// caller cancelling ctx are shutdown noise, not health, so they are dropped.
func (r *reader) reportStatus(ctx context.Context, err error) {
	if r.cfg.OnStatus == nil {
		return
	}
	if err != nil && ctx.Err() != nil {
		return
	}
	r.cfg.OnStatus(err)
}

func (r *reader) waitWithBackoff(ctx context.Context, backoff time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(backoff):
	}
}

func (r *reader) closeConnection(ctx context.Context) {
	if r.conn != nil {
		_ = r.conn.Close(ctx)
		r.conn = nil
	}
}

func (r *reader) connect(ctx context.Context) error {
	conn, err := pgconn.Connect(ctx, r.cfg.DatabaseURL)
	if err != nil {
		return fmt.Errorf("connect to postgres: %w", err)
	}
	r.conn = conn
	// Fresh decoder per connection. The server re-sends Relation messages on
	// a new session, and a connection that dropped mid streamed transaction
	// would otherwise leave inStream set, making ParseV2 read a phantom Xid
	// off every following message and silently discard events.
	r.decoder = newDecoder()
	r.logger.Info("connected to postgres")
	return nil
}

func (r *reader) setupReplication(ctx context.Context) error {
	sysident, err := pglogrepl.IdentifySystem(ctx, r.conn)
	if err != nil {
		return fmt.Errorf("identify system: %w", err)
	}
	r.logger.Info("system identified",
		slog.String("system_id", sysident.SystemID),
		slog.Int("timeline", int(sysident.Timeline)),
		slog.String("xlog_pos", sysident.XLogPos.String()),
		slog.String("db_name", sysident.DBName),
	)

	if err := r.ensurePublication(ctx); err != nil {
		return err
	}

	startLSN, err := r.ensureReplicationSlot(ctx)
	if err != nil {
		return err
	}

	if err := r.startReplicationWithRetry(ctx, startLSN); err != nil {
		return err
	}

	r.clientLSN.Store(uint64(startLSN))
	r.logger.Info("replication started", slog.String("lsn", startLSN.String()))
	return nil
}

// ensurePublication brings the named publication in line with cfg.Tables.
//
// State machine:
//
//	            desired = ALL              desired = scoped list
//	exists=no   CREATE FOR ALL TABLES      CREATE FOR TABLE ...
//	exists,all  no-op                      DROP + CREATE FOR TABLE ...
//	exists,scoped DROP + CREATE ALL        ALTER ... SET TABLE ... (if list differs)
//
// Postgres does not allow ALTER PUBLICATION to flip between FOR ALL TABLES
// and FOR TABLE, so we drop and recreate in those cases. The cdc binary is
// deployed as a singleton with a `Recreate` strategy, so there's no other
// reader actively consuming the publication at startup.
func (r *reader) ensurePublication(ctx context.Context) error {
	for _, t := range r.cfg.Tables {
		if !isSafeIdentifier(t) {
			return fmt.Errorf("invalid table name in cdc Tables: %q", t)
		}
	}

	desiredTables, err := r.resolveDesiredTables(ctx)
	if err != nil {
		return err
	}
	desiredAll := len(r.cfg.Tables) == 0

	exists, currentAllTables, err := r.fetchPublicationState(ctx)
	if err != nil {
		return err
	}

	if !exists {
		if !r.cfg.AutoCreatePub {
			return fmt.Errorf("publication %q does not exist and auto-create is disabled", r.cfg.PublicationName)
		}
		return r.createPublication(ctx, desiredAll, desiredTables)
	}

	if desiredAll && currentAllTables {
		r.logger.Info("publication already FOR ALL TABLES",
			slog.String("publication", r.cfg.PublicationName),
		)
		return nil
	}
	if desiredAll && !currentAllTables {
		if !r.cfg.AutoCreatePub {
			return fmt.Errorf("publication %q is scoped but FOR ALL TABLES requested; auto-create is disabled", r.cfg.PublicationName)
		}
		r.logger.Warn("recreating publication: widening to FOR ALL TABLES",
			slog.String("publication", r.cfg.PublicationName),
		)
		if err := r.dropPublication(ctx); err != nil {
			return err
		}
		return r.createPublication(ctx, true, nil)
	}
	if !desiredAll && currentAllTables {
		if !r.cfg.AutoCreatePub {
			return fmt.Errorf("publication %q is FOR ALL TABLES but a scoped list was requested; auto-create is disabled", r.cfg.PublicationName)
		}
		r.logger.Warn("recreating publication: narrowing to scoped table list",
			slog.String("publication", r.cfg.PublicationName),
			slog.Int("table_count", len(desiredTables)),
		)
		if err := r.dropPublication(ctx); err != nil {
			return err
		}
		return r.createPublication(ctx, false, desiredTables)
	}
	return r.syncPublicationTables(ctx, desiredTables)
}

// resolveDesiredTables filters cfg.Tables to those that actually exist in the
// `public` schema. Missing tables produce a warning but don't fail startup.
func (r *reader) resolveDesiredTables(ctx context.Context) ([]string, error) {
	if len(r.cfg.Tables) == 0 {
		return nil, nil
	}
	quoted := make([]string, len(r.cfg.Tables))
	for i, t := range r.cfg.Tables {
		quoted[i] = "'" + t + "'"
	}
	sql := fmt.Sprintf(
		"SELECT tablename FROM pg_catalog.pg_tables WHERE schemaname = 'public' AND tablename IN (%s)",
		strings.Join(quoted, ", "),
	)
	res := r.conn.Exec(ctx, sql)
	rows, err := res.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("check tables: %w", err)
	}
	found := make(map[string]struct{}, len(r.cfg.Tables))
	if len(rows) > 0 {
		for _, row := range rows[0].Rows {
			found[string(row[0])] = struct{}{}
		}
	}
	existing := make([]string, 0, len(r.cfg.Tables))
	missing := make([]string, 0)
	for _, t := range r.cfg.Tables {
		if _, ok := found[t]; ok {
			existing = append(existing, t)
		} else {
			missing = append(missing, t)
		}
	}
	if len(missing) > 0 {
		r.logger.Warn("skipping tables that don't exist in postgres",
			slog.Any("missing", missing),
		)
	}
	if len(existing) == 0 {
		return nil, fmt.Errorf("none of the configured tables exist in postgres: %v", r.cfg.Tables)
	}
	return existing, nil
}

func (r *reader) fetchPublicationState(ctx context.Context) (exists, allTables bool, err error) {
	res := r.conn.Exec(ctx, fmt.Sprintf(
		"SELECT puballtables FROM pg_publication WHERE pubname = '%s'",
		r.cfg.PublicationName,
	))
	rows, err := res.ReadAll()
	if err != nil {
		return false, false, fmt.Errorf("check publication: %w", err)
	}
	if len(rows) == 0 || len(rows[0].Rows) == 0 {
		return false, false, nil
	}
	val := strings.TrimSpace(string(rows[0].Rows[0][0]))
	return true, val == "t" || val == "true", nil
}

func (r *reader) createPublication(ctx context.Context, forAllTables bool, tables []string) error {
	var sql string
	if forAllTables {
		sql = fmt.Sprintf("CREATE PUBLICATION %s FOR ALL TABLES", r.cfg.PublicationName)
		r.logger.Info("creating publication FOR ALL TABLES",
			slog.String("publication", r.cfg.PublicationName),
		)
	} else {
		sql = fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", r.cfg.PublicationName, strings.Join(tables, ", "))
		r.logger.Info("creating scoped publication",
			slog.String("publication", r.cfg.PublicationName),
			slog.Int("table_count", len(tables)),
			slog.Any("tables", tables),
		)
	}
	if _, err := r.conn.Exec(ctx, sql).ReadAll(); err != nil {
		return fmt.Errorf("create publication: %w%s", err, publicationPermissionHint(err, sql))
	}
	return nil
}

// publicationPermissionHint turns a bare "permission denied" from publication
// DDL into an actionable message. Creating or altering a publication needs
// CREATE on the database plus ownership of every published table, which a
// least-privilege replication role deliberately lacks, so the fix is a
// superuser running the statement, and the hint carries it verbatim.
func publicationPermissionHint(err error, sql string) string {
	var pgError *pgconn.PgError
	if !errors.As(err, &pgError) || pgError.Code != "42501" {
		return ""
	}
	return fmt.Sprintf(" (the connecting role cannot manage publications; run this once as a superuser: %s)", sql)
}

func (r *reader) dropPublication(ctx context.Context) error {
	sql := fmt.Sprintf("DROP PUBLICATION IF EXISTS %s", r.cfg.PublicationName)
	if _, err := r.conn.Exec(ctx, sql).ReadAll(); err != nil {
		return fmt.Errorf("drop publication: %w%s", err, publicationPermissionHint(err, sql))
	}
	return nil
}

// syncPublicationTables reconciles a scoped publication's table list to the
// desired set using ALTER PUBLICATION ... SET TABLE.
func (r *reader) syncPublicationTables(ctx context.Context, desired []string) error {
	current, err := r.currentPublicationTables(ctx)
	if err != nil {
		return err
	}
	if equalStringSets(current, desired) {
		r.logger.Info("publication already in desired shape",
			slog.String("publication", r.cfg.PublicationName),
			slog.Int("table_count", len(desired)),
		)
		return nil
	}
	if !r.cfg.AutoCreatePub {
		return fmt.Errorf("publication %q table list does not match desired and auto-create is disabled", r.cfg.PublicationName)
	}
	sql := fmt.Sprintf("ALTER PUBLICATION %s SET TABLE %s", r.cfg.PublicationName, strings.Join(desired, ", "))
	r.logger.Info("altering publication table list",
		slog.String("publication", r.cfg.PublicationName),
		slog.Any("from", current),
		slog.Any("to", desired),
	)
	if _, err := r.conn.Exec(ctx, sql).ReadAll(); err != nil {
		return fmt.Errorf("alter publication: %w%s", err, publicationPermissionHint(err, sql))
	}
	return nil
}

func (r *reader) currentPublicationTables(ctx context.Context) ([]string, error) {
	sql := fmt.Sprintf(
		"SELECT tablename FROM pg_catalog.pg_publication_tables WHERE pubname = '%s' AND schemaname = 'public'",
		r.cfg.PublicationName,
	)
	res := r.conn.Exec(ctx, sql)
	rows, err := res.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("read publication tables: %w", err)
	}
	out := make([]string, 0)
	if len(rows) > 0 {
		for _, row := range rows[0].Rows {
			out = append(out, string(row[0]))
		}
	}
	return out, nil
}

func equalStringSets(a, b []string) bool {
	if len(a) != len(b) {
		return false
	}
	set := make(map[string]struct{}, len(a))
	for _, s := range a {
		set[s] = struct{}{}
	}
	for _, s := range b {
		if _, ok := set[s]; !ok {
			return false
		}
	}
	return true
}

// isSafeIdentifier validates that s is a plain SQL identifier we're willing
// to interpolate into DDL. We don't accept quoting, schemas, or dots, so
// tables must live in `public` and follow `[A-Za-z_][A-Za-z0-9_]*`.
func isSafeIdentifier(s string) bool {
	if s == "" {
		return false
	}
	for i, r := range s {
		switch {
		case r == '_':
		case r >= 'a' && r <= 'z':
		case r >= 'A' && r <= 'Z':
		case i > 0 && r >= '0' && r <= '9':
		default:
			return false
		}
	}
	return true
}

func (r *reader) ensureReplicationSlot(ctx context.Context) (pglogrepl.LSN, error) {
	checkResult := r.conn.Exec(ctx, fmt.Sprintf(
		"SELECT confirmed_flush_lsn FROM pg_replication_slots WHERE slot_name = '%s'",
		r.cfg.SlotName,
	))
	rows, err := checkResult.ReadAll()
	if err != nil {
		return 0, fmt.Errorf("check replication slot: %w", err)
	}
	if len(rows) > 0 && len(rows[0].Rows) > 0 && len(rows[0].Rows[0]) > 0 {
		startLSN, parseErr := pglogrepl.ParseLSN(string(rows[0].Rows[0][0]))
		if parseErr != nil {
			return 0, fmt.Errorf("parse confirmed_flush_lsn: %w", parseErr)
		}
		return startLSN, nil
	}
	if !r.cfg.AutoCreateSlot {
		return 0, fmt.Errorf("replication slot %q does not exist and auto-create is disabled", r.cfg.SlotName)
	}

	r.logger.Info("creating replication slot", slog.String("slot_name", r.cfg.SlotName))
	slotResult, err := pglogrepl.CreateReplicationSlot(
		ctx, r.conn, r.cfg.SlotName, "pgoutput",
		pglogrepl.CreateReplicationSlotOptions{Temporary: false},
	)
	if err != nil {
		return 0, fmt.Errorf("create replication slot: %w", err)
	}
	startLSN, err := pglogrepl.ParseLSN(slotResult.ConsistentPoint)
	if err != nil {
		return 0, fmt.Errorf("parse consistent_point: %w", err)
	}
	return startLSN, nil
}

func (r *reader) startReplicationWithRetry(ctx context.Context, startLSN pglogrepl.LSN) error {
	pluginArguments := []string{
		"proto_version '2'",
		fmt.Sprintf("publication_names '%s'", r.cfg.PublicationName),
		"messages 'true'",
		"streaming 'true'",
	}
	deadline := time.Now().Add(r.cfg.SlotRetryTimeout)

	for {
		err := pglogrepl.StartReplication(
			ctx, r.conn, r.cfg.SlotName, startLSN,
			pglogrepl.StartReplicationOptions{PluginArgs: pluginArguments},
		)
		if err == nil {
			return nil
		}
		if time.Now().After(deadline) {
			return fmt.Errorf("start replication after timeout: %w", err)
		}
		r.logger.Warn("replication slot busy, retrying",
			slog.String("error", err.Error()),
			slog.Duration("retry_in", r.cfg.SlotRetryInterval),
		)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(r.cfg.SlotRetryInterval):
		}
	}
}

func (r *reader) streamLoop(ctx context.Context, handler Handler) error {
	nextDeadline := time.Now().Add(r.cfg.StandbyTimeout)

	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		if time.Now().After(nextDeadline) {
			if err := r.sendStandbyStatus(ctx); err != nil {
				return fmt.Errorf("send standby status: %w", err)
			}
			nextDeadline = time.Now().Add(r.cfg.StandbyTimeout)
		}

		msgCtx, cancel := context.WithDeadline(ctx, nextDeadline)
		rawMsg, err := r.conn.ReceiveMessage(msgCtx)
		cancel()

		if err != nil {
			if pgconn.Timeout(err) {
				continue
			}
			return fmt.Errorf("receive message: %w", err)
		}

		if errMsg, ok := rawMsg.(*pgproto3.ErrorResponse); ok {
			r.logger.Error("postgres WAL error",
				slog.String("severity", errMsg.Severity),
				slog.String("code", errMsg.Code),
				slog.String("message", errMsg.Message),
			)
			return fmt.Errorf("postgres WAL error: %s", errMsg.Message)
		}

		result, err := r.decoder.decode(rawMsg)
		if err != nil {
			return fmt.Errorf("decode: %w", err)
		}

		for _, event := range result.events {
			if err := handler(ctx, event); err != nil {
				return fmt.Errorf("handler: %w", err)
			}
		}

		if uint64(result.lsn) > r.clientLSN.Load() {
			r.clientLSN.Store(uint64(result.lsn))
		}
	}
}

func (r *reader) sendStandbyStatus(ctx context.Context) error {
	lsn := pglogrepl.LSN(r.clientLSN.Load())
	return pglogrepl.SendStandbyStatusUpdate(
		ctx, r.conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: lsn},
	)
}

func minDuration(a, b time.Duration) time.Duration {
	if a < b {
		return a
	}
	return b
}
