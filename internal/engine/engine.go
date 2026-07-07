// Package engine turns customer-database change events into recomputed
// Database Sync docs. It owns the dirty-key set, the per-key coalescing that
// keeps one hot row from stampeding the customer's database, and the mapping
// between doc channels and query definitions. The load-shedding discipline is
// ported from the internal gateway's dispatcher (services/realtime/internal/
// dispatcher): one refetch per dirty key per round, in-flight dedup so a key
// is never computed twice concurrently, and a re-dirty during compute just
// schedules one more round.
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/Foony-Limited/foony-sync/internal/spec"
)

// touchGrace protects a freshly warmed or touched channel from being dropped
// by a liveness poll whose fleet snapshot predates the subscriber's attach.
// It also bounds how long a lost unsubscribe keeps a doc recomputing: a
// channel absent from polls stays live at most this long past its last signal.
const touchGrace = 15 * time.Minute

// workerCount is the refetch parallelism per source. It matches the
// executor's customer-DB pool size on purpose: more workers would just queue
// on connections.
const workerCount = 2

// queryRetryDelay paces the retry of a doc or keysSql query that failed. The
// work goes back on the queue so a transient customer-DB blip cannot leave a
// doc stale forever, and the pause keeps a down database from being retried
// in a tight loop.
const queryRetryDelay = 5 * time.Second

// Publisher delivers a computed doc to its db: channel on foony.
type Publisher interface {
	PublishDoc(ctx context.Context, channel string, doc json.RawMessage) error
}

// Runner executes customer SQL: the doc query and keysSql reverse indexes.
type Runner interface {
	RunDoc(ctx context.Context, sql string, args []any) (json.RawMessage, error)
	// RunKeys returns each result row's values in column order (column i is
	// the value for param $i+1).
	RunKeys(ctx context.Context, sql string, args []any, maxKeys int) ([][]any, error)
}

// Event is one row change from the replication stream, reduced to what the
// engine needs.
type Event struct {
	// Table is the fully qualified "schema.table" name.
	Table   string
	OldData map[string]any
	NewData map[string]any
}

// Engine coalesces change events into doc recomputes. One Engine per source.
type Engine struct {
	logger    *slog.Logger
	runner    Runner
	publisher Publisher

	// queries by name (with each query's param count precomputed from its
	// SQL), and watches indexed by fully qualified table name. Both are
	// immutable after New: definitions come from the local config file,
	// loaded once at startup.
	queries map[string]queryDef
	watches map[string][]watchRef

	mutex sync.Mutex
	// dirty holds channels awaiting a recompute; inflight marks channels a
	// worker is currently computing so a second worker never races it (a
	// re-dirty during compute lands back in dirty and runs next round).
	dirty    map[string]struct{}
	inflight map[string]bool
	// lookups holds pending keysSql reverse-index runs, deduped by (watch,
	// args), with the same inflight discipline as docs. Queueing them keeps
	// the replication handler off the customer database and collapses a hot
	// row's repeated changes into one lookup.
	lookups        map[string]pendingLookup
	lookupInflight map[string]bool
	// live tracks channels with subscriber interest (value = when we last saw
	// evidence: a poll listing, a warm, or a touch). Only live channels are
	// recomputed on change; cold ones wait for a warm. Our own publishes are
	// deliberately NOT evidence, or an abandoned doc on a busy table would
	// stay live forever.
	live map[string]time.Time
	wake chan struct{}
	// retryDelay is queryRetryDelay, held on the struct so tests can shorten it.
	retryDelay time.Duration
	// skippedValues counts param values that could not become a channel
	// segment (charset/length), a per-source diagnostic.
	skippedValues int64
}

type watchRef struct {
	queryName string
	watch     spec.Watch
}

// queryDef is a definition plus its param count, resolved once at New so the
// hot paths never re-scan the SQL.
type queryDef struct {
	spec.Query
	arity int
}

// pendingLookup is one deduped keysSql invocation waiting for a worker.
type pendingLookup struct {
	query queryDef
	watch spec.Watch
	args  []any
}

// New returns an engine over the supplied definitions. Start launches the
// workers.
func New(logger *slog.Logger, runner Runner, publisher Publisher, definitions []spec.Query) *Engine {
	queries := make(map[string]queryDef, len(definitions))
	watches := map[string][]watchRef{}
	for _, query := range definitions {
		queries[query.Name] = queryDef{Query: query, arity: spec.ParamCount(query.SQL)}
		for _, watch := range query.Watches {
			table := qualifiedTable(watch.Table)
			watches[table] = append(watches[table], watchRef{queryName: query.Name, watch: watch})
		}
	}
	return &Engine{
		logger:         logger,
		runner:         runner,
		publisher:      publisher,
		queries:        queries,
		watches:        watches,
		dirty:          map[string]struct{}{},
		inflight:       map[string]bool{},
		lookups:        map[string]pendingLookup{},
		lookupInflight: map[string]bool{},
		live:           map[string]time.Time{},
		wake:           make(chan struct{}, 1),
		retryDelay:     queryRetryDelay,
	}
}

// Start runs the refetch workers until ctx ends.
func (engine *Engine) Start(ctx context.Context) {
	for i := 0; i < workerCount; i++ {
		go engine.runWorker(ctx)
	}
}

// WatchedTables returns the bare (public-schema) table names the definitions
// watch, in the shape the replication reader's publication reconcile expects.
func (engine *Engine) WatchedTables() []string {
	tables := make([]string, 0, len(engine.watches))
	for table := range engine.watches {
		tables = append(tables, strings.TrimPrefix(table, "public."))
	}
	sort.Strings(tables)
	return tables
}

// DirtyDepth reports the pending work backlog (docs awaiting a recompute plus
// queued reverse-index lookups), for heartbeats.
func (engine *Engine) DirtyDepth() int64 {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	return int64(len(engine.dirty) + len(engine.lookups))
}

// SyncLive reconciles the live set against a liveness poll: the fleet's list
// of db: channels with subscribers. Listed channels are (re)leased, unlisted
// ones are dropped unless they were warmed or touched within touchGrace (the
// poll snapshot may predate a fresh subscriber). An incomplete poll (the
// fleet view was truncated or unavailable) only adds, never drops: when we
// cannot see the whole truth, keeping a dead doc warm beats going silent on a
// watched one.
//
// A listed channel that was not already live is also marked dirty, mirroring
// Touch: it may have skipped changes while it was cold (or wrongly dropped by
// an earlier poll). The startup poll takes this path for every doc because
// the live set starts empty, giving each one recompute per process start;
// that heals any change the reader acked but the agent never applied before
// a crash, and lets docs converge after a WAL safety-valve detach + restart.
func (engine *Engine) SyncLive(channels []string, complete bool) {
	now := time.Now()
	engine.mutex.Lock()
	fresh := make(map[string]time.Time, len(channels))
	if !complete {
		fresh = engine.live
	} else {
		for channel, lastSignal := range engine.live {
			if now.Sub(lastSignal) < touchGrace {
				fresh[channel] = lastSignal
			}
		}
	}
	dirtied := false
	for _, channel := range channels {
		if _, _, err := engine.parseChannel(channel); err != nil {
			continue
		}
		if _, wasLive := engine.live[channel]; !wasLive {
			engine.dirty[channel] = struct{}{}
			dirtied = true
		}
		fresh[channel] = now
	}
	engine.live = fresh
	engine.mutex.Unlock()
	if dirtied {
		engine.signal()
	}
}

// Warm handles a warm request: a client attached to a channel whose doc is
// missing from the retained store. The channel becomes live and is computed
// as soon as a worker is free.
func (engine *Engine) Warm(channel string) {
	if _, _, err := engine.parseChannel(channel); err != nil {
		engine.logger.Debug("warm request for unknown channel", "channel", channel, "error", err.Error())
		return
	}
	engine.mutex.Lock()
	engine.live[channel] = time.Now()
	engine.dirty[channel] = struct{}{}
	engine.mutex.Unlock()
	engine.signal()
}

// Touch handles a watcher signal: someone started subscribing to a doc that
// is still retained. A live doc just gets its lease renewed (it has been
// receiving every change, so it is current). A doc that was NOT live may have
// skipped changes while cold, so it is recomputed too.
func (engine *Engine) Touch(channel string) {
	if _, _, err := engine.parseChannel(channel); err != nil {
		engine.logger.Debug("touch for unknown channel", "channel", channel, "error", err.Error())
		return
	}
	engine.mutex.Lock()
	_, wasLive := engine.live[channel]
	engine.live[channel] = time.Now()
	if !wasLive {
		engine.dirty[channel] = struct{}{}
	}
	engine.mutex.Unlock()
	if !wasLive {
		engine.signal()
	}
}

// HandleEvent maps one row change to affected docs and marks them dirty. It
// never touches the customer database: keysSql lookups are queued for the
// workers, so per-event cost is a map lookup plus set inserts and the
// replication stream is never stalled behind SQL.
func (engine *Engine) HandleEvent(event Event) {
	for _, ref := range engine.watches[event.Table] {
		query := engine.queries[ref.queryName]
		if len(ref.watch.Columns) > 0 {
			// Old and new rows both map: an UPDATE that moves a row between
			// keys must refresh the doc it left and the doc it joined.
			engine.markFromRow(query, ref.watch, event.OldData)
			engine.markFromRow(query, ref.watch, event.NewData)
			continue
		}
		if strings.TrimSpace(ref.watch.KeysSQL) == "" {
			// A watch on a param-less query: any change dirties its only doc.
			engine.markDirty(BuildChannel(query.Name, nil))
			continue
		}
		engine.enqueueLookup(query, ref.watch, event)
	}
}

// runWorker drains pending work until ctx ends: reverse-index lookups first
// (each can mark more docs dirty), then doc recomputes.
func (engine *Engine) runWorker(ctx context.Context) {
	for {
		if key, lookup, ok := engine.takeLookup(); ok {
			engine.runLookup(ctx, key, lookup)
			engine.finish(func() { delete(engine.lookupInflight, key) })
			continue
		}
		channel, ok := engine.takeDirty()
		if !ok {
			select {
			case <-ctx.Done():
				return
			case <-engine.wake:
			}
			continue
		}
		engine.compute(ctx, channel)
		engine.finish(func() { delete(engine.inflight, channel) })
	}
}

// finish clears one unit's inflight mark and wakes a worker if work remains
// (a re-dirty or new lookup may have landed while this unit ran).
func (engine *Engine) finish(clearInflight func()) {
	engine.mutex.Lock()
	clearInflight()
	pending := len(engine.dirty) > 0 || len(engine.lookups) > 0
	engine.mutex.Unlock()
	if pending {
		engine.signal()
	}
}

// takeDirty pops one dirty channel that is live and not already being
// computed. Channels no longer live (nobody subscribed) are dropped: the next
// watcher warms or touches them back.
func (engine *Engine) takeDirty() (string, bool) {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	for channel := range engine.dirty {
		if engine.inflight[channel] {
			continue
		}
		delete(engine.dirty, channel)
		if _, isLive := engine.live[channel]; !isLive {
			continue
		}
		engine.inflight[channel] = true
		return channel, true
	}
	return "", false
}

// compute runs the doc query and publishes the result. Zero rows publish a
// JSON null: "this doc does not exist" is a state subscribers must see.
func (engine *Engine) compute(ctx context.Context, channel string) {
	query, values, err := engine.parseChannel(channel)
	if err != nil {
		return
	}
	// Segments bind as text; Postgres casts them from the query's context
	// (pgx encodes Go strings into whatever type each param needs).
	args := make([]any, len(values))
	for index, value := range values {
		args[index] = value
	}
	doc, err := engine.runner.RunDoc(ctx, query.SQL, args)
	if err != nil {
		engine.logger.Warn("doc query failed", "channel", channel, "error", err.Error())
		// Query failures re-dirty the doc just like publish failures below:
		// the channel already left the dirty set, so dropping it here would
		// leave the doc stale until that exact row changes again.
		engine.retryLater(ctx, func() { engine.markDirty(channel) })
		return
	}
	if err := engine.publisher.PublishDoc(ctx, channel, doc); err != nil {
		engine.logger.Warn("doc publish failed", "channel", channel, "error", err.Error())
		// Publish failures re-dirty the doc so it retries once foony is
		// reachable again, instead of staying stale until the next change.
		engine.mutex.Lock()
		engine.dirty[channel] = struct{}{}
		engine.mutex.Unlock()
	}
}

// markFromRow derives one doc key from a changed row via the direct column
// mapping (element i of columns feeds channel segment i) and marks it dirty.
// Rows missing a mapped column (e.g. a DELETE's replica-identity-only old
// row) derive nothing.
func (engine *Engine) markFromRow(query queryDef, watch spec.Watch, row map[string]any) {
	if row == nil {
		return
	}
	values := make([]string, len(watch.Columns))
	for index, column := range watch.Columns {
		raw, ok := row[column]
		if !ok {
			return
		}
		value, ok := channelValue(raw)
		if !ok {
			engine.noteSkippedValue(query.Name, value)
			return
		}
		values[index] = value
	}
	engine.markDirty(BuildChannel(query.Name, values))
}

// enqueueLookup extracts the reverse-index arguments from the changed row and
// queues one keysSql run for the workers. Inputs come from the new row,
// falling back to the old (deletes only carry old data). The dedup key means
// a hot row changed a thousand times costs one lookup, not a thousand.
func (engine *Engine) enqueueLookup(query queryDef, watch spec.Watch, event Event) {
	row := event.NewData
	if row == nil {
		row = event.OldData
	}
	if row == nil {
		return
	}
	args := make([]any, len(watch.KeysColumns))
	for placeholder, column := range watch.KeysColumns {
		index, err := strconv.Atoi(strings.TrimPrefix(placeholder, "$"))
		if err != nil || index < 1 || index > len(args) {
			return
		}
		value, ok := row[column]
		if !ok {
			return
		}
		args[index-1] = value
	}
	// KeysSQL distinguishes two watches of the same query on the same table;
	// %#v keeps differently typed args distinct.
	key := fmt.Sprintf("%s\x00%s\x00%#v", query.Name, watch.KeysSQL, args)
	engine.mutex.Lock()
	engine.lookups[key] = pendingLookup{query: query, watch: watch, args: args}
	engine.mutex.Unlock()
	engine.signal()
}

// takeLookup pops one pending lookup that is not already running, mirroring
// takeDirty: a re-enqueue while a lookup runs lands back in the map and runs
// again next round, so a change mid-lookup is never lost.
func (engine *Engine) takeLookup() (string, pendingLookup, bool) {
	engine.mutex.Lock()
	defer engine.mutex.Unlock()
	for key, lookup := range engine.lookups {
		if engine.lookupInflight[key] {
			continue
		}
		delete(engine.lookups, key)
		engine.lookupInflight[key] = true
		return key, lookup, true
	}
	return "", pendingLookup{}, false
}

// runLookup runs the reverse index against the customer database and marks
// every returned doc key dirty. key is the dedup key takeLookup popped, so a
// failed run can be requeued exactly as a new event for the same row would be.
func (engine *Engine) runLookup(ctx context.Context, key string, lookup pendingLookup) {
	maxKeys := lookup.watch.MaxKeys
	if maxKeys == 0 {
		maxKeys = spec.DefaultMaxKeys
	}
	rows, err := engine.runner.RunKeys(ctx, lookup.watch.KeysSQL, lookup.args, maxKeys)
	if errors.Is(err, ErrTooManyKeys) {
		// Over the cap is permanent for this change: a retry runs the same
		// query into the same cap. The fallback recomputes every live doc of
		// the query, a superset of the affected docs bounded by real
		// subscribers, so nothing goes stale and nothing loops.
		marked := engine.markQueryLiveDirty(lookup.query.Name)
		engine.logger.Warn("keysSql fan-out exceeded maxKeys, recomputing every live doc of the query instead",
			"query", lookup.query.Name, "table", lookup.watch.Table, "args", logValue(lookup.args),
			"maxKeys", maxKeys, "liveDocs", marked)
		return
	}
	if err != nil {
		engine.logger.Warn("keysSql failed", "query", lookup.query.Name, "table", lookup.watch.Table,
			"args", logValue(lookup.args), "error", err.Error())
		// A dropped lookup would silently drop every doc it fans out to, so a
		// transient failure requeues it after the retry pause.
		engine.retryLater(ctx, func() { engine.requeueLookup(key, lookup) })
		return
	}
	for _, keyRow := range rows {
		if len(keyRow) != lookup.query.arity {
			engine.logger.Warn("keysSql must return one column per query param, in param order",
				"query", lookup.query.Name, "table", lookup.watch.Table,
				"columns", len(keyRow), "params", lookup.query.arity)
			return
		}
		values := make([]string, len(keyRow))
		complete := true
		for index, raw := range keyRow {
			value, ok := channelValue(raw)
			if !ok {
				engine.noteSkippedValue(lookup.query.Name, value)
				complete = false
				break
			}
			values[index] = value
		}
		if complete {
			engine.markDirty(BuildChannel(lookup.query.Name, values))
		}
	}
}

func (engine *Engine) markDirty(channel string) {
	engine.mutex.Lock()
	engine.dirty[channel] = struct{}{}
	engine.mutex.Unlock()
	engine.signal()
}

// retryLater waits out the retry delay (or the context) and requeues failed
// work. It runs on the worker goroutine on purpose: against a failing
// customer database, pausing the worker IS the backoff, and it cannot become
// a hot loop because the worker only re-takes the work after the pause.
func (engine *Engine) retryLater(ctx context.Context, requeue func()) {
	select {
	case <-ctx.Done():
	case <-time.After(engine.retryDelay):
	}
	requeue()
}

// requeueLookup puts a failed lookup back in the pending map under its dedup
// key. The key is still marked inflight while this runs; takeLookup skips it
// until the worker's finish clears the mark, exactly like a re-enqueue racing
// a running lookup.
func (engine *Engine) requeueLookup(key string, lookup pendingLookup) {
	engine.mutex.Lock()
	engine.lookups[key] = lookup
	engine.mutex.Unlock()
	engine.signal()
}

func (engine *Engine) signal() {
	select {
	case engine.wake <- struct{}{}:
	default:
	}
}

// noteSkippedValue counts a doc-key value that cannot become a channel
// segment and logs it, value included: the log runs on the customer's own
// infrastructure, so showing their data costs nothing and finding the
// offending rows would otherwise mean guesswork.
func (engine *Engine) noteSkippedValue(queryName, value string) {
	engine.mutex.Lock()
	engine.skippedValues++
	skipped := engine.skippedValues
	engine.mutex.Unlock()
	// Log the first few and then sample, so a high-churn mismatch does not
	// flood the agent's output.
	if skipped <= 5 || skipped%1000 == 0 {
		engine.logger.Warn("param value cannot become a channel segment (allowed: A-Za-z0-9_- up to 64 bytes)",
			"query", queryName, "value", logValue(value), "total", skipped)
	}
}

// markQueryLiveDirty marks every live doc of one query dirty. It is the
// fallback when a reverse index cannot enumerate the affected docs (the
// keysSql fan-out exceeded maxKeys): the query's whole live set is a superset
// of the right answer and is bounded by real subscribers. Returns how many
// docs it marked.
func (engine *Engine) markQueryLiveDirty(queryName string) int {
	exact := BuildChannel(queryName, nil)
	prefix := exact + ":"
	engine.mutex.Lock()
	marked := 0
	for channel := range engine.live {
		if channel == exact || strings.HasPrefix(channel, prefix) {
			engine.dirty[channel] = struct{}{}
			marked++
		}
	}
	engine.mutex.Unlock()
	if marked > 0 {
		engine.signal()
	}
	return marked
}

// logValue renders a customer value (a skipped doc key, keysSql args) for a
// log line, capped so a mapped blob or JSON column cannot dump kilobytes into
// one line.
func logValue(value any) string {
	text := fmt.Sprintf("%v", value)
	if len(text) > maxLoggedValueBytes {
		return text[:maxLoggedValueBytes] + "..."
	}
	return text
}

// maxLoggedValueBytes caps a logged customer value, see logValue.
const maxLoggedValueBytes = 100

// parseChannel resolves a db: channel to its query and raw param values.
func (engine *Engine) parseChannel(channel string) (queryDef, []string, error) {
	if !spec.IsSyncDocChannel(channel) {
		return queryDef{}, nil, fmt.Errorf("not a db: channel: %s", channel)
	}
	segments := strings.Split(channel[len(spec.SyncDocPrefix):], ":")
	query, ok := engine.queries[segments[0]]
	if !ok {
		return queryDef{}, nil, fmt.Errorf("unknown query %q", segments[0])
	}
	values := segments[1:]
	if len(values) != query.arity {
		return queryDef{}, nil, fmt.Errorf("channel %s has %d params, query %s uses %d", channel, len(values), query.Name, query.arity)
	}
	for _, value := range values {
		if !spec.ValidChannelValue(value) {
			return queryDef{}, nil, fmt.Errorf("channel %s has an invalid param segment", channel)
		}
	}
	return query, values, nil
}

// qualifiedTable maps a watch's bare table name to the "schema.table" form
// replication events carry. Watches are public-schema-only in v1, matching the
// reader's publication management.
func qualifiedTable(table string) string {
	return "public." + table
}

// BuildChannel returns the db: doc channel for a query and its param values.
func BuildChannel(queryName string, values []string) string {
	if len(values) == 0 {
		return spec.SyncDocPrefix + queryName
	}
	return spec.SyncDocPrefix + queryName + ":" + strings.Join(values, ":")
}

// channelValue renders a row value as a channel segment. Unsupported types or
// values outside the channel charset yield ok=false and the doc is skipped,
// but the derived text still comes back so the skipped-value log can show
// what was rejected (nil is the exception and returns empty text).
func channelValue(value any) (string, bool) {
	var text string
	switch typed := value.(type) {
	case nil:
		return "", false
	case string:
		text = typed
	case bool:
		text = strconv.FormatBool(typed)
	case int64:
		text = strconv.FormatInt(typed, 10)
	case int32:
		text = strconv.FormatInt(int64(typed), 10)
	case int:
		text = strconv.Itoa(typed)
	case float64:
		// pgoutput decodes numerics as strings, so a float here means a
		// customer mapped a float column, which cannot be a stable key. The
		// text form still comes back for the skipped-value log.
		return strconv.FormatFloat(typed, 'g', -1, 64), false
	case [16]byte:
		text = formatUUID(typed)
	default:
		text = fmt.Sprintf("%v", typed)
	}
	if !spec.ValidChannelValue(text) {
		return text, false
	}
	return text, true
}

func formatUUID(id [16]byte) string {
	var builder strings.Builder
	for index, part := range id {
		switch index {
		case 4, 6, 8, 10:
			builder.WriteByte('-')
		}
		builder.WriteString(fmt.Sprintf("%02x", part))
	}
	return builder.String()
}
