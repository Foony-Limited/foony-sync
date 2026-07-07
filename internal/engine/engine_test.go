package engine

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"testing"
	"time"

	"github.com/Foony-Limited/foony-sync/internal/spec"
)

// fakeRunner records executed SQL and serves canned results.
type fakeRunner struct {
	mutex    sync.Mutex
	docCalls []string
	docArgs  [][]any
	keysRows [][]any
	docErr   error
	keysErr  error
}

func (runner *fakeRunner) RunDoc(ctx context.Context, sql string, args []any) (json.RawMessage, error) {
	runner.mutex.Lock()
	defer runner.mutex.Unlock()
	if runner.docErr != nil {
		return nil, runner.docErr
	}
	runner.docCalls = append(runner.docCalls, sql)
	runner.docArgs = append(runner.docArgs, args)
	return json.RawMessage(`{"ok":true}`), nil
}

func (runner *fakeRunner) RunKeys(ctx context.Context, sql string, args []any, maxKeys int) ([][]any, error) {
	runner.mutex.Lock()
	defer runner.mutex.Unlock()
	if runner.keysErr != nil {
		return nil, runner.keysErr
	}
	return runner.keysRows, nil
}

// fakePublisher records published docs and signals each publish.
type fakePublisher struct {
	mutex     sync.Mutex
	published map[string]json.RawMessage
	signal    chan string
	err       error
}

func newFakePublisher() *fakePublisher {
	return &fakePublisher{published: map[string]json.RawMessage{}, signal: make(chan string, 64)}
}

func (publisher *fakePublisher) PublishDoc(ctx context.Context, channel string, doc json.RawMessage) error {
	publisher.mutex.Lock()
	defer publisher.mutex.Unlock()
	if publisher.err != nil {
		return publisher.err
	}
	publisher.published[channel] = doc
	select {
	case publisher.signal <- channel:
	default:
	}
	return nil
}

func (publisher *fakePublisher) waitFor(t *testing.T, channel string) {
	t.Helper()
	deadline := time.After(3 * time.Second)
	for {
		publisher.mutex.Lock()
		_, ok := publisher.published[channel]
		publisher.mutex.Unlock()
		if ok {
			return
		}
		select {
		case <-publisher.signal:
		case <-deadline:
			t.Fatalf("doc for %s was never published", channel)
		}
	}
}

func ordersQuery() spec.Query {
	return spec.Query{
		Name: "orders",
		SQL:  "SELECT to_json($1::text)",
		Watches: []spec.Watch{
			{Table: "orders", Columns: []string{"tenant_id"}},
		},
	}
}

// markLive sets channels live directly, bypassing the recompute SyncLive
// schedules for newly live channels, so tests can observe other paths.
func markLive(testEngine *Engine, channels ...string) {
	testEngine.mutex.Lock()
	defer testEngine.mutex.Unlock()
	for _, channel := range channels {
		testEngine.live[channel] = time.Now()
	}
}

func startEngine(t *testing.T, runner Runner, publisher Publisher, queries ...spec.Query) *Engine {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	testEngine := New(slog.Default(), runner, publisher, queries)
	testEngine.Start(ctx)
	return testEngine
}

func TestWarmComputesAndPublishesADoc(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{}
	publisher := newFakePublisher()
	testEngine := startEngine(t, runner, publisher, ordersQuery())

	testEngine.Warm("db:orders:tenant42")
	publisher.waitFor(t, "db:orders:tenant42")

	runner.mutex.Lock()
	defer runner.mutex.Unlock()
	if len(runner.docArgs) != 1 || runner.docArgs[0][0] != "tenant42" {
		t.Fatalf("doc args = %v, want [tenant42]", runner.docArgs)
	}
}

func TestWarmIgnoresUnknownAndMalformedChannels(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{}
	publisher := newFakePublisher()
	testEngine := startEngine(t, runner, publisher, ordersQuery())

	testEngine.Warm("db:unknown:x")
	testEngine.Warm("db:orders")            // arity mismatch
	testEngine.Warm("db:orders:a:b")        // arity mismatch
	testEngine.Warm("chat:orders:tenant42") // not a doc channel
	testEngine.Warm("db:orders:tenant42")
	publisher.waitFor(t, "db:orders:tenant42")

	publisher.mutex.Lock()
	defer publisher.mutex.Unlock()
	if len(publisher.published) != 1 {
		t.Fatalf("published %d docs, want 1: %v", len(publisher.published), publisher.published)
	}
}

func TestEventRefreshesOnlyLiveDocs(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{}
	publisher := newFakePublisher()
	testEngine := startEngine(t, runner, publisher, ordersQuery())
	markLive(testEngine, "db:orders:live1")

	testEngine.HandleEvent(Event{
		Table:   "public.orders",
		NewData: map[string]any{"tenant_id": "live1"},
	})
	testEngine.HandleEvent(Event{
		Table:   "public.orders",
		NewData: map[string]any{"tenant_id": "cold1"},
	})
	publisher.waitFor(t, "db:orders:live1")

	publisher.mutex.Lock()
	defer publisher.mutex.Unlock()
	if _, ok := publisher.published["db:orders:cold1"]; ok {
		t.Fatal("cold doc was recomputed; only live docs should refresh on change")
	}
}

func TestUpdateMovingARowRefreshesBothDocs(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{}
	publisher := newFakePublisher()
	testEngine := startEngine(t, runner, publisher, ordersQuery())
	markLive(testEngine, "db:orders:before", "db:orders:after")

	testEngine.HandleEvent(Event{
		Table:   "public.orders",
		OldData: map[string]any{"tenant_id": "before"},
		NewData: map[string]any{"tenant_id": "after"},
	})
	publisher.waitFor(t, "db:orders:before")
	publisher.waitFor(t, "db:orders:after")
}

func TestKeysSQLWatchFansOutToReturnedKeys(t *testing.T) {
	t.Parallel()
	query := ordersQuery()
	query.Watches = append(query.Watches, spec.Watch{
		Table:       "users",
		KeysSQL:     "SELECT tenant_id FROM x WHERE user_id = $1",
		KeysColumns: map[string]string{"$1": "id"},
	})
	runner := &fakeRunner{keysRows: [][]any{
		{"t1"},
		{"t2"},
	}}
	publisher := newFakePublisher()
	testEngine := startEngine(t, runner, publisher, query)
	markLive(testEngine, "db:orders:t1", "db:orders:t2")

	testEngine.HandleEvent(Event{
		Table:   "public.users",
		NewData: map[string]any{"id": "u1"},
	})
	publisher.waitFor(t, "db:orders:t1")
	publisher.waitFor(t, "db:orders:t2")
}

func TestIdenticalLookupsCoalesce(t *testing.T) {
	t.Parallel()
	query := ordersQuery()
	query.Watches = []spec.Watch{{
		Table:       "users",
		KeysSQL:     "SELECT tenant_id FROM x WHERE user_id = $1",
		KeysColumns: map[string]string{"$1": "id"},
	}}
	// No Start: the engine only enqueues, so the pending set is observable.
	testEngine := New(slog.Default(), &fakeRunner{}, newFakePublisher(), []spec.Query{query})

	for i := 0; i < 100; i++ {
		testEngine.HandleEvent(Event{
			Table:   "public.users",
			NewData: map[string]any{"id": "hot-user"},
		})
	}
	testEngine.HandleEvent(Event{
		Table:   "public.users",
		NewData: map[string]any{"id": "other-user"},
	})

	testEngine.mutex.Lock()
	defer testEngine.mutex.Unlock()
	if len(testEngine.lookups) != 2 {
		t.Fatalf("pending lookups = %d, want 2 (hot row deduped, distinct row kept)", len(testEngine.lookups))
	}
}

func TestTouchRenewsLiveDocsAndRecomputesColdOnes(t *testing.T) {
	t.Parallel()
	// No Start: the dirty set is observable without workers draining it.
	testEngine := New(slog.Default(), &fakeRunner{}, newFakePublisher(), []spec.Query{ordersQuery()})
	markLive(testEngine, "db:orders:live1")

	testEngine.Touch("db:orders:live1")
	testEngine.Touch("db:orders:cold1")

	testEngine.mutex.Lock()
	defer testEngine.mutex.Unlock()
	if _, dirtied := testEngine.dirty["db:orders:live1"]; dirtied {
		t.Fatal("touching a live doc must not recompute it (it has seen every change)")
	}
	if _, dirtied := testEngine.dirty["db:orders:cold1"]; !dirtied {
		t.Fatal("touching a cold doc must recompute it (it may have skipped changes)")
	}
	if _, live := testEngine.live["db:orders:cold1"]; !live {
		t.Fatal("a touched doc must become live")
	}
}

func TestSyncLiveDropsAbandonedDocsButHonorsGraceAndIncompletePolls(t *testing.T) {
	t.Parallel()
	testEngine := New(slog.Default(), &fakeRunner{}, newFakePublisher(), []spec.Query{ordersQuery()})
	testEngine.SyncLive([]string{"db:orders:kept", "db:orders:fresh", "db:orders:gone"}, true)

	// Backdate "gone" past the grace window; "fresh" stays recent, simulating
	// a warm that raced the poll snapshot.
	testEngine.mutex.Lock()
	testEngine.live["db:orders:gone"] = time.Now().Add(-touchGrace - time.Minute)
	testEngine.mutex.Unlock()

	// An incomplete poll must not drop anything it cannot see.
	testEngine.SyncLive([]string{}, false)
	testEngine.mutex.Lock()
	if len(testEngine.live) != 3 {
		t.Fatalf("live after incomplete poll = %d docs, want all 3 kept", len(testEngine.live))
	}
	testEngine.mutex.Unlock()

	testEngine.SyncLive([]string{"db:orders:kept"}, true)
	testEngine.mutex.Lock()
	defer testEngine.mutex.Unlock()
	if _, live := testEngine.live["db:orders:kept"]; !live {
		t.Fatal("a doc listed by the poll must stay live")
	}
	if _, live := testEngine.live["db:orders:fresh"]; !live {
		t.Fatal("a recently signaled doc must survive an unlisting poll (grace)")
	}
	if _, live := testEngine.live["db:orders:gone"]; live {
		t.Fatal("an abandoned doc past grace must be dropped by a complete poll")
	}
}

func TestSyncLiveDirtiesNewlyLiveChannels(t *testing.T) {
	t.Parallel()
	// No Start: the dirty set is observable without workers draining it.
	testEngine := New(slog.Default(), &fakeRunner{}, newFakePublisher(), []spec.Query{ordersQuery()})

	// The startup poll: the live set is empty, so every listed doc gets one
	// recompute. This heals changes the reader acked but the agent never
	// applied before a crash.
	testEngine.SyncLive([]string{"db:orders:a", "db:orders:b"}, true)
	testEngine.mutex.Lock()
	if len(testEngine.dirty) != 2 {
		t.Fatalf("dirty after the startup poll = %d channels, want 2", len(testEngine.dirty))
	}
	testEngine.dirty = map[string]struct{}{}
	testEngine.mutex.Unlock()

	// A later poll: a channel that stayed live has seen every change and must
	// not recompute; one that just became live may have skipped changes.
	testEngine.SyncLive([]string{"db:orders:a", "db:orders:c"}, true)
	testEngine.mutex.Lock()
	defer testEngine.mutex.Unlock()
	if _, dirtied := testEngine.dirty["db:orders:a"]; dirtied {
		t.Fatal("a channel that stayed live must not be recomputed by a poll")
	}
	if _, dirtied := testEngine.dirty["db:orders:c"]; !dirtied {
		t.Fatal("a channel that just became live must be recomputed")
	}
}

func TestDocQueryFailureRetriesTheDoc(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{docErr: fmt.Errorf("statement timeout")}
	publisher := newFakePublisher()
	testEngine := New(slog.Default(), runner, publisher, []spec.Query{ordersQuery()})
	testEngine.retryDelay = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	testEngine.Start(ctx)

	testEngine.Warm("db:orders:tenant42")
	time.Sleep(50 * time.Millisecond)
	runner.mutex.Lock()
	runner.docErr = nil
	runner.mutex.Unlock()
	// The failed query re-dirtied the doc after the retry delay, so it
	// recomputes on its own once the database recovers.
	publisher.waitFor(t, "db:orders:tenant42")
}

func TestKeysQueryFailureRetriesTheLookup(t *testing.T) {
	t.Parallel()
	query := ordersQuery()
	query.Watches = []spec.Watch{{
		Table:       "users",
		KeysSQL:     "SELECT tenant_id FROM x WHERE user_id = $1",
		KeysColumns: map[string]string{"$1": "id"},
	}}
	runner := &fakeRunner{
		keysErr:  fmt.Errorf("connection refused"),
		keysRows: [][]any{{"t1"}},
	}
	publisher := newFakePublisher()
	testEngine := New(slog.Default(), runner, publisher, []spec.Query{query})
	testEngine.retryDelay = 5 * time.Millisecond
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	testEngine.Start(ctx)
	// markLive, not SyncLive: SyncLive would dirty the newly live channel and
	// publish the doc before the lookup retry gets exercised.
	markLive(testEngine, "db:orders:t1")

	testEngine.HandleEvent(Event{
		Table:   "public.users",
		NewData: map[string]any{"id": "u1"},
	})
	time.Sleep(50 * time.Millisecond)
	runner.mutex.Lock()
	runner.keysErr = nil
	runner.mutex.Unlock()
	// The failed lookup was requeued, so its fan-out lands once the database
	// recovers.
	publisher.waitFor(t, "db:orders:t1")
}

func TestPublishFailureRetriesTheDoc(t *testing.T) {
	t.Parallel()
	runner := &fakeRunner{}
	publisher := newFakePublisher()
	publisher.err = fmt.Errorf("foony unreachable")
	testEngine := startEngine(t, runner, publisher, ordersQuery())

	testEngine.Warm("db:orders:tenant42")
	time.Sleep(100 * time.Millisecond)
	publisher.mutex.Lock()
	publisher.err = nil
	publisher.mutex.Unlock()
	// The failed publish left the doc dirty; any wake recomputes it.
	testEngine.Warm("db:orders:tenant42")
	publisher.waitFor(t, "db:orders:tenant42")
}

func TestChannelValueRejectsUnkeyableValues(t *testing.T) {
	t.Parallel()
	if _, ok := channelValue(nil); ok {
		t.Fatal("nil should not become a channel segment")
	}
	if _, ok := channelValue("has space"); ok {
		t.Fatal("invalid charset should not become a channel segment")
	}
	if _, ok := channelValue(1.5); ok {
		t.Fatal("floats should not become channel segments")
	}
	value, ok := channelValue(int64(42))
	if !ok || value != "42" {
		t.Fatalf("channelValue(42) = %q, %v", value, ok)
	}
}

func TestKeysRowWidthMustMatchParamCount(t *testing.T) {
	t.Parallel()
	query := ordersQuery()
	query.Watches = []spec.Watch{{
		Table:       "users",
		KeysSQL:     "SELECT tenant_id, extra FROM x WHERE user_id = $1",
		KeysColumns: map[string]string{"$1": "id"},
	}}
	runner := &fakeRunner{keysRows: [][]any{{"t1", "oops"}}}
	publisher := newFakePublisher()
	testEngine := startEngine(t, runner, publisher, query)
	markLive(testEngine, "db:orders:t1")

	testEngine.HandleEvent(Event{
		Table:   "public.users",
		NewData: map[string]any{"id": "u1"},
	})
	time.Sleep(50 * time.Millisecond)
	publisher.mutex.Lock()
	defer publisher.mutex.Unlock()
	if len(publisher.published) != 0 {
		t.Fatalf("a keysSql row with the wrong width must not key a doc: %v", publisher.published)
	}
}

func TestBareWatchDirtiesAParamLessQuery(t *testing.T) {
	t.Parallel()
	query := spec.Query{
		Name:    "stats",
		SQL:     "SELECT json_build_object('total', count(*)) FROM orders",
		Watches: []spec.Watch{{Table: "orders"}},
	}
	runner := &fakeRunner{}
	publisher := newFakePublisher()
	testEngine := startEngine(t, runner, publisher, query)
	markLive(testEngine, "db:stats")

	testEngine.HandleEvent(Event{
		Table:   "public.orders",
		NewData: map[string]any{"anything": "x"},
	})
	publisher.waitFor(t, "db:stats")
}
