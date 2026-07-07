package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

// executorPoolSize caps the agent's footprint on the customer database for
// doc and keysSql queries (the replication connection is separate). Matches
// the engine's worker count.
const executorPoolSize = 2

// Executor runs customer SQL on the customer's own database. Every session is
// pinned read-only with a statement timeout at connect time, so no customer
// query, hostile or accidental, can write or run forever. This session
// pinning, not SQL inspection, is the real safety layer.
type Executor struct {
	pool *pgxpool.Pool
	// maxDocBytes rejects doc results larger than the platform doc cap.
	maxDocBytes int
}

// NewExecutor connects the customer-DB query pool. dsn is the plain (non
// replication) connection string.
func NewExecutor(ctx context.Context, dsn string, statementTimeout time.Duration, maxDocBytes int) (*Executor, error) {
	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("dbsync: parse database url: %w", err)
	}
	config.MaxConns = executorPoolSize
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		if _, err := conn.Exec(ctx, "SET default_transaction_read_only = on"); err != nil {
			return fmt.Errorf("dbsync: pin session read-only: %w", err)
		}
		if _, err := conn.Exec(ctx, fmt.Sprintf("SET statement_timeout = %d", statementTimeout.Milliseconds())); err != nil {
			return fmt.Errorf("dbsync: set statement timeout: %w", err)
		}
		return nil
	}
	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("dbsync: connect query pool: %w", err)
	}
	return &Executor{pool: pool, maxDocBytes: maxDocBytes}, nil
}

// Close releases the query pool.
func (executor *Executor) Close() {
	executor.pool.Close()
}

// Ping verifies the query pool can reach the database.
func (executor *Executor) Ping(ctx context.Context) error {
	return executor.pool.Ping(ctx)
}

// Pool exposes the underlying pool for agent housekeeping queries (slot lag,
// slot drop). Doc and keys queries go through RunDoc/RunKeys.
func (executor *Executor) Pool() *pgxpool.Pool {
	return executor.pool
}

// RunDoc executes a doc query: exactly one column, at most one row. Zero rows
// return JSON null (the doc does not exist). The result must marshal to JSON
// within the doc size cap.
func (executor *Executor) RunDoc(ctx context.Context, sql string, args []any) (json.RawMessage, error) {
	rows, err := executor.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("dbsync: doc query: %w", err)
	}
	defer rows.Close()
	if len(rows.FieldDescriptions()) != 1 {
		return nil, errors.New("dbsync: a doc query must return exactly one column (wrap it in to_jsonb, json_agg, or json_build_object)")
	}
	if !rows.Next() {
		if err := rows.Err(); err != nil {
			return nil, fmt.Errorf("dbsync: doc query: %w", err)
		}
		return json.RawMessage("null"), nil
	}
	var value any
	if err := rows.Scan(&value); err != nil {
		return nil, fmt.Errorf("dbsync: doc scan: %w", err)
	}
	if rows.Next() {
		return nil, errors.New("dbsync: a doc query must return at most one row (aggregate multi-row results with json_agg)")
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dbsync: doc query: %w", err)
	}
	doc, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("dbsync: doc must be JSON-encodable: %w", err)
	}
	if len(doc) > executor.maxDocBytes {
		return nil, fmt.Errorf("dbsync: doc is %d bytes, over the %d byte cap", len(doc), executor.maxDocBytes)
	}
	return doc, nil
}

// ErrTooManyKeys reports a keysSql result over the watch's maxKeys cap. The
// engine matches it with errors.Is: unlike a transient query failure this is
// permanent for the change that triggered it, so instead of retrying, the
// engine recomputes every live doc of the query (a superset of the affected
// docs, bounded by real subscribers).
var ErrTooManyKeys = errors.New("dbsync: keys query returned more rows than maxKeys")

// RunKeys executes a keysSql reverse index and returns up to maxKeys rows,
// each row's values in column order (column i feeds query param $i+1).
// Hitting the cap is ErrTooManyKeys by design: a truncated key set would
// silently leave some docs stale, so the caller must fall back to something
// that covers every affected doc.
func (executor *Executor) RunKeys(ctx context.Context, sql string, args []any, maxKeys int) ([][]any, error) {
	rows, err := executor.pool.Query(ctx, sql, args...)
	if err != nil {
		return nil, fmt.Errorf("dbsync: keys query: %w", err)
	}
	defer rows.Close()
	results := [][]any{}
	for rows.Next() {
		if len(results) >= maxKeys {
			return nil, fmt.Errorf("%w (%d)", ErrTooManyKeys, maxKeys)
		}
		values, err := rows.Values()
		if err != nil {
			return nil, fmt.Errorf("dbsync: keys scan: %w", err)
		}
		results = append(results, values)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("dbsync: keys query: %w", err)
	}
	return results, nil
}
