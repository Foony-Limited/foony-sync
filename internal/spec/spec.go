// Package spec defines the Database Sync live-query shape shared by the
// control plane (which validates and stores definitions) and the foony-sync
// agent (which executes them against the customer's database). See
// design/architecture/database-sync.md.
package spec

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"
)

// Limits on a single query definition. These bound doc-channel cardinality and
// per-event fan-out; they are definition-shape caps, not billing plan limits.
const (
	MaxQueryNameLength = 48
	MaxParamsPerQuery  = 4
	MaxWatchesPerQuery = 16
	MaxSQLLength       = 8192
	// MaxKeysCap bounds a reverse-index watch's per-event fan-out: one row
	// change may dirty at most this many docs.
	MaxKeysCap = 1000
	// DefaultMaxKeys applies when a keysSql watch does not set maxKeys.
	DefaultMaxKeys = 100
	// MaxParamValueLength caps one param value inside a channel name.
	MaxParamValueLength = 64
)

// Watch maps changes on one customer table to affected doc keys. Exactly one
// of Columns or KeysSQL must be set: Columns reads param values straight off
// the changed row; KeysSQL is the reverse index for join queries, run against
// the customer database with values from the changed row to return one row
// per affected doc.
type Watch struct {
	Table string `json:"table"`
	// Columns names the changed row's columns that carry the query's param
	// values, in order: element i feeds $i+1 and channel segment i+1. Must
	// have exactly one column per query param.
	Columns []string `json:"columns,omitempty"`
	// KeysSQL is a single-statement SELECT returning one column per query
	// param, in param order, one row per affected doc.
	KeysSQL string `json:"keysSql,omitempty"`
	// KeysColumns maps KeysSQL placeholders ($1..$n) to columns of the changed
	// row that feed them.
	KeysColumns map[string]string `json:"keysColumns,omitempty"`
	// MaxKeys caps how many docs one change event may dirty via KeysSQL.
	MaxKeys int `json:"maxKeys,omitempty"`
}

// Query is one live-query definition: the doc SQL plus the watches that keep
// its docs fresh. The SQL's $1..$n placeholders are the query's params; their
// values become the channel segments, in placeholder order, so a doc lives on
// db:<Name>:<$1 value>:...:<$n value>. Values bind as text and Postgres casts
// them from context (write an explicit cast like $1::bigint when it cannot).
// Definitions live in the agent's local config file and never leave it; the
// server only ever sees the Summary.
type Query struct {
	Name    string  `json:"name"`
	SQL     string  `json:"sql"`
	Watches []Watch `json:"watches"`
}

// QuerySummary is the read-only shape the agent reports on heartbeats so the
// dashboard can show what a source serves without holding the definitions.
type QuerySummary struct {
	Name   string   `json:"name"`
	Tables []string `json:"tables"`
}

// Summary reduces a definition to its reportable shape.
func (query Query) Summary() QuerySummary {
	tables := make([]string, 0, len(query.Watches))
	seen := map[string]bool{}
	for _, watch := range query.Watches {
		if !seen[watch.Table] {
			seen[watch.Table] = true
			tables = append(tables, watch.Table)
		}
	}
	return QuerySummary{Name: query.Name, Tables: tables}
}

var (
	queryNamePattern  = regexp.MustCompile(`^[a-z0-9][a-z0-9_-]{0,47}$`)
	identifierPattern = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	// tablePattern is a bare table name: watches cover the public schema only
	// in v1, matching the replication reader's publication management.
	tablePattern        = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)
	channelValuePattern = regexp.MustCompile(`^[A-Za-z0-9_-]{1,64}$`)
	placeholderPattern  = regexp.MustCompile(`^\$[1-9][0-9]?$`)
	paramRefPattern     = regexp.MustCompile(`\$([0-9]+)`)
)

// ParamCount reports how many params a query's SQL uses: the highest $n
// placeholder it references. A $n inside a string literal counts too, a
// deliberate simplification (it can only over-count, which fails watch
// validation loudly at load rather than mis-keying docs at runtime).
func ParamCount(sql string) int {
	highest := 0
	for _, match := range paramRefPattern.FindAllStringSubmatch(sql, -1) {
		if n, err := strconv.Atoi(match[1]); err == nil && n > highest {
			highest = n
		}
	}
	return highest
}

// ValidChannelValue reports whether value may appear as a param segment of a
// db: doc channel name. int/uuid/bool values always pass shape-wise; text is
// the type this actually restricts.
func ValidChannelValue(value string) bool {
	return channelValuePattern.MatchString(value)
}

// Validate checks a query definition's shape. Execution-level guarantees (one
// row, one JSON column, cost) are enforced by the agent's read-only session
// and statement timeout, not here.
func Validate(query Query) error {
	if !queryNamePattern.MatchString(query.Name) {
		return fmt.Errorf("name must be 1-%d characters of a-z 0-9 _ - and start with a letter or digit", MaxQueryNameLength)
	}
	if err := validateSelect(query.SQL, "sql"); err != nil {
		return err
	}
	arity := ParamCount(query.SQL)
	if arity > MaxParamsPerQuery {
		return fmt.Errorf("a query may use at most %d params ($1..$%d)", MaxParamsPerQuery, MaxParamsPerQuery)
	}
	if len(query.Watches) > MaxWatchesPerQuery {
		return fmt.Errorf("a query may declare at most %d watches", MaxWatchesPerQuery)
	}
	for index, watch := range query.Watches {
		if err := validateWatch(watch, arity); err != nil {
			return fmt.Errorf("watch %d (%s): %w", index+1, watch.Table, err)
		}
	}
	return nil
}

func validateWatch(watch Watch, arity int) error {
	if !tablePattern.MatchString(watch.Table) {
		return errors.New("table must be a plain identifier in the public schema")
	}
	hasColumns := len(watch.Columns) > 0
	hasKeysSQL := strings.TrimSpace(watch.KeysSQL) != ""
	if hasColumns == hasKeysSQL && arity > 0 {
		return errors.New("declare exactly one of columns (direct mapping) or keysSql (reverse index)")
	}
	if hasColumns {
		if watch.MaxKeys != 0 || len(watch.KeysColumns) > 0 {
			return errors.New("maxKeys and keysColumns only apply to keysSql watches")
		}
		if len(watch.Columns) != arity {
			return fmt.Errorf("columns lists %d columns but the sql uses %d params (element i feeds $i+1)", len(watch.Columns), arity)
		}
		for _, column := range watch.Columns {
			if !identifierPattern.MatchString(column) {
				return fmt.Errorf("column %q must be a plain identifier", column)
			}
		}
		return nil
	}
	if !hasKeysSQL {
		// A watch on a param-less query needs no mapping: any change on the
		// table dirties the query's only doc.
		return nil
	}
	if err := validateSelect(watch.KeysSQL, "keysSql"); err != nil {
		return err
	}
	if len(watch.KeysColumns) == 0 {
		return errors.New("keysSql needs keysColumns mapping its $n placeholders to row columns")
	}
	for placeholder, column := range watch.KeysColumns {
		if !placeholderPattern.MatchString(placeholder) {
			return fmt.Errorf("keysColumns key %q must be a $n placeholder", placeholder)
		}
		if !identifierPattern.MatchString(column) {
			return fmt.Errorf("column %q must be a plain identifier", column)
		}
	}
	if watch.MaxKeys < 0 || watch.MaxKeys > MaxKeysCap {
		return fmt.Errorf("maxKeys must be between 1 and %d", MaxKeysCap)
	}
	return nil
}

// validateSelect is the shape gate for customer SQL: one statement, and it
// reads. Real safety is execution-side (the agent pins the session read-only
// with a statement timeout); this exists to fail obvious mistakes early with
// a clear message. Semicolons are rejected outright rather than parsed around
// string literals, a deliberate simplification.
func validateSelect(sql, field string) error {
	trimmed := strings.TrimSpace(sql)
	trimmed = strings.TrimSuffix(trimmed, ";")
	if trimmed == "" {
		return fmt.Errorf("%s is required", field)
	}
	if len(trimmed) > MaxSQLLength {
		return fmt.Errorf("%s is longer than %d characters", field, MaxSQLLength)
	}
	if strings.Contains(trimmed, ";") {
		return fmt.Errorf("%s must be a single statement", field)
	}
	lowered := strings.ToLower(trimmed)
	if !strings.HasPrefix(lowered, "select") && !strings.HasPrefix(lowered, "with") {
		return fmt.Errorf("%s must be a SELECT (or WITH ... SELECT)", field)
	}
	return nil
}
