// Package spec defines the Database Sync live-query shape shared by the
// control plane (which validates and stores definitions) and the foony-sync
// agent (which executes them against the customer's database). See
// design/architecture/database-sync.md.
package spec

import (
	"errors"
	"fmt"
	"regexp"
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

// Param is one declared query parameter. Values become channel-name segments,
// so types are restricted to values that fit the channel charset.
type Param struct {
	Name string `json:"name"`
	// Type is text, int, uuid, or bool.
	Type string `json:"type"`
}

// Watch maps changes on one customer table to affected doc keys. Exactly one
// of Columns or KeysSQL must be set: Columns reads param values straight off
// the changed row; KeysSQL is the reverse index for join queries, run against
// the customer database with values from the changed row to return one row
// per affected params-tuple.
type Watch struct {
	Table string `json:"table"`
	// Columns maps param name to the column of Table that carries its value.
	Columns map[string]string `json:"columns,omitempty"`
	// KeysSQL is a single-statement SELECT returning one column per query
	// param, aliased to the param names, one row per affected doc.
	KeysSQL string `json:"keysSql,omitempty"`
	// KeysColumns maps KeysSQL placeholders ($1..$n) to columns of the changed
	// row that feed them.
	KeysColumns map[string]string `json:"keysColumns,omitempty"`
	// MaxKeys caps how many docs one change event may dirty via KeysSQL.
	MaxKeys int `json:"maxKeys,omitempty"`
}

// Query is one live-query definition: the doc SQL plus the watches that keep
// its docs fresh. Docs live on the channel db:<Name>:<param values>.
// Definitions live in the agent's local config file and never leave it; the
// server only ever sees the Summary.
type Query struct {
	Name    string  `json:"name"`
	SQL     string  `json:"sql"`
	Params  []Param `json:"params"`
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
)

// ValidParamType reports whether t is a supported param type.
func ValidParamType(t string) bool {
	return t == "text" || t == "int" || t == "uuid" || t == "bool"
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
	if len(query.Params) > MaxParamsPerQuery {
		return fmt.Errorf("a query may declare at most %d params", MaxParamsPerQuery)
	}
	paramNames := make(map[string]bool, len(query.Params))
	for _, param := range query.Params {
		if !identifierPattern.MatchString(param.Name) {
			return fmt.Errorf("param name %q must be a plain identifier", param.Name)
		}
		if !ValidParamType(param.Type) {
			return fmt.Errorf("param %q type must be text, int, uuid, or bool", param.Name)
		}
		if paramNames[param.Name] {
			return fmt.Errorf("param %q is declared twice", param.Name)
		}
		paramNames[param.Name] = true
	}
	if len(query.Watches) > MaxWatchesPerQuery {
		return fmt.Errorf("a query may declare at most %d watches", MaxWatchesPerQuery)
	}
	for index, watch := range query.Watches {
		if err := validateWatch(watch, paramNames); err != nil {
			return fmt.Errorf("watch %d (%s): %w", index+1, watch.Table, err)
		}
	}
	return nil
}

func validateWatch(watch Watch, paramNames map[string]bool) error {
	if !tablePattern.MatchString(watch.Table) {
		return errors.New("table must be a plain identifier in the public schema")
	}
	hasColumns := len(watch.Columns) > 0
	hasKeysSQL := strings.TrimSpace(watch.KeysSQL) != ""
	if hasColumns == hasKeysSQL {
		return errors.New("declare exactly one of columns (direct mapping) or keysSql (reverse index)")
	}
	if hasColumns {
		if watch.MaxKeys != 0 || len(watch.KeysColumns) > 0 {
			return errors.New("maxKeys and keysColumns only apply to keysSql watches")
		}
		for param, column := range watch.Columns {
			if !paramNames[param] {
				return fmt.Errorf("columns maps unknown param %q", param)
			}
			if !identifierPattern.MatchString(column) {
				return fmt.Errorf("column %q must be a plain identifier", column)
			}
		}
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
