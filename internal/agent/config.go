package agent

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"time"

	"github.com/Foony-Limited/foony-sync/internal/spec"
)

// heartbeatInterval is how often the agent reports health. Fixed: it tunes
// dashboard freshness on our side, so it is not a customer knob.
const heartbeatInterval = 30 * time.Second

// maxDocBytes rejects a computed doc before shipping it. Fixed at the
// platform's publish-time cap, which is what would reject it anyway; the
// local check just fails with a clearer error.
const maxDocBytes = 256 << 10

// FileConfig is the agent's local config file (foony-sync.json): the live
// queries plus the two protect-my-database knobs. Definitions live here,
// next to the code that owns the schema, and never leave the machine; the
// agent reports only name and table summaries to the dashboard.
type FileConfig struct {
	Queries []spec.Query `json:"queries"`
	// StatementTimeoutMs pins every query the agent runs (default 5000).
	StatementTimeoutMs int `json:"statementTimeoutMs,omitempty"`
	// WalRetentionCapBytes is the safety valve: past this much retained WAL
	// the agent drops its replication slot rather than risk filling the
	// database's disk (default 4 GiB; 0 keeps the default, -1 disables).
	WalRetentionCapBytes int64 `json:"walRetentionCapBytes,omitempty"`
}

// settings are FileConfig's knobs with defaults applied.
type settings struct {
	statementTimeout     time.Duration
	walRetentionCapBytes int64
}

// LoadConfig reads and validates the agent config file. Every query is
// checked with the same rules the docs describe, so a typo fails fast at
// startup with a message naming the query.
func LoadConfig(path string) (FileConfig, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return FileConfig{}, fmt.Errorf("dbsync: read config %s: %w", path, err)
	}
	var config FileConfig
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return FileConfig{}, fmt.Errorf("dbsync: parse config %s: %w", path, err)
	}
	if len(config.Queries) == 0 {
		return FileConfig{}, fmt.Errorf("dbsync: config %s declares no queries", path)
	}
	names := map[string]bool{}
	for _, query := range config.Queries {
		if err := spec.Validate(query); err != nil {
			return FileConfig{}, fmt.Errorf("dbsync: query %q: %w", query.Name, err)
		}
		if names[query.Name] {
			return FileConfig{}, fmt.Errorf("dbsync: query %q is declared twice", query.Name)
		}
		names[query.Name] = true
	}
	return config, nil
}

// resolved applies defaults to the file's optional knobs.
func (config FileConfig) resolved() settings {
	resolved := settings{
		statementTimeout:     5 * time.Second,
		walRetentionCapBytes: 4 << 30,
	}
	if config.StatementTimeoutMs > 0 {
		resolved.statementTimeout = time.Duration(config.StatementTimeoutMs) * time.Millisecond
	}
	if config.WalRetentionCapBytes > 0 {
		resolved.walRetentionCapBytes = config.WalRetentionCapBytes
	}
	if config.WalRetentionCapBytes < 0 {
		resolved.walRetentionCapBytes = 0
	}
	return resolved
}

// querySummaries is what heartbeats report about the config: names and
// watched tables only, never the SQL.
func querySummaries(queries []spec.Query) []spec.QuerySummary {
	summaries := make([]spec.QuerySummary, len(queries))
	for index, query := range queries {
		summaries[index] = query.Summary()
	}
	return summaries
}
