package agent

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "foony-sync.json")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadConfigAcceptsAValidFile(t *testing.T) {
	t.Parallel()
	path := writeConfig(t, `{
		"queries": [{
			"name": "orders",
			"sql": "SELECT coalesce(json_agg(o.*), '[]') FROM orders o WHERE o.tenant_id = $1",
			"params": [{"name": "tenantId", "type": "text"}],
			"watches": [{"table": "orders", "columns": {"tenantId": "tenant_id"}}]
		}],
		"statementTimeoutMs": 2000
	}`)
	config, err := LoadConfig(path)
	if err != nil {
		t.Fatalf("LoadConfig() = %v, want nil", err)
	}
	resolved := config.resolved()
	if resolved.statementTimeout != 2*time.Second {
		t.Fatalf("statementTimeout = %v, want 2s", resolved.statementTimeout)
	}
	if resolved.walRetentionCapBytes != 4<<30 {
		t.Fatalf("walRetentionCapBytes default = %v, want 4 GiB", resolved.walRetentionCapBytes)
	}
	summaries := querySummaries(config.Queries)
	if len(summaries) != 1 || summaries[0].Name != "orders" || summaries[0].Tables[0] != "orders" {
		t.Fatalf("summaries = %+v", summaries)
	}
}

func TestLoadConfigRejections(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name    string
		content string
		want    string
	}{
		{"no queries", `{"queries": []}`, "declares no queries"},
		{"unknown field", `{"queries": [{"name": "a", "sql": "SELECT 1"}], "settings": {}}`, "unknown field"},
		{"invalid query", `{"queries": [{"name": "BAD NAME", "sql": "SELECT 1"}]}`, "name must be"},
		{"duplicate query", `{"queries": [{"name": "a", "sql": "SELECT 1"}, {"name": "a", "sql": "SELECT 2"}]}`, "declared twice"},
		{"not json", `queries:`, "parse config"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := writeConfig(t, tc.content)
			_, err := LoadConfig(path)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("LoadConfig() = %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestLoadConfigMissingFile(t *testing.T) {
	t.Parallel()
	if _, err := LoadConfig(filepath.Join(t.TempDir(), "missing.json")); err == nil {
		t.Fatal("LoadConfig should fail on a missing file")
	}
}

func TestWalCapDisable(t *testing.T) {
	t.Parallel()
	config := FileConfig{WalRetentionCapBytes: -1}
	if config.resolved().walRetentionCapBytes != 0 {
		t.Fatal("walRetentionCapBytes = -1 should disable the safety valve")
	}
}
