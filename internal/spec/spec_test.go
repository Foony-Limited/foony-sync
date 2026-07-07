package spec

import (
	"strings"
	"testing"
)

func validQuery() Query {
	return Query{
		Name: "orders",
		SQL:  "SELECT coalesce(json_agg(o.*), '[]') FROM orders o WHERE o.tenant_id = $1",
		Watches: []Watch{
			{Table: "orders", Columns: []string{"tenant_id"}},
		},
	}
}

func TestParamCount(t *testing.T) {
	t.Parallel()
	cases := []struct {
		sql  string
		want int
	}{
		{"SELECT 1", 0},
		{"SELECT * FROM t WHERE a = $1", 1},
		{"SELECT * FROM t WHERE a = $2 AND b = $1", 2},
		{"SELECT * FROM t WHERE a = $1 AND a2 = $1 AND b = $10", 10},
	}
	for _, tc := range cases {
		if got := ParamCount(tc.sql); got != tc.want {
			t.Fatalf("ParamCount(%q) = %d, want %d", tc.sql, got, tc.want)
		}
	}
}

func TestValidateAcceptsAWellFormedQuery(t *testing.T) {
	t.Parallel()
	if err := Validate(validQuery()); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestValidateAcceptsAKeysSQLWatch(t *testing.T) {
	t.Parallel()
	query := validQuery()
	query.Watches = append(query.Watches, Watch{
		Table:       "users",
		KeysSQL:     "SELECT tenant_id::text FROM orders WHERE assignee = $1",
		KeysColumns: map[string]string{"$1": "id"},
		MaxKeys:     500,
	})
	if err := Validate(query); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestValidateAcceptsAParamLessQueryWithABareWatch(t *testing.T) {
	t.Parallel()
	query := Query{
		Name:    "stats",
		SQL:     "SELECT json_build_object('total', count(*)) FROM orders",
		Watches: []Watch{{Table: "orders"}},
	}
	if err := Validate(query); err != nil {
		t.Fatalf("Validate() = %v, want nil", err)
	}
}

func TestValidateRejections(t *testing.T) {
	t.Parallel()
	testCases := []struct {
		name   string
		mutate func(*Query)
		want   string
	}{
		{"uppercase name", func(q *Query) { q.Name = "Orders" }, "name must be"},
		{"long name", func(q *Query) { q.Name = strings.Repeat("a", 49) }, "name must be"},
		{"empty sql", func(q *Query) { q.SQL = "  " }, "sql is required"},
		{"two statements", func(q *Query) { q.SQL = "SELECT 1; SELECT 2" }, "single statement"},
		{"not a select", func(q *Query) { q.SQL = "DELETE FROM orders" }, "must be a SELECT"},
		{"with is fine but update is not", func(q *Query) { q.SQL = "UPDATE orders SET x = 1" }, "must be a SELECT"},
		{"too many params", func(q *Query) {
			q.SQL = "SELECT 1 WHERE a = $1 AND b = $2 AND c = $3 AND d = $4 AND e = $5"
		}, "at most 4 params"},
		{"schema-qualified table", func(q *Query) { q.Watches[0].Table = "billing.orders" }, "plain identifier"},
		{"watch with both flavors", func(q *Query) {
			q.Watches[0].KeysSQL = "SELECT 1"
		}, "exactly one of"},
		{"watch with neither flavor", func(q *Query) {
			q.Watches[0].Columns = nil
		}, "exactly one of"},
		{"columns count under the param count", func(q *Query) {
			q.SQL = "SELECT 1 WHERE a = $1 AND b = $2"
		}, "columns lists 1"},
		{"columns count over the param count", func(q *Query) {
			q.Watches[0].Columns = []string{"tenant_id", "extra"}
		}, "columns lists 2"},
		{"bad column identifier", func(q *Query) {
			q.Watches[0].Columns = []string{"tenant id"}
		}, "plain identifier"},
		{"keysSql without keysColumns", func(q *Query) {
			q.Watches[0] = Watch{Table: "users", KeysSQL: "SELECT 1"}
		}, "needs keysColumns"},
		{"keysSql maxKeys over cap", func(q *Query) {
			q.Watches[0] = Watch{Table: "users", KeysSQL: "SELECT 1", KeysColumns: map[string]string{"$1": "id"}, MaxKeys: MaxKeysCap + 1}
		}, "maxKeys must be"},
		{"keysColumns bad placeholder", func(q *Query) {
			q.Watches[0] = Watch{Table: "users", KeysSQL: "SELECT 1", KeysColumns: map[string]string{"one": "id"}}
		}, "placeholder"},
	}
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			query := validQuery()
			tc.mutate(&query)
			err := Validate(query)
			if err == nil {
				t.Fatalf("Validate() = nil, want error containing %q", tc.want)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Validate() = %q, want it to contain %q", err.Error(), tc.want)
			}
		})
	}
}

func TestValidChannelValue(t *testing.T) {
	t.Parallel()
	valid := []string{"tenant42", "a", "TRUE", "550e8400-e29b-41d4-a716-446655440000", strings.Repeat("x", 64)}
	for _, value := range valid {
		if !ValidChannelValue(value) {
			t.Fatalf("ValidChannelValue(%q) = false, want true", value)
		}
	}
	invalid := []string{"", "has space", "semi;colon", "col:on", "dot.ted", strings.Repeat("x", 65), "emoji🎈"}
	for _, value := range invalid {
		if ValidChannelValue(value) {
			t.Fatalf("ValidChannelValue(%q) = true, want false", value)
		}
	}
}
