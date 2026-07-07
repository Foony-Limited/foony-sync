package spec

import (
	"strings"
	"testing"
)

func validQuery() Query {
	return Query{
		Name: "orders",
		SQL:  "SELECT coalesce(json_agg(o.*), '[]') FROM orders o WHERE o.tenant_id = $1",
		Params: []Param{
			{Name: "tenantId", Type: "text"},
		},
		Watches: []Watch{
			{Table: "orders", Columns: map[string]string{"tenantId": "tenant_id"}},
		},
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
		KeysSQL:     `SELECT tenant_id::text AS "tenantId" FROM orders WHERE assignee = $1`,
		KeysColumns: map[string]string{"$1": "id"},
		MaxKeys:     500,
	})
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
			q.Params = []Param{{Name: "a", Type: "text"}, {Name: "b", Type: "text"}, {Name: "c", Type: "text"}, {Name: "d", Type: "text"}, {Name: "e", Type: "text"}}
		}, "at most 4 params"},
		{"bad param type", func(q *Query) { q.Params[0].Type = "float" }, "type must be"},
		{"duplicate param", func(q *Query) {
			q.Params = []Param{{Name: "a", Type: "text"}, {Name: "a", Type: "int"}}
		}, "declared twice"},
		{"schema-qualified table", func(q *Query) { q.Watches[0].Table = "billing.orders" }, "plain identifier"},
		{"watch with both flavors", func(q *Query) {
			q.Watches[0].KeysSQL = "SELECT 1"
		}, "exactly one of"},
		{"watch with neither flavor", func(q *Query) {
			q.Watches[0].Columns = nil
		}, "exactly one of"},
		{"columns maps unknown param", func(q *Query) {
			q.Watches[0].Columns = map[string]string{"nope": "tenant_id"}
		}, "unknown param"},
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
