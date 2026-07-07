package agent

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/Foony-Limited/foony-sync/internal/spec"
)

// checkReplicaIdentities warns, once per table at startup, when logical
// replication cannot carry a column a watch reads off old rows. With the
// default replica identity the old row image only holds the primary key: a
// DELETE (or an UPDATE that changes the key column) on a doc keyed by any
// other column never maps back to that doc, so it keeps showing departed rows
// until something else changes. keysSql input columns matter too, because
// DELETEs feed the lookup from the old row. The check is best-effort: a
// failed catalog query is logged at debug and skipped, never fatal.
func checkReplicaIdentities(ctx context.Context, pool *pgxpool.Pool, queries []spec.Query, logger *slog.Logger) {
	for table, columns := range watchedColumnsByTable(queries) {
		identityColumns, full, err := replicaIdentityColumns(ctx, pool, table)
		if err != nil {
			logger.Debug("replica identity check skipped", "table", table, "error", err.Error())
			continue
		}
		if full {
			continue
		}
		missing := make([]string, 0, len(columns))
		for _, column := range columns {
			if !identityColumns[column] {
				missing = append(missing, column)
			}
		}
		if len(missing) == 0 {
			continue
		}
		logger.Warn(fmt.Sprintf(
			"table %s: watched column(s) %s are not in the replica identity, so DELETEs and key-changing UPDATEs cannot refresh their docs. Fix with: ALTER TABLE %s REPLICA IDENTITY FULL",
			table, strings.Join(missing, ", "), table))
	}
}

// watchedColumnsByTable collects, per watched table, every column a watch may
// need to read off a changed row: direct columns mappings and keysSql input
// columns. Column lists come out sorted so warnings are stable.
func watchedColumnsByTable(queries []spec.Query) map[string][]string {
	byTable := map[string]map[string]struct{}{}
	for _, query := range queries {
		for _, watch := range query.Watches {
			columns := byTable[watch.Table]
			if columns == nil {
				columns = map[string]struct{}{}
				byTable[watch.Table] = columns
			}
			for _, column := range watch.Columns {
				columns[column] = struct{}{}
			}
			for _, column := range watch.KeysColumns {
				columns[column] = struct{}{}
			}
		}
	}
	result := make(map[string][]string, len(byTable))
	for table, columns := range byTable {
		list := make([]string, 0, len(columns))
		for column := range columns {
			list = append(list, column)
		}
		sort.Strings(list)
		result[table] = list
	}
	return result
}

// replicaIdentityColumns returns the columns Postgres includes in old row
// images for a public-schema table, and whether the identity is FULL (every
// column). Identity "nothing" yields an empty set: old rows carry no columns
// at all.
func replicaIdentityColumns(ctx context.Context, pool *pgxpool.Pool, table string) (map[string]bool, bool, error) {
	var identity string
	err := pool.QueryRow(ctx,
		`SELECT c.relreplident::text FROM pg_class c
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 WHERE n.nspname = 'public' AND c.relname = $1`, table).Scan(&identity)
	if err != nil {
		return nil, false, fmt.Errorf("dbsync: replica identity of %s: %w", table, err)
	}
	switch identity {
	case "f":
		return nil, true, nil
	case "n":
		return map[string]bool{}, false, nil
	}
	// "d" uses the primary key; "i" uses the index marked REPLICA IDENTITY.
	indexFilter := "i.indisprimary"
	if identity == "i" {
		indexFilter = "i.indisreplident"
	}
	rows, err := pool.Query(ctx, fmt.Sprintf(
		`SELECT a.attname FROM pg_index i
		 JOIN pg_class c ON c.oid = i.indrelid
		 JOIN pg_namespace n ON n.oid = c.relnamespace
		 JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
		 WHERE n.nspname = 'public' AND c.relname = $1 AND %s`, indexFilter), table)
	if err != nil {
		return nil, false, fmt.Errorf("dbsync: identity columns of %s: %w", table, err)
	}
	defer rows.Close()
	columns := map[string]bool{}
	for rows.Next() {
		var column string
		if err := rows.Scan(&column); err != nil {
			return nil, false, fmt.Errorf("dbsync: identity columns of %s: %w", table, err)
		}
		columns[column] = true
	}
	if err := rows.Err(); err != nil {
		return nil, false, fmt.Errorf("dbsync: identity columns of %s: %w", table, err)
	}
	return columns, false, nil
}
