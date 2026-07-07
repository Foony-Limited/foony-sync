# foony-sync

The Foony [Database Sync](https://foony.io/docs/database-sync) agent. It runs next to
your Postgres, holds your database credentials and live-query definitions locally
(neither ever reaches Foony), watches row changes over logical replication, re-runs the
affected queries, and publishes only the results as live documents on `db:` channels.

Clients subscribe to those channels with any Foony Realtime SDK and get the current
result immediately, then a fresh copy every time the underlying rows change.

## Run it

Get an agent key from your app's Database Sync tab in the dashboard, write your queries
in a `foony-sync.json`, then:

```bash
docker run -d --name foony-sync \
  -e FOONY_SYNC_KEY="myapp.kid_abc:sk_..." \
  -e DATABASE_URL="postgres://foony_sync:...@localhost:5432/mydb" \
  -v $(pwd)/foony-sync.json:/foony-sync.json \
  -e FOONY_SYNC_CONFIG=/foony-sync.json \
  ghcr.io/foony-limited/foony-sync
```

The agent needs a role with `REPLICATION` and SELECT on the tables you watch, and
`wal_level = logical` on the server:

```sql
CREATE ROLE foony_sync REPLICATION LOGIN PASSWORD '...';
GRANT SELECT ON orders TO foony_sync;
```

See the [Database Sync docs](https://foony.io/docs/database-sync) for the full
walkthrough: query definitions, watch rules, doc channels, and client subscriptions.

## Configuration

Environment:

- `FOONY_SYNC_KEY` (required): the source credential from the dashboard
  (`appSlug.keyId:secret`).
- `DATABASE_URL` (required): the Postgres DSN. The agent adds
  `replication=database` itself for the WAL connection.
- `FOONY_SYNC_CONFIG`: path to the definitions file. Defaults to
  `./foony-sync.json`.
- `FOONY_URL`: data-plane override. Defaults to `https://realtime.foony.io`.

The definitions file holds the live queries plus two protect-my-database knobs:

```json
{
  "queries": [{
    "name": "orders",
    "sql": "SELECT coalesce(json_agg(o.* ORDER BY o.created_at DESC), '[]') FROM orders o WHERE o.tenant_id = $1 AND o.status = 'open'",
    "params": [{ "name": "tenantId", "type": "text" }],
    "watches": [
      { "table": "orders", "columns": { "tenantId": "tenant_id" } }
    ]
  }],
  "statementTimeoutMs": 5000,
  "walRetentionCapBytes": 4294967296
}
```

- `statementTimeoutMs` pins every query the agent runs (default 5000).
- `walRetentionCapBytes` is the safety valve: past this much retained WAL the agent
  drops its replication slot rather than risk filling the database's disk (default
  4 GiB, -1 disables).

Definitions never leave the machine. The dashboard only ever sees name-and-table
summaries from heartbeats.

## What it talks to

Outbound only, to `FOONY_URL`:

- Doc publishes and the `dbsync:warm` subscription, through the
  [realtime-go](https://github.com/Foony-Limited/realtime-go) SDK.
- Heartbeats and live-doc polls over REST.

The only inbound traffic it acts on is a warm request naming a `db:` channel to
compute. Your database is reached only from this process.

## Build from source

```bash
go build .          # or: go install github.com/Foony-Limited/foony-sync@latest
go test ./...
```

The published image is built by [release-image.yml](.github/workflows/release-image.yml)
on every version tag and pushed to `ghcr.io/foony-limited/foony-sync`.

## License

[Apache-2.0](./LICENSE) © Foony Limited
