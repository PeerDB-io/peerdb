# Native JSON in ClickHouse: `PEERDB_CLICKHOUSE_ENABLE_JSON`

By default, PeerDB stores source JSON in ClickHouse as `String`. With `PEERDB_CLICKHOUSE_ENABLE_JSON=true` it
creates native ClickHouse `JSON` columns instead, so fields can be queried as subcolumns without
`JSONExtract*`.

## Using it

- Set it globally or per mirror (see [`flow/internal/dynamicconf.go`](../flow/internal/dynamicconf.go)).
- It requires ClickHouse 25.3 or newer. On an older server, CDC mirror creation and resync fail instead of
  falling back to `String`.

| Source column | Off (default) | On | On + `PEERDB_NULLABLE`, nullable column | Non-object values |
|---|---|---|---|---|
| Postgres `json`/`jsonb`, MySQL/MariaDB `JSON` | `String` | `JSON` | `Nullable(JSON)` | wrapped |
| Postgres `json[]`/`jsonb[]` | `String` | `Array(JSON)` | `Array(Nullable(JSON))` | elements wrapped |
| MongoDB full document (`doc`)\* | `String` | `JSON` | `JSON` | none, always an object |
| BigQuery `STRUCT` | `String` | `JSON` | `Nullable(JSON)` | none, always an object |
| CockroachDB `json`/`jsonb`, BigQuery `JSON` | `String` | `JSON` | `Nullable(JSON)` | not wrapped, the insert fails |
| CockroachDB `json[]`, BigQuery `ARRAY<JSON>` | `String` | `String` | `String` | |

\* With structured ingestion (MongoDB only), each document field goes to a column whose type is provided in the corresponding table mapping, so the
setting doesn't change those columns. It only changes `_peerdb_malformed_data`, the column that records values
that don't fit their column: `Nullable(String)` when off, `Nullable(JSON)` when on. Nothing is wrapped. A scalar
sent to a declared `JSON` column is recorded in `_peerdb_malformed_data`.

ClickHouse `JSON` only accepts objects at the top level, so the sources that wrap (Postgres, MySQL, MariaDB)
turn any other value into `{"_value": ...}`. The same rule applies to each element of a `json[]`:

| Source value | `JSON`, `Array(JSON)` element | `Nullable(JSON)`, `Array(Nullable(JSON))` element |
|---|---|---|
| `{"theme":"dark"}` | `{"theme":"dark"}` | `{"theme":"dark"}` |
| `"dark"` / `42` / `["a","b"]` | `{"_value":"dark"}` / `{"_value":42}` / `{"_value":["a","b"]}` | same |
| JSON `null` | `{}` | `{}` |
| SQL `NULL` | `{}` | `NULL` |

A SQL `NULL` array lands as `[]`. Snapshot and CDC produce the same values.

So for a `jsonb` column `preferences` and a `jsonb[]` column `tags`, object fields are subcolumns and wrapped
values sit under `_value`:

```sql
SELECT preferences.theme, preferences._value, tags[1].name FROM users;
```

A per-column destination type overrides the mapping. A `JSON`, `Nullable(JSON)`, `Array(JSON)` or
`Array(Nullable(JSON))` override gets the same wrapping even with the setting off. This only applies on Postgres
and MySQL sources (array overrides on Postgres only), on a ClickHouse 25.3+ server. A `String` override keeps the
raw text.

## How it works

**Wrapping happens in the source reader.** The Postgres and MySQL readers, on both snapshot and CDC, wrap
values before they leave the source. Gates live in [`flow/model/qvalue/json.go`](../flow/model/qvalue/json.go):
- `ShouldWrapNonObjectJSON` for scalar columns.
- `ShouldWrapJSONArrayElements` for `json[]` elements.

A value is left alone if its first non-blank byte is `{`. A top-level JSON string is always quoted, so it can't
be mistaken for an object.

**Column types are chosen by [`ToDWHColumnType`](../flow/model/qvalue/kind.go)**, through two checks:
- `ShouldUseNativeJSONType` checks the setting and the live server version.
- `ShouldUseArrayJSONType` additionally requires a Postgres source and a new enough mirror version. Only the
  Postgres reader wraps array elements, so CockroachDB and BigQuery `json[]` stay `String`.

**On the ClickHouse side** ([`normalize_query.go`](../flow/connectors/clickhouse/normalize_query.go), [`table_function.go`](../flow/connectors/clickhouse/table_function.go)):
- Normalize and the snapshot insert cast the text to the column type.
- SQL `NULL` into a non-nullable `JSON` column becomes `{}`.
- For `Array(JSON)`, null elements are mapped to `{}`, because `JSONExtract('[null,null]', 'Array(JSON)')`
  returns `[]`.
- PeerDB sends `type_json_skip_duplicated_paths=1`. On ClickHouse 25.8+ and newer mirrors it also sends
  `json_type_escape_dots_in_keys=1`.

**Schema changes:** a Postgres JSON column added during CDC keeps its default when wrapping applies. The default
is wrapped, and for `json[]` it is written as `DEFAULT JSONExtract('[...]', 'Array(JSON)')`, because ClickHouse
rejects a string literal there.

**MariaDB** stores `JSON` as `LONGTEXT` with a `json_valid("col")` check. Schema discovery detects that check
(server 10.4.3+) and types the column as JSON.

**Per-mirror behaviour.** At creation, a mirror stores:
- the resolved setting, in its env;
- the flag `clickhouse_json_supported`, when the server is 25.3+;
- its internal version.

Resync keeps all three. Changes to the JSON mapping are gated by internal versions
([`flow/shared/constants.go`](../flow/shared/constants.go)), so a mirror keeps the behaviour it was created with; recreate it to pick up newer
mappings.

## Tests as reference

- [`flow/e2e/clickhouse_json_suite.go`](../flow/e2e/clickhouse_json_suite.go): Postgres, MySQL and MariaDB
  (`json[]` and ADD COLUMN defaults on Postgres only).
- [`flow/e2e/mongo_clickhouse/mongo_test.go`](../flow/e2e/mongo_clickhouse/mongo_test.go): MongoDB.

## Limitations

- Changing the setting on a running mirror (`updated_env`) is not blocked and leaves existing columns
  inconsistent with the new casts.
- Mirrors created before the wrapping, or before ClickHouse was upgraded to 25.3, don't wrap. Recreate them.
- Lowercase `json` overrides skip wrapping; parameterised `JSON(...)` overrides are rejected.
- SQL `NULL` elements in a `json[]` ADD COLUMN default land as `{}`, even in `Array(Nullable(JSON))`.
- MariaDB JSON columns added by online schema tools (gh-ost, pt-osc) land as `String`.
