# Switchboard

A PostgreSQL wire protocol proxy that lets standard PostgreSQL clients (psql, pgcli, any driver) query upstream PostgreSQL, MySQL, and MongoDB databases. SQL queries are passed through verbatim to PostgreSQL and MySQL upstreams — no SQL translation occurs. MongoDB accepts Extended JSON wire commands directly.

Built for operator debugging and diagnostics against production databases. The security model is designed to prevent accidental mistakes (fat-fingered DROPs, unintended writes), not to stop a motivated attacker — the trust boundary is infrastructure-level access control.

## Usage

Connect with any PostgreSQL client. The `database` parameter selects the peer:

```sh
psql "host=localhost port=5732 dbname=my_peer"

# catalog is a special peer
psql "host=localhost port=5732 dbname=catalog"
```

Write queries in the upstream's native SQL dialect:

```sql
SELECT * FROM users WHERE active = true LIMIT 10;
EXPLAIN SELECT * FROM orders;
-- MySQL
SHOW TABLES;
SELECT @@GLOBAL.gtid_executed;
```

**MongoDB** — write Extended JSON wire commands:

```json
{"listCollections": 1}
{"listDatabases": 1}
{"find": "users", "filter": {"active": true}}
{"find": "users", "filter": {"age": {"$gt": 21}}, "sort": {"name": 1}, "limit": 10}
{"aggregate": "orders", "pipeline": [{"$match": {"status": "A"}}, {"$group": {"_id": "$item", "total": {"$sum": "$amount"}}}], "cursor": {}}
{"find": "users", "filter": {"_id": {"$oid": "507f1f77bcf86cd799439011"}}, "limit": 1, "singleBatch": true}
{"ping": 1}
{"explain": {"find": "users", "filter": {}}, "verbosity": "executionStats"}
```

MongoDB results are returned as Extended JSON. BSON types use Extended JSON syntax (e.g. `{"$oid": "..."}`, `{"$date": "..."}`, `{"$numberLong": "..."}`).

Type `help` for a list of allowed wire commands with doc links.

To preview help output without a running server: `go test ./mongodb/ -run TestPrintAllHelp -v`

## Architecture

```
psql / pgcli / any pgwire client
           │
           │  PostgreSQL wire protocol (simple query only)
           ▼
┌─────────────────────────────────────────────────────┐
│  Server (server.go)                                 │
│  TCP listener, per-connection goroutine dispatch,   │
│  cancel request routing via (pid,secret) → session  │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  Session (session.go)                               │
│  Owns one client conn + one upstream conn.          │
│  Handles:                                           │
│    1. SSL/startup negotiation (protocol.go)         │
│    2. SCRAM-SHA-256 auth (auth.go)                  │
│    3. Upstream creation via factory (upstream.go)   │
│    4. Message loop: Query → CheckQuery → Exec →     │
│       stream rows with guardrail checks → reply     │
│    5. Timeout enforcement (query, idle, write)      │
└──────────────────────┬──────────────────────────────┘
                       │
┌──────────────────────▼──────────────────────────────┐
│  Upstream interface (upstream.go)                   │
│                                                     │
│  ┌─────────────┐ ┌────────────┐ ┌────────────────┐  │
│  │  PostgreSQL │ │   MySQL    │ │    MongoDB     │  │
│  │  (pgx)      │ │ (go-mysql) │ │ (mongo-driver) │  │
│  └─────────────┘ └────────────┘ └────────────────┘  │
└─────────────────────────────────────────────────────┘
```

### Connection lifecycle

1. Client TCP connects. Server spawns a goroutine for the session.
2. SSL/GSS negotiation (respond `N` — TLS not supported, network assumed secure).
3. Parse `StartupMessage`. Extract peer name from `database` parameter, guardrail overrides from `options`.
4. SCRAM-SHA-256 authentication against a single proxy-level password (`PEERDB_SWITCHBOARD_PASSWORD` env var).
5. `NewUpstream()` factory loads the peer from the catalog (or connects directly to the PeerDB catalog if database is `"catalog"`), dispatches by `DBType` to create the upstream connection.
6. Forward upstream's `BackendKeyData` (pid, secret) to client. Register session in server's cancel index.
7. Send `ParameterStatus` messages from upstream's `ServerParameters()`, then `ReadyForQuery`.
8. Enter message loop: only `Query` and `Terminate` are handled; all other message types (extended protocol, `COPY`, etc.) are rejected.
9. On `Terminate` or idle timeout or disconnect, close upstream and client connections, deregister from cancel index.

### Query execution flow

1. Trim whitespace. Empty queries return `EmptyQueryResponse`.
2. Reset guardrail counters.
3. `upstream.CheckQuery(query)` — upstream-specific security validation (see per-upstream details below). Rejected queries get SQLSTATE `54000`.
4. `upstream.Exec(ctx, query)` returns a `ResultIterator`.
5. For each result set: send `RowDescription`, stream `DataRow` messages one at a time, checking row/byte guardrails before each send.
6. If a guardrail trips, cancel the context, close the iterator, send error with SQLSTATE `54000`.
7. Send `CommandComplete` (or `EmptyQueryResponse`) per result set.
8. After all result sets, `CloseAll()` the iterator, send `ReadyForQuery`.

### Cancel support

Client sends a `CancelRequest` with (pid, secret). Server looks up the session and calls `upstream.Cancel()`:

- **PostgreSQL**: native `CancelRequest` on the pgconn.
- **MySQL**: opens an ephemeral connection, runs `KILL QUERY <connection_id>`.
- **MongoDB**: queries `currentOp` for operations matching a per-session comment tag, kills via `killOp`.

### Guardrails (guardrails.go)

The guardrails are safety nets against operator mistakes, not a security boundary. Someone with access to the proxy already has infrastructure-level trust. The goal is to make it hard to accidentally run `DROP TABLE` or retrieve a billion rows, not to prevent someone who is deliberately trying to circumvent the checks.

Two kinds of enforcement, at different layers:

- **Query validation** — `upstream.CheckQuery()`, database-specific. Blocks dangerous queries before execution. Does not aim to be exploit-proof.
- **Result limits** — `Guardrails` struct, shared across all upstreams. Counts rows and bytes (including wire protocol overhead) per query, aborts when limits are exceeded.

Limits are configurable per session via startup `options` (`-c peerdb.max_rows=N`, `-c peerdb.max_bytes=N`, `-c peerdb.query_timeout=S`, `-c peerdb.write_timeout=S`, `-c peerdb.idle_timeout=S`) with env var defaults:

| Option | Env var | Default |
|--------|---------|---------|
| `peerdb.max_rows` | `PEERDB_SWITCHBOARD_MAX_ROWS` | 10,000 |
| `peerdb.max_bytes` | `PEERDB_SWITCHBOARD_MAX_BYTES` | 100 MB |
| `peerdb.query_timeout` | `PEERDB_SWITCHBOARD_QUERY_TIMEOUT_SECONDS` | 30s |
| `peerdb.write_timeout` | `PEERDB_SWITCHBOARD_WRITE_TIMEOUT_SECONDS` | 30s |
| `peerdb.idle_timeout` | — | 30 min |

## Upstream interface

Defined in `upstream.go`. Every upstream must implement:

```go
type Upstream interface {
    Exec(ctx context.Context, query string) (ResultIterator, error)
    TxStatus() byte                              // 'I' idle, 'T' in tx, 'E' error
    ServerParameters(ctx context.Context) map[string]string
    BackendKeyData() (pid uint32, secret uint32)
    Cancel(ctx context.Context) error
    Close() error
    CheckQuery(query string) error
}

type ResultIterator interface {
    NextResult() bool
    FieldDescriptions() []FieldDescription
    NextRow() bool
    RowValues() [][]byte   // text-encoded values
    CommandTag() string    // "SELECT 5", "UPDATE 3", etc.
    Err() error
    Close()
    CloseAll() error
}
```

### Existing upstreams

**PostgreSQL** (`upstream_postgres.go`)
- Forwards queries via `pgconn.Exec()` (multi-statement aware).
- Sets `default_transaction_read_only = on`, `statement_timeout`, and `idle_in_transaction_session_timeout` at session init.
- `CheckQuery`: splits with `pgsplit`, then first-keyword allowlist (SELECT, TABLE, VALUES, WITH, EXPLAIN, SHOW, transaction control, SET/RESET/DISCARD, cursor ops, PREPARE/EXECUTE/DEALLOCATE) + substring checks for `default_transaction_read_only` and `set_config` bypass attempts + blocks `RESET ALL` and `DISCARD ALL` (would undo session-level read-only). Anything not on the allowlist is rejected. Defense-in-depth with `default_transaction_read_only = on` as the upstream write gate.
- `FieldDescriptions` forwarded as-is from pgx (preserves OIDs, type info).
- `TxStatus` reflects real transaction state from the upstream connection.

**MySQL** (`upstream_mysql.go`)
- Forwards queries via go-mysql `Execute()`.
- Sets `SESSION TRANSACTION READ ONLY`, `max_execution_time`, lock wait timeouts at session init.
- `CheckQuery`: allowlist approach using TiDB SQL parser. Allows SELECT (with restrictions), SHOW, DESCRIBE, EXPLAIN, transaction control. Blocks `SELECT INTO`, locking reads, dangerous functions (`load_file`, `get_lock`, etc.). Recursively checks subqueries.
- Maps MySQL column types to PostgreSQL OIDs for display alignment (numeric types → right-aligned in psql).
- `TxStatus` always returns `'I'` (idle).
- Cancel via `KILL QUERY` over ephemeral connection.

**MongoDB** (`upstream_mongodb.go`, `mongosh/`)
- Input is Extended JSON wire commands (e.g. `{"find": "coll", "filter": {}}`). `mongosh.Compile()` parses the JSON, validates the command against an allowlist, and determines whether the result is a cursor or scalar. `help` is matched by regex and returns a formatted command reference.
- `CheckQuery`: compilation itself is the validation — the command allowlist (`mongosh/command/allowlist.go`) rejects anything not explicitly listed. Default-deny.
- Three result paths, all presented as PostgreSQL result sets:
  - **Cursor results** (find, aggregate, listCollections, listIndexes) — streamed one document per row via `MongoCursorIterator`. Each row is a single `result` column (OID 114, json) containing canonical Extended JSON (`bson.MarshalExtJSON` with relaxed mode off). Command tag: `SELECT <n>`.
  - **Scalar results** (count, distinct, ping, etc.) — one row, one `result` column, same Extended JSON encoding, via `MongoScalarIterator`. Command tag: `OK`.
  - **Formatted results** (help) — `FormattedIterator` returns columnar TEXT output. Command tag: `SELECT <n>`.
- Cancel embeds a per-session comment tag (`peerdb-<8hex>`) in commands, then uses `currentOp` / `killOp` to stop matching operations.
- `TxStatus` always returns `'I'`.
- The `mongosh/` subpackage contains: `compile.go` (Extended JSON parsing, shell command handling, orchestration) and `command/allowlist.go` (wire command allowlist with metadata for cursor/scalar result kind and admin DB routing). The test suite (`mongosh/compile_test.go`) validates compilation, allowlist enforcement, and shell commands.

## Requirements for a new upstream

### 1. Implement the `Upstream` interface

Create `upstream_<name>.go` in the `switchboard` package. Implement all methods of the `Upstream` interface. Constructor should follow the pattern `NewXxxUpstream(ctx, config, ...) (Upstream, error)`.

### 2. Implement `ResultIterator`

Your iterator translates the upstream driver's result representation into the text-encoded `[][]byte` rows that the session streams to the client.

Key constraints:
- `RowValues()` must return text-format byte slices. The session does not do any type conversion.
- `FieldDescriptions` determines how psql renders columns. Set `DataTypeOID` strategically: use numeric OIDs (e.g. 23 for int4, 1700 for numeric) for right-aligned display, text OID (25) or json OID (114) otherwise.
- Multi-statement support: if the upstream can return multiple result sets, `NextResult()` must advance through them. If not, return `true` once then `false`.
- `CommandTag()` must return a valid PostgreSQL command tag string after `Close()` is called on the current result. Examples: `"SELECT 5"`, `"UPDATE 3"`, `"INSERT 0 1"`.
- `Err()` should wrap upstream errors in `*UpstreamError` (contains `pgproto3.ErrorResponse`) so the session can forward structured error details. If you return a plain error, the session wraps it in a generic `ErrorResponse` with SQLSTATE `XX000`.
- `CloseAll()` must release all resources (connections, cursors) and return any final error.

### 3. Implement `CheckQuery`

This runs before `Exec` and must reject anything dangerous for a read-only diagnostic session. The bar is "prevent accidental damage," not "withstand adversarial bypass" — catch honest mistakes reliably, but don't over-invest in edge cases that only matter if someone is actively trying to break out.

Guidelines:
- Use a real parser where practical. MySQL uses TiDB's AST parser, MongoDB uses goja + a command registry/allowlist. PostgreSQL uses a first-keyword allowlist with `pgsplit` for statement splitting and a `default_transaction_read_only = on` filter, because a real parser would be a very heavy CGo dependency. Match the approach to what the upstream's security model requires.
- Default-deny is preferred: allowlist safe operations rather than blocklisting dangerous ones, where the query language makes this practical.
- Reject `pg_catalog` queries (psql `\d` commands) — they don't work against non-PostgreSQL upstreams. Use the shared `SqlSelectPgCatalogRe` regex from `upstream.go`.
- Return a clear error message explaining what was blocked and why.

### 4. Implement cancel

`Cancel(ctx)` must stop a running query. The mechanism is upstream-specific:
- If the upstream supports native cancel (like PostgreSQL's `CancelRequest`), use it.
- If not, you need a way to identify and kill the running operation. MySQL uses `KILL QUERY <id>`. MongoDB embeds a comment tag in commands and searches `currentOp`.

`BackendKeyData()` returns (pid, secret) used to route cancel requests from the client to the correct session. The (pid, secret) pair must be unique across sessions — use the upstream's real pid where available, or a fixed value (e.g. MongoDB uses pid=0) with a random secret.

### 5. Implement `ServerParameters`

Return a `map[string]string` of fake PostgreSQL parameters so psql doesn't break. At minimum:

```
server_version      e.g. "16.0-yourdb-proxy"
server_encoding     "UTF8"
client_encoding     "UTF8"
DateStyle           "ISO, MDY"
TimeZone            "UTC"
integer_datetimes   "on"
standard_conforming_strings "on"
```

psql doesn't validate these, but it expects them during startup.

### 6. Implement `TxStatus`

Return `'I'` (idle) if the upstream doesn't expose transaction state. If it does (like PostgreSQL), return the real value (`'I'`, `'T'`, or `'E'`).

### 7. Register in the factory

Add a case to the `switch peer.Type` in `NewUpstream()` (`upstream.go`). Load the peer's config from the proto, validate it, call your constructor.

### 8. Set read-only at session level

If the upstream supports session-level read-only mode, enable it during connection setup (not per-query). PostgreSQL sets `default_transaction_read_only = on`, MySQL sets `SESSION TRANSACTION READ ONLY`. This is defense-in-depth — `CheckQuery` is the primary gate.

### 9. Tests

Add unit tests for `CheckQuery` logic in the `switchboard` package. Add integration tests in `e2e/switchboard_<name>_test.go` covering: basic queries, guardrail enforcement, cancel, error propagation, and blocked query rejection.
