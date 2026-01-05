# PgWire MySQL Proxy - Requirements & Design

## Goal

Allow operators to use `psql` as a universal SQL client for MySQL databases (and potentially other DBs in the future). The proxy speaks PostgreSQL wire protocol to clients while forwarding queries to MySQL.

## Non-Goals

- SQL dialect translation (queries are native MySQL, passed through verbatim)
- Full PostgreSQL semantic compatibility
- Binary format support
- Prepared statements / extended query protocol

---

## Core Behavior

**Query Flow:**
1. Client (psql) connects via PostgreSQL wire protocol
2. Proxy authenticates client (TBD: pass-through to MySQL or proxy-level auth)
3. Client sends SQL query in MySQL dialect
4. Proxy forwards query verbatim to MySQL
5. Proxy converts MySQL result set to PostgreSQL wire format
6. Client displays results

**Result Faithfulness:**
- Show MySQL data as-is, human-readable
- No semantic transformation of values
- Dates, timestamps, etc. rendered as MySQL returns them

---

## Type Mapping (Simple)

Everything returned as text format. OIDs chosen for psql display alignment:

| MySQL Type | PG OID | PG Type Name | Display |
|------------|--------|--------------|---------|
| TINYINT, SMALLINT, MEDIUMINT, INT, BIGINT | 20 | INT8 | right-aligned |
| FLOAT, DOUBLE | 701 | FLOAT8 | right-aligned |
| DECIMAL | 1700 | NUMERIC | right-aligned |
| Everything else | 25 | TEXT | left-aligned |

Actual values are the text representation MySQL provides - no parsing/reformatting.

---

## Transaction Status

**Decision:** No tracking. Always return `'I'` (idle).

Rationale:
- MySQL doesn't expose transaction state like PostgreSQL
- Would require parsing BEGIN/COMMIT/ROLLBACK
- psql works fine without it, just no `*` indicator in prompt
- Admin tool use case doesn't need transaction awareness

---

## Cancel Support

**Decision:** Implement via `KILL QUERY`.

Flow:
1. psql sends `CancelRequest` with (pid, secret) on separate connection
2. Proxy looks up session by (pid, secret)
3. Proxy opens ephemeral MySQL connection with same credentials
4. Executes `KILL QUERY <connection_id>`
5. Closes ephemeral connection

Permissions: None required beyond basic grants - MySQL allows users to kill their own queries.

Store `connection_id` (from `conn.GetConnectionID()`) in session during upstream connect.

---

## Server Parameters (Startup Greeting)

Fake values sufficient for psql:

```
server_version     = "8.0.0-mysql-proxy" (or actual MySQL version)
server_encoding    = "UTF8"
client_encoding    = "UTF8"
DateStyle          = "ISO, MDY"
TimeZone           = "UTC"
integer_datetimes  = "on"
standard_conforming_strings = "on"
```

psql doesn't validate these meaningfully.

---

## Error Handling

Map MySQL errors to PostgreSQL ErrorResponse:
- Severity: "ERROR"
- Code: `"HY000"` (general error) or simple mapping of common MySQL codes
- Message: MySQL error message verbatim

No need for detailed SQLSTATE mapping - message is what matters for admin use.

---

## Guardrails

Reuse existing guardrail infrastructure:
- Row limit (MaxRows)
- Byte limit (MaxBytes)
- Deny list (default dangerous: LOAD DATA, etc.)
- Allow list (optional)

Update default deny list for MySQL-specific dangerous statements:
- `LOAD DATA` (file access)
- `INTO OUTFILE` / `INTO DUMPFILE`
- `RESET` (admin)
- `FLUSH` (admin)
- `PURGE` (binary logs)

---

## What Stays the Same

- TCP listener and connection management
- Session lifecycle and cleanup
- TLS support (client-side)
- Idle timeout handling
- SQL splitting for guardrail checks
- Metrics collection
- Graceful shutdown
