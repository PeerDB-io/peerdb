# MySQL Diagnostic Query Guardrails (Allowlist Spec)

This spec defines the **SQL allowlist + AST restrictions** for an admin tool that lets trusted operators run **diagnostic, read-only** queries against customer MySQL with guardrails against accidental side effects.

## Goals

- Accept “normal” diagnostic queries (including EXPLAIN and multi-step read-only transactions).
- Prevent accidental writes, file export, session/transaction read-write mode flips, and locking reads.

---

## 1) Parsing & statement handling

### MUST
- Parse SQL with a real MySQL-compatible parser (e.g., TiDB parser) and validate using the AST (no substring-only logic).
- Support **multiple statements per request** (to allow transactions), by parsing into `stmts[]` and validating **each statement**.

---

## 2) Top-level statement allowlist

Only these statement families are allowed:

### Read-only query statements
- `SELECT` (including `WITH`/CTEs, subqueries, joins, set-ops like `UNION`)
- `SHOW` (no additional restrictions)
- `DESCRIBE` / `DESC` (no additional restrictions)
- `EXPLAIN` (restricted; see below)

### Transaction control (read-only only)
- `START TRANSACTION ...`
- `BEGIN` (synonym)
- `COMMIT`
- `ROLLBACK`
- `SAVEPOINT <name>`
- `RELEASE SAVEPOINT <name>`
- `ROLLBACK TO SAVEPOINT <name>`

Everything else is **DENIED** (including but not limited to DML/DDL, `SET`, `USE`, `CALL`, `DO`, `LOAD DATA`, `LOCK TABLES`, `FLUSH`, `RESET`, `KILL`, `PREPARE/EXECUTE`, `XA ...`, etc.).

---

## 3) Transaction rules (read-only)

Transactions are allowed only if they cannot be made read-write.

### MUST
- On session bootstrap, set:
  - `SET SESSION tx_read_only = ON`

### Recommended sequence validation (optional but simple)
If multiple statements are provided in one request:
- Allow any interleaving of:
  - transaction control statements listed above, and
  - allowed read-only query statements (SELECT/SHOW/DESCRIBE/EXPLAIN)
- Still enforce all per-statement restrictions below (e.g., no `FOR UPDATE`, no `INTO OUTFILE`, etc.).

---

## 4) SELECT rules (AST-based restrictions)

### Allowed
- Normal SELECT features: CTEs, joins, subqueries, `UNION`, `WHERE/GROUP/HAVING/ORDER`, `LIMIT/OFFSET`, etc.
- `SLEEP(n)` is allowed.

### Denied (MUST)

#### 4.1) `SELECT ... INTO ...` (any kind)
Reject any SELECT that has an INTO target, including:
- `INTO OUTFILE`
- `INTO DUMPFILE`
- `INTO @var` (into user variables)

Implementation: reject if `SelectStmt.SelectIntoOpt != nil` (or equivalent in your AST).

#### 4.2) Locking reads
Reject any SELECT that requests locks, including:
- `FOR UPDATE`
- `FOR SHARE`
- `LOCK IN SHARE MODE`
- `NOWAIT`, `SKIP LOCKED` variants

Implementation: reject if `SelectStmt.LockInfo != nil` and lock type is not “none”.

### Denied functions (SHOULD; defense-in-depth)
Even in read-only mode, these introduce side effects or operational risk. Reject SELECT if it contains calls to:
- File access: `LOAD_FILE(path)`
- User-level locks: `GET_LOCK`, `RELEASE_LOCK`, `IS_FREE_LOCK`, `IS_USED_LOCK`

Implementation: traverse expression nodes; when encountering function-call nodes, compare function name case-insensitively against the denylist.

> Note: UDFs are environment-specific. If you want to be safer, also deny *unknown* functions unless allowlisted, but that may break legitimate diagnostics.

---

## 5) EXPLAIN rules

### Allowed
- `EXPLAIN <select>` (including EXPLAIN FORMAT variants)
- `EXPLAIN ANALYZE <select>` (including EXPLAIN FORMAT variants)

### Denied (MUST)
- EXPLAIN of non-SELECT statements (even if MySQL supports it)

---

## 6) SHOW rules

- Allow all `SHOW ...` statements with no extra restrictions.
- (Still subject to global constraints like single-request timeouts and cancellation.)

---

## 7) DESCRIBE / DESC rules

- Allow all `DESCRIBE` / `DESC` statements with no extra restrictions.

---

## 8) Explicitly disallowed statements (non-exhaustive)

Even if they look “diagnostic”, deny:
- Any `SET ...` (including `SET TRANSACTION`, `SET SESSION`, `SET @@...`)
- Any DDL/DML: `INSERT/UPDATE/DELETE/REPLACE`, `CREATE/ALTER/DROP/TRUNCATE/RENAME`
- Admin/maintenance: `ANALYZE TABLE`, `OPTIMIZE TABLE`, `REPAIR TABLE`, `FLUSH`, `RESET`, `KILL`
- Stored code / side effects: `CALL`, `DO`
- Prepared statements: `PREPARE`, `EXECUTE`, `DEALLOCATE PREPARE`
- File import/export: `LOAD DATA`, and any `SELECT ... INTO ...` (covered above)
- Table locks: `LOCK TABLES`, `UNLOCK TABLES`

---

## 9) Session bootstrap (tool-enforced; user SQL cannot change it)

Because `SET` is disallowed in user SQL, the tool should configure the session on connect:

- `SET SESSION tx_read_only = ON`
- `SET SESSION max_execution_time = <ms>` (best-effort; primarily affects SELECT)
- Lock wait timeouts:
  - `SET SESSION lock_wait_timeout = <seconds>` (metadata lock waits)
  - `SET SESSION innodb_lock_wait_timeout = <seconds>` (InnoDB row lock waits)
- Optional network timeouts:
  - `SET SESSION net_read_timeout = <seconds>`
  - `SET SESSION net_write_timeout = <seconds>`

Also implement proxy-side cancellation (context deadline + kill query) so timeouts aren’t only best-effort.
