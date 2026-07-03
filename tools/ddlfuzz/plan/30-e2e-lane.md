# 30 — Live-binlog end-to-end slow lane

Component plan under `00-overview.md` (binding). Validates what the in-process parse oracle
structurally cannot:

1. **Plumbing**: real QueryEvent bytes + status vars flowing through
   `sqlModeFromStatusVars` + `parseQueryEvent`, exactly as `cdc.go:557-583` consumes them.
2. **Semantics**: our extracted schema delta vs the schema change the server *actually applied*
   (information_schema diff). This is the only lane that can catch the crown-jewel failure:
   a statement we classify as benign that in fact changed the fixture table's columns.

Everything here lives in `/Users/ilia/Code/peerdb/tools/ddlfuzz/e2e/` plus one thin main under
`cmd/ddlfuzz-e2e/` (see Contract issues #1). All paths below are relative to
`/Users/ilia/Code/peerdb/tools/ddlfuzz/` unless absolute.

## Goal

- Run K=4 workers per engine against live `mysql:9.7.0` and `mariadb:13.0.1-rc` containers,
  executing constrained-profile DDL on a fixture table, diffing information_schema before/after.
- Tail each server's binlog with a go-mysql `BinlogSyncer` configured like `cdc.go`, pair every
  QueryEvent 1:1 with the statement the worker submitted, and run the plumbing + semantic checks.
- Emit findings into the shared `state/findings/<sig>` pipeline with `lane:"e2e"`.
- Confirm supervisor-queued post-fix repros live; feed generator-refinement and
  oracle-cross-check side channels.
- Crash-restartable, health-checkable, stats-reporting; lifecycle owned by component 40, with
  up/down/health scripts provided here.

### Image-tag ground truth (verified 2026-07-03 against Docker Hub)

- `mysql:9.7.0` **exists** (also 9.7.1). Pin **`mysql:9.7.0`** — exact match for the oracle
  checkout (tag mysql-9.7.0). arm64 manifest present. Zero expected version skew.
- MariaDB has **no 13.1 tag** (13.1.0-alpha is unreleased main). Available: `13.0.1-rc`
  (arm64 present), then 12.3.2 GA. Pin **`mariadb:13.0.1-rc`**. Skew vs the 13.1.0-alpha
  oracle is expected signal: a live-vs-oracle behavioral difference on MariaDB may be a genuine
  13.0→13.1 server change, not a harness bug. Disposition: ledger entry citing the MariaDB
  commit/KB page, per the 00 correctness contract (no severity tiers; every divergence gets a
  disposition).

## Interfaces

### Consumed

- **Export shim** `flow/connectors/mysql/ddlfuzz_export.go` (`//go:build ddlfuzz`), from 00:
  `FuzzDDLSignature(query []byte, sqlMode uint64, isMariaDB bool) (string, error)`.
- **Shim additions this component requires** (exact text; component 20 lands the file, these two
  functions must be included — see Contract issues #2):

  ```go
  // FuzzSQLModeFromStatusVars re-exports sqlModeFromStatusVars for the e2e lane's
  // plumbing check against real QueryEvent status vars.
  func FuzzSQLModeFromStatusVars(statusVars []byte) (uint64, bool) {
      return sqlModeFromStatusVars(statusVars)
  }

  // FuzzParseForE2E parses like the production QueryEvent path and returns the parsed
  // statements as JSON for structured (non-signature) comparison. err is parseQueryEvent's
  // error; panics are NOT recovered here.
  //
  // JSON shape (field names fixed, schema below):
  // {"stmts":[
  //   {"kind":"alter_table","schema":"","table":"t","specs":[
  //     {"op":"add","cols":[{"name":"c","type_str":"decimal(10,2) unsigned",
  //       "not_null":true,"precision":10,"scale":2}],"has_position":false},
  //     {"op":"change","old_name":"a","cols":[<col>],"has_position":false},
  //     {"op":"rename_col","old_name":"a","new_name":"b"},
  //     {"op":"drop","old_name":"a"}]},
  //   {"kind":"rename_table","pairs":[{"old_schema":"","old_table":"a",
  //     "new_schema":"","new_table":"b"}]}]}
  func FuzzParseForE2E(query []byte, sqlMode uint64, isMariaDB bool) ([]byte, error)
  ```

  Mapping from `ddlAlterSpec` (writeup, API section): `NewColumnName != ""` → `rename_col`;
  `len(NewColumns) > 0 && OldColumnName != ""` → `change`; `len(NewColumns) > 0` → `add`
  (MODIFY is an `add`-shaped spec with one column, same as production consumption); else →
  `drop`. `precision`/`scale` are `ddlColumnDef.Precision/Scale` verbatim (−1 = not written).
  This intentionally parallels the oracle digest JSON (00) so 20's reconciliation helpers can be
  shared; the only deltas are `precision`/`scale` ints instead of `params_written` and no
  `"other"` kind (benign statements produce no entry, matching `parseQueryEvent`).
- **Component 20** (`internal/` packages; interfaces this plan needs, 20 must export them):
  - `internal/gen`: `func GenerateConstrained(r *rand.Rand, v Vocab, p Profile) string` where

    ```go
    type Vocab struct {
        Table      string   // unqualified fixture table name
        Columns    []string // current live column names (worker refreshes from snapshot)
        FreshNames []string // pool for new column names (fixture-adversarial spellings)
        Types      []string // type vocabulary (written forms incl. params), engine-specific
        IsMariaDB  bool
    }
    type Profile struct {
        HeadsAlterOnly     bool    // only ALTER TABLE / RENAME TABLE heads
        NoAlterRenameTo    bool    // exclude RENAME [TO|AS|=] inside spec lists
        NoConvertCharset   bool    // exclude CONVERT TO CHARACTER SET
        RenameTableWeight  float64 // fraction of RENAME TABLE statements (0.05)
        MaxSpecs           int     // 3
    }
    ```

  - `internal/findings`: `func Record(stateDir string, f Finding) (sig string, isNew bool, err error)`
    writing the 00 `findings/<sig>/{repro.sql,meta.json}` layout with 20's canonical descriptor
    (stable, identifier-insensitive). `Finding` carries class, engine, sql_mode, statement bytes,
    our_sig/our_error, and an opaque `Meta map[string]any` merged into meta.json.
  - `internal/minimize`: fast-lane minimizer callable in-process:
    `func Minimize(stmt []byte, sqlMode uint64, engine string, reproduces func([]byte) bool) []byte`.
  - `ddlfuzz replay` subcommand accepting `--from <jsonl> --expect-accept` (oracle cross-check;
    consumed indirectly via supervisor cron — see Cross-checks).
  - Fast-lane corpus at `state/corpus/{mysql,mariadb}/<sha1>` + `<sha1>.meta.json` (read-only).
- **Docker** (compose v2), ports 13306/13307 on localhost.
- **flow module** via `replace` (go-mysql `client` + `replication` packages, `QkindFromMysqlColumnType`).

### Provided

- `e2e/compose.yml`, `e2e/up.sh`, `e2e/down.sh`, `e2e/health.sh`, `e2e/smoke.sh`.
- `build/ddlfuzz-e2e` binary (`go build -tags ddlfuzz -o build/ddlfuzz-e2e ./cmd/ddlfuzz-e2e`).
- Findings with `lane:"e2e"` in the shared pipeline; classes defined in Design §7.
- State files (all under `state/`, additions to the 00 layout — Contract issues #3):
  - `e2e-stats.json` — heartbeat stats (schema in §9), consumed by component 40.
  - `e2e-exec-rejects.jsonl` — generator-refinement feed.
  - `e2e-live-accepted.jsonl` — sampled live-accepted `(statement, sql_mode, engine)` for the
    oracle cross-check replay.
  - `e2e-queue/{pending,processing,done}/` — supervisor confirmation-replay handoff (§8).

## Design

### 1. Containers (`e2e/compose.yml`)

Own compose project `ddlfuzz-e2e`, own default network, distinct container names and host ports
(ancillary uses 3306/3307/3308; toxiproxy publishes 9902-9904, 10001, 12001-12005, 14001-14003,
18474; dozzle 8118 — 13306/13307 collide with nothing).

Settings rationale (verified against docs + cdc.go):

- **DDL QueryEvents are emitted regardless of `binlog_format`** — DDL is always statement-logged
  (MySQL ref manual 5.4.4.1 "Statements that ... DDL are logged as statements"; MariaDB KB
  "Binary Log Formats"). So `binlog_format`/`binlog-row-metadata` are irrelevant to this lane and
  deliberately omitted (this lane never consumes row events).
- **sql_mode status var is always present**: both engines write `Q_FLAGS2_CODE` (0) then
  `Q_SQL_MODE_CODE` (1) unconditionally for every Query event (mysql
  `sql/log_event.cc Query_log_event::write`, maria `sql/log_event_server.cc:1083-1087` — the
  exact ordering `sqlModeFromStatusVars` depends on). `FuzzSQLModeFromStatusVars` returning
  `ok=false` on a live event is therefore itself a plumbing finding.
- **File/pos, not GTID**: `cdc.go` supports both (`startCdcStreamingFilePos` /
  `startCdcStreamingGtid`); file/pos (`syncer.StartSync(pos)`) is the simpler and needs zero
  extra server config (MySQL 9 defaults: `log_bin=ON`, `gtid_mode=OFF`). Position captured at
  matcher start via `SHOW BINARY LOG STATUS` (MySQL ≥8.4 removed `SHOW MASTER STATUS`) and
  `SHOW BINLOG STATUS` (MariaDB ≥10.5 name; MASTER-form deprecated on 13.x).
- **MariaDB needs `--log-bin`** (off by default); MySQL does not.
- `explicit_defaults_for_timestamp=ON` on both: kills the implicit
  `timestamp NOT NULL DEFAULT CURRENT_TIMESTAMP` class so nullability comparison stays exact.
- tmpfs datadir + `sync_binlog=0` + `innodb_flush_log_at_trx_commit=0`: throughput; data loss on
  restart is fine (lane is stateless, §9).
- `binlog_expire_logs_seconds=86400` ample; matcher-backlog throttling (§9) keeps lag ≪ that.

```yaml
name: ddlfuzz-e2e

services:
  mysql:
    container_name: ddlfuzz-mysql
    image: mysql:9.7.0
    command:
      - --server-id=1
      - --sync_binlog=0
      - --innodb_flush_log_at_trx_commit=0
      - --innodb_buffer_pool_size=256M
      - --explicit_defaults_for_timestamp=ON
      - --binlog_expire_logs_seconds=86400
      - --skip-name-resolve
      - --character-set-server=utf8mb4
    tmpfs:
      - /var/lib/mysql
    ports:
      - "13306:3306"
    environment:
      MYSQL_ROOT_PASSWORD: ddlfuzz
    healthcheck:
      test: ["CMD", "mysqladmin", "ping", "-h", "localhost"]
      interval: 2s
      timeout: 10s
      retries: 30
      start_period: 30s

  mariadb:
    container_name: ddlfuzz-mariadb
    image: mariadb:13.0.1-rc
    command:
      - --log-bin=fuzz
      - --server-id=1
      - --sync_binlog=0
      - --innodb_flush_log_at_trx_commit=0
      - --innodb_buffer_pool_size=256M
      - --explicit_defaults_for_timestamp=ON
      - --binlog_expire_logs_seconds=86400
      - --skip-name-resolve
      - --character-set-server=utf8mb4
    tmpfs:
      - /var/lib/mysql
    ports:
      - "13307:3306"
    environment:
      MARIADB_ROOT_PASSWORD: ddlfuzz
    healthcheck:
      test: ["CMD", "mariadb-admin", "ping", "-h", "localhost", "-uroot", "-pddlfuzz"]
      interval: 2s
      timeout: 10s
      retries: 30
      start_period: 30s
```

tmpfs means every container (re)start reinitializes the datadir (~10-25s); `up.sh` waits on
health. No volumes, no initdb mounts — workers create their own schemas.

### 2. Fixture

Per worker k ∈ 1..4 and engine: schema `fuzz_w<k>`, table `fixture`. Worker session does
`USE fuzz_w<k>`; every statement it executes is unqualified or qualified as `fuzz_w<k>` — this
makes `ev.Schema` (the session default db) a reliable per-worker filter for the matcher.

Canonical fixture, MySQL (exact statement; the reset replays it verbatim):

```sql
CREATE TABLE `fixture` (
  `id` bigint NOT NULL,
  `c_int` int,
  `c_tiny` tinyint(1),
  `c_dec` decimal(12,4),
  `c_dbl` double,
  `c_vchar` varchar(64),
  `c_text` text,
  `c_vbin` varbinary(32),
  `c_blob` blob,
  `c_date` date,
  `c_dt` datetime(3),
  `c_ts` timestamp(6) NULL,
  `c_time` time,
  `c_year` year,
  `c_enum` enum('a','b'),
  `c_set` set('x','y'),
  `c_json` json,
  `c_bit` bit(8),
  `c_geom` geometry,
  `first` int,
  `after` int,
  `period` int,
  `system` int,
  `vector` int,
  `back``tick` int,
  `имя_utf8` int,
  PRIMARY KEY (`id`)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
```

MariaDB variant: identical plus, before the PRIMARY KEY line:

```sql
  `c_uuid` uuid,
  `c_inet4` inet4,
  `c_inet6` inet6,
  `c_vec` vector(4),
```

Adversarial names cover the parser's documented LL(2) ambiguity points (`first`, `after`,
`period`, `system`, `vector` — writeup §Parser), a doubled-backtick identifier, and a non-ASCII
UTF-8 identifier. MariaDB UDT columns exercise the open-type-slot path; `QkindFromMysqlColumnType`
errors on them symmetrically on both comparison sides (renders `ERR`), matching production's
log+skip behavior.

`FreshNames` pool for generated ADD/CHANGE/RENAME targets:
`n1..n8, first2, after2, period2, system2, vector2, ` + "`" + `new``tick` + "`" + `-spelled name,
имя2` — new names reuse the adversarial spellings.

### 3. sql_mode palette

Per case the worker issues `SET SESSION sql_mode = '<entry>'` (SET of a system variable is not
binlogged; the value reaches the binlog only via subsequent events' status vars — exactly the
production propagation path we are validating). Palette:

| entry | engines | notes |
|---|---|---|
| `''` | both | weight 3 |
| `'ANSI_QUOTES'` | both | |
| `'NO_BACKSLASH_ESCAPES'` | both | |
| `'ANSI_QUOTES,NO_BACKSLASH_ESCAPES'` | both | |
| `'ANSI'` | both | composite; expands differently per engine — exercises expansion |
| `'ORACLE'` | mariadb | composite: includes PIPES_AS_CONCAT, ANSI_QUOTES, ... |
| `'MSSQL'` | mariadb | enables `[bracket]` idents in the lexer |
| `'MSSQL,NO_BACKSLASH_ESCAPES'` | mariadb | |

Because composites expand server-side, the expected bitmask is **not** computed from the entry
string. Instead, after SET the worker reads back `SELECT @@SESSION.sql_mode` and derives
`expectRelevant uint64` by mapping the returned comma-separated names through:

```go
var relevantBits = map[string]uint64{
    "ANSI_QUOTES":           1 << 2,  // sqlModeANSIQuotes  (ddl_lexer.go:13)
    "ORACLE":                1 << 9,  // sqlModeOracle      (MariaDB only)
    "MSSQL":                 1 << 10, // sqlModeMSSQL       (MariaDB only)
    "NO_BACKSLASH_ESCAPES":  1 << 20, // sqlModeNoBackslashEscapes
}
const relevantMask = 1<<2 | 1<<9 | 1<<20 | 1<<10
```

These four bits are the only ones `ddl_lexer.go`/`ddl_parser.go` consult; both engines agree on
positions below bit 32 (writeup §Lexer). Server-added/expanded bits outside the mask are expected
and ignored. Readback also stores the canonical string for findings meta.

### 4. Case protocol & binlog correlation

Case IDs: `<runNonce>_<seq>` where runNonce = 8 hex chars of process-start unix nanos, seq
monotonic per worker. Uniqueness across restarts matters only for log readability — the matcher
never reads pre-restart binlog (position captured fresh at startup).

Worker executes, per case n, on its single connection (serially):

1. **Marker**: `DROP TABLE IF EXISTS ` + "`" + `__m_<caseID>` + "`" — a real QueryEvent in schema
   `fuzz_w<k>` (DROP TABLE IF EXISTS of a nonexistent table IS binlogged on both engines).
   Before executing, the worker pushes `expect{kind:marker, caseID, text}` onto its FIFO to the
   matcher.
2. `SET SESSION sql_mode = '<palette entry>'` + readback (§3). Not binlogged.
3. **Before-snapshot** (§5 query). Reads; not binlogged.
4. **DDL under test**. On driver error → record `exec-reject` (input unreachable by the binlog
   invariant; append to `e2e-exec-rejects.jsonl` if generator/corpus-sourced, answer the queue if
   queue-sourced), push nothing, done. On success → **after-snapshot**, compute the ground-truth
   delta, push `expect{kind:ddl, caseID, text, sqlModeReadback, expectRelevant, before, after}`.
5. **Reset when due** (policy below): `DROP TABLE ` + "`" + `fixture` + "`" (or the renamed name)
   + canonical CREATE; both pushed as `expect{kind:control}`.

**Matcher pairing.** The binlog is a single totally-ordered stream; the server executes each
session's statements in submission order; every binlogged statement carries the session default
db as `ev.Schema`. Worker k only ever has default db `fuzz_w<k>` and never touches other schemas,
so the subsequence of QueryEvents with `string(ev.Schema) == "fuzz_w<k>"` is exactly worker k's
binlogged-statement sequence, in order. The matcher therefore pops worker k's FIFO positionally.
That alone is sufficient for correctness; the markers add (a) a resync anchor with an embedded
caseID so any unexpected event (e.g. a server-generated statement) is detected at the very next
marker instead of silently shifting all subsequent pairings, and (b) positive confirmation of
exec-rejects: marker n immediately followed by marker n+1 proves case n produced no event, which
must agree with the worker's recorded exec-reject.

Pairing text policy: marker/control texts must match byte-for-byte — mismatch is a **harness
error** (crash loudly, do not file a finding). For the DDL under test, byte-inequality of
`ev.Query` vs submitted text is itself reportable signal: per the writeup's binlog invariants,
ALTER/RENAME are binlogged verbatim (`rewrite_query` touches only credential-bearing statements).
Policy: if bytes differ, still run both checks against `ev.Query`, and file finding class
`e2e-query-rewrite` carrying both texts (dedup collapses repeats). Known benign exception to
pre-encode: a trailing `` /* generated by server */`` comment (both engines append it to
server-generated DROPs; should never appear on our DDL, but if observed with an otherwise equal
prefix, classify as `e2e-query-rewrite` all the same — let disposition decide).

If the FIFO is empty when an event for `fuzz_w<k>` arrives, the matcher blocks up to 30s for the
worker to push (worker computes the after-snapshot before pushing); timeout = harness error.

**Reset policy.** A fresh before-snapshot is taken every case, so fixture drift never corrupts
comparisons — reset exists only to keep the vocabulary canonical. Reset when: column count > 60
or < 15, the table was renamed away (RENAME cases), the after-snapshot lost ≥ 3 canonical
adversarial-name columns, or 32 cases elapsed since last reset. Consecutive independent ADD-only
cases therefore batch naturally between resets (each case's before-snapshot is the previous
after-snapshot); no extra machinery.

### 5. Ground-truth snapshot & delta

Exact snapshot query (mirrors the production `getTableSchemaForTable` source of truth,
`cdc.go:127-137`, plus `column_key` for the PRI reconciliation):

```sql
SELECT column_name, ordinal_position, column_type, is_nullable, column_key,
       numeric_precision, numeric_scale
FROM information_schema.columns
WHERE table_schema = 'fuzz_w<k>' AND table_name = 'fixture'
ORDER BY ordinal_position
```

(`table_schema`/`table_name` are literals per worker; no injection surface.) Delta =
`map[name]colRow` diff: `added` (in after only), `dropped` (in before only), `retained` with
per-column changes to `column_type`, `is_nullable`, `ordinal_position`,
`numeric_precision/scale`. RENAME TABLE cases instead snapshot
`SELECT table_name FROM information_schema.tables WHERE table_schema='fuzz_w<k>'`.

### 6. Checks (run by the matcher per paired DDL event)

**(a) Plumbing.**

1. `mode, ok := FuzzSQLModeFromStatusVars(ev.StatusVars)`; `!ok` → finding
   `e2e-statusvar-walk` (meta: status_vars hex).
2. `mode & relevantMask != expectRelevant` (from the worker's readback, §3) → finding
   `e2e-sqlmode-mismatch`.
3. `sigLive, errLive := FuzzDDLSignature(ev.Query, mode, isMariaDB)` vs
   `sigSub, errSub := FuzzDDLSignature(submittedText, expectRelevant, isMariaDB)`:
   any difference in (sig, err-ness) → finding `e2e-plumbing-sig` (catches status-var walker bugs
   feeding wrong modes, event-buffer aliasing, and query rewrites reaching the parser).
   Panic anywhere → contained by a per-event `recover` in the matcher → finding `e2e-panic` with
   the event hex; matcher continues.

**(b) Semantics** — predicted-state applier. Parse once:
`stmtsJSON, err := FuzzParseForE2E(ev.Query, mode, isMariaDB)`.

- `err != nil`: production would log + count `ParseSQLErrorsCounter` and skip (`cdc.go:564-568`)
  — but the server executed it, so extraction was lost. If the delta is empty → finding
  `e2e-parse-error-live-accept` only if not already ledgered; if the delta is non-empty → same
  class, higher-priority meta flag `schema_changed:true`. (Reported-error on
  actionable-that-executed is by design "reported", not silent — still a divergence per the 00
  contract: file it, dispositions sort it out.)
- `err == nil`: apply the parsed specs, in order, to the before-snapshot symbolically:

  | op | predicted effect |
  |---|---|
  | `add` (no old_name) | for each col: name present in after; qkind(after `column_type`) == qkind(our `type_str`); `is_nullable=='NO'` iff our `not_null` (reconciliations below) |
  | `change old→col` | `old` absent in after (unless col.name==old); col checked as add |
  | `rename_col old→new` | `old` absent, `new` present, `new`'s column_type == before[`old`].column_type |
  | `drop old` | `old` absent in after |
  | `rename_table pairs` | table-name sets move per pairs |

  qkind = `QkindFromMysqlColumnType(s, true, 0)` on **both** sides (our `type_str` and
  info_schema `column_type`); errors render `ERR` and `ERR==ERR` is a match (mirrors production
  log+skip and the tidb-diff `ddlDiffColSig` convention).

  Assertions:
  1. **Name sets**: predicted added/dropped/renamed name sets == actual delta name sets. Any
     actual added/dropped column not predicted → finding `e2e-missed-column-effect`. **The
     benign-but-schema-changed special case** (our parse yields zero stmts, delta non-empty) is
     this class with meta `benign_classification:true` — the safety-property violation this lane
     exists to catch.
  2. **Per-column**: qkind and nullability as per the table above; mismatch → finding
     `e2e-col-attr` (meta: which attribute, both values).
  3. **Decimal precision/scale**: only for qkind==numeric. Written params (`precision/scale
     >= 0`): must equal `numeric_precision/scale`. Unwritten (−1): server default applies —
     reconcile exactly as component 20's written-vs-default rule (reference, not duplicated
     here; 20 owns it — e2e imports the same helper from `internal/compare`). Mismatch →
     `e2e-col-attr`.
  4. **Position**: if any retained column's `ordinal_position` changed and no parsed spec has
     `has_position:true` and no drop/add-before-end explains it → finding `e2e-position-missed`
     (a missed FIRST/AFTER means production can't flag position-shifting DDL,
     `cdc.go:918-923,955-960`). The reverse (has_position:true, no shift) is NOT a finding:
     `ADD ... AFTER <last-column>` legitimately shifts nothing.

  Reconciliations (encoded, not ad hoc):
  - `is_nullable=='NO'` with our `not_null:false` is accepted iff after-snapshot
    `column_key=='PRI'` or the type maps to serial semantics — the parser extracts only written
    NOT NULL; PRIMARY KEY-implied NOT NULL is invisible to it by spec. Any other direction or
    cause → finding.
  - MariaDB `json` alias: our `type_str` `json` (qkind JSON) vs info_schema `longtext` (qkind
    text). Expected first finding, disposition ledger (production has the same asymmetry between
    ALTER-derived and info_schema-derived schemas). Don't special-case in code; let dedup+ledger
    absorb it — it is genuine signal worth one ledger entry.

**Profile exclusions rationale** (§Interfaces `Profile`): `RENAME TO` inside ALTER and
`CONVERT TO CHARACTER SET` are consumed-and-dropped by the parser **by spec** (writeup); live
they rename the table / rewrite column types, which the applier would flag every time. Excluding
them from the constrained generator avoids burning the lane on two known, spec'd, would-be-ledger
entries; their parse behavior stays covered by the fast lane. They remain reachable via the
confirmation queue (§8), which bypasses the profile.

### 7. Findings

All findings go through `internal/findings.Record` (20's descriptor → `<sig>`), with
`lane:"e2e"` and meta extended with (additive keys — Contract issues #4):

```jsonc
{
  "lane": "e2e",
  "class": "e2e-missed-column-effect",   // one of the classes defined in §6 + §8
  "engine": "mariadb",
  "sql_mode": 537001984,                  // extracted from status vars
  "sql_mode_name": "ANSI_QUOTES,NO_BACKSLASH_ESCAPES",
  "submitted_text": "...", "binlog_text_differs": false,
  "info_schema_delta": { "added": [...], "dropped": [...], "changed": [...] },
  "binlog_event_hex": "…",                // event.RawData
  "status_vars_hex": "…",
  "server_image": "mariadb:13.0.1-rc"     // skew bookkeeping vs the 13.1 oracle
}
```

`repro.sql` = the submitted statement bytes.

**Minimization**: first try the fast lane — call `internal/minimize.Minimize` with
`reproduces := func(b []byte) bool { fast-lane check via oracle+shim diverges the same way }`
(20 exposes this predicate for a single case). If the divergence is e2e-only (oracle and our
parse agree; the disagreement is with the live server), fall back to replay-based bisection:
ddmin over the ALTER spec list (split on top-level commas via a paren/quote-aware splitter —
reuse 20's tokenizer helper if exported, else a local 30-line splitter), re-executing each
candidate through the live pipeline on a dedicated minimizer schema `fuzz_min`, max 50
executions, keep the smallest statement that still reproduces the same class+descriptor. Store
`minimized:true/false` accordingly.

### 8. Side channels

- **Confirmation queue** (supervisor → e2e): supervisor drops
  `state/e2e-queue/pending/<sig>.json` `{sig, engine, sql_mode, sql_mode_name, statement}` after
  the fix agent claims a fix. A queue poller (one goroutine) claims via rename to
  `processing/<sig>.json` (rename is atomic; at-least-once on crash), runs the statement through
  a full live case on worker 1's engine-matching lane (bypassing the generator profile), writes
  `done/<sig>.json` `{sig, result: "confirmed-fixed"|"still-diverges"|"exec-reject", details}`.
  `exec-reject` means live can't validate (statement not executable against the fixture) —
  supervisor falls back to fast-lane verification only.
- **Generator refinement**: every exec-reject appends
  `{engine, source:"gen"|"corpus", statement, sql_mode_name, error}` to
  `state/e2e-exec-rejects.jsonl`. Not findings — unreachable inputs. Component 20 may mine it to
  tighten the constrained profile.
- **Oracle cross-check** (live-accept vs oracle verdict): every 50th successfully executed case
  appends `{engine, sql_mode, statement}` to `state/e2e-live-accepted.jsonl`. Supervisor cron
  (40) runs `ddlfuzz replay --from state/e2e-live-accepted.jsonl --expect-accept`; an oracle
  **reject** of a live-accepted statement is a finding against the oracle harness, class
  `oracle-reject-live-accept`, filed by the replay path (20's code, our data).

### 9. Throughput, ops, crash-resumability

- **Rate model** (tmpfs, empty tables, INSTANT/INPLACE alters): marker ~0.5ms + SET+readback
  ~0.5ms + snapshots 2×~1ms + DDL ~2-5ms + amortized reset (~8ms/32 cases) ≈ 6-9ms/case →
  ~110-170 cases/s/worker theoretical; assume contention halves it: **~200-300 cases/s/engine**
  with K=4. Over 72h that's >50M executed cases/engine — far beyond need; the lane can afford to
  be throttled hard and still exhaust its purpose. Measure in smoke and record actuals in
  `report.md`.
- **Backlog throttling**: matcher-pending count per engine (sum of FIFO depths). Workers pause
  while pending > 256, resume < 64. Keeps binlog lag bounded and memory flat.
- **Stats** `state/e2e-stats.json`, rewritten atomically (tmp+rename) every 10s:

  ```jsonc
  {"updated_at": 1751500000,
   "engines": {"mysql": {"cases": 123456, "exec_rejects": 23456, "findings": 2,
                "matcher_pending": 12, "matcher_last_event_at": 1751499999,
                "resets": 3901, "queue_done": 4}, "mariadb": {…}}}
  ```

  Supervisor heartbeat embeds it; staleness > 60s or `matcher_last_event_at` lag > 300s while
  cases advance = unhealthy (supervisor restarts the lane binary; containers only if
  `health.sh` fails).
- **Crash-resumability**: the lane persists nothing between cases except append-only jsonl,
  queue files, and findings — all written atomically (findings dir via 20's Record; jsonl via
  O_APPEND single-writer; queue via rename). On restart: recreate schemas/fixtures
  (`DROP DATABASE IF EXISTS fuzz_w<k>` + recreate), capture a fresh binlog position, start
  matcher, go. In-flight cases are safe to lose: e2e inputs are derived (generator is seeded
  fresh; corpus rewrites re-derivable; queue items are at-least-once via `processing/` re-scan
  on startup — anything in `processing/` older than 10min moves back to `pending/`). The lane is
  a validator, not the coverage driver — no unique corpus/coverage state lives here, so losing
  in-flight work costs only seconds of throughput.
- **Container lifecycle**: owned by component 40. Scripts provided:
  - `e2e/up.sh`: `docker compose -f "$(dirname "$0")/compose.yml" up -d --wait --wait-timeout 180`
  - `e2e/down.sh`: `docker compose -f "$(dirname "$0")/compose.yml" down -v --timeout 20`
  - `e2e/health.sh`: `docker compose -f ... ps --format json` → exit 0 iff both services
    healthy; plus a 1s TCP connect to 13306/13307.

## Implementation steps

Module context: everything in `tools/ddlfuzz` (module `github.com/PeerDB-io/peerdb/tools/ddlfuzz`,
`replace github.com/PeerDB-io/peerdb/flow => ../../flow`). All Go builds/tests for this lane use
`-tags ddlfuzz`.

1. **Shim additions** — add `FuzzSQLModeFromStatusVars` and `FuzzParseForE2E` (exact code in
   Interfaces) to `flow/connectors/mysql/ddlfuzz_export.go`. `FuzzParseForE2E` body: call
   `parseQueryEvent`, walk `[]ddlStatement` with a type switch mirroring
   `ddl_parser_tidb_diff_test.go:90-109`'s shape, marshal the structs above with
   `encoding/json`. Verify: `cd flow && go build -tags ddlfuzz ./...`.

2. **Compose + scripts** — write `e2e/compose.yml` exactly as §1, `e2e/up.sh`, `e2e/down.sh`,
   `e2e/health.sh` (§9; `chmod +x`). Verify: `e2e/up.sh && e2e/health.sh && e2e/down.sh`.

3. **Package `e2e/` skeleton** (package `e2e`), files:
   - `e2e/fixture.go` — canonical CREATE TABLE consts (`fixtureMySQL`, `fixtureMariaDB`, §2),
     schema setup/teardown (`CREATE DATABASE IF NOT EXISTS fuzz_w<k>`, `USE`, create fixture),
     reset logic + policy (§4), FreshNames pool.
   - `e2e/snapshot.go` — snapshot query (§5), `type colRow struct{Name string; Ordinal int;
     ColumnType, IsNullable, ColumnKey string; NumPrec, NumScale sql.NullInt64}`,
     `type snapshot map[string]colRow`, `func diff(before, after snapshot) delta`.
   - `e2e/sqlmode.go` — palette table (§3), `relevantBits`, readback parsing
     (`strings.Split(readback, ",")` → OR of mapped bits).
   - `e2e/worker.go` — case protocol (§4): connect via
     `github.com/go-mysql-org/go-mysql/client.Connect("127.0.0.1:13306", "root", "ddlfuzz",
     "fuzz_w<k>")` (same dep as flow), serial loop, statement sources mixed per case:
     70% `internal/gen.GenerateConstrained` with `Vocab{Columns: <current snapshot names>, …}`,
     25% corpus rewrite (step 5), 5% RENAME TABLE; queue items (step 7) preempt.
   - `e2e/matcher.go` — one per engine: `replication.NewBinlogSyncer(replication.
     BinlogSyncerConfig{ServerID: rand.Uint32(), Flavor: "mysql"|"mariadb", Host: "127.0.0.1",
     Port: 13306|13307, User: "root", Password: "ddlfuzz", DisableRetrySync: true,
     UseDecimal: true, ParseTime: true, HeartbeatPeriod: time.Minute})` — mirrors
     `cdc.go startSyncer` minus TLS/dialer/cache. Start position: `SHOW BINARY LOG STATUS`
     (mysql) / `SHOW BINLOG STATUS` (mariadb) on a plain connection → `mysql.Position` →
     `StartSync(pos)`. Event loop: only `*replication.QueryEvent` with `ev.Schema` ==
     some `fuzz_w<k>`; positional FIFO pairing + marker resync (§4); per-event `defer recover()`.
   - `e2e/checks.go` — plumbing checks (§6a) + applier/semantic checks (§6b), reconciliation
     rules, finding construction (§7).
   - `e2e/queue.go`, `e2e/stats.go`, `e2e/rejects.go` — §8, §9 (atomic file idioms specified
     there).

4. **Applier** (`e2e/checks.go` core): implement the op table in §6b as
   `func applyPredicted(before snapshot, stmts parsedStmts) predicted` +
   `func compare(pred predicted, d delta, after snapshot) []finding`. Every rule and
   reconciliation from §6b becomes one code branch — no unlisted tolerances.

5. **Corpus rewrite** (`e2e/rewrite.go`): pick a random `state/corpus/<engine>/<sha1>` (+ its
   `.meta.json` sql_mode); run `FuzzParseForE2E` on it; skip unless it yields ≥1
   `alter_table`/`rename_table`. Substitute identifiers **textually with verification**: replace
   every occurrence of the parsed table name (bare and backtick-quoted spellings) with `fixture`,
   map each distinct referenced old-column name onto live fixture columns (cycled), each distinct
   new/added name onto FreshNames; word-boundary replacement only (neighbor bytes not
   `[A-Za-z0-9_$\x80-\xFF]`). Then verify: `FuzzParseForE2E` on the rewritten text must yield the
   same op/spec structure (kinds, spec ops, has_position, type_strs) modulo names — else drop the
   entry. Mis-rewrites that survive verification simply exec-reject live (harmless, logged).

6. **Main** `cmd/ddlfuzz-e2e/main.go`: flags `--state <dir>` (default `../state` resolved
   absolute), `--engines mysql,mariadb`, `--workers 4`, `--cases N` (0 = unbounded),
   `--mysql-addr 127.0.0.1:13306`, `--mariadb-addr 127.0.0.1:13307`, `--smoke`. Wiring: per
   engine — setup schemas → capture position → start matcher goroutine → start K worker
   goroutines → stats ticker → queue poller. SIGTERM/SIGINT: stop workers, drain matcher 10s,
   final stats write, exit 0. Any harness error (§4) → log + exit 3 (supervisor restarts).
   Build: `go build -tags ddlfuzz -o build/ddlfuzz-e2e ./cmd/ddlfuzz-e2e`.

7. **Unit tests** (no containers; `go test -tags ddlfuzz ./e2e/`):
   - applier/compare over canned before/after snapshots for every op + every reconciliation
     (PRI-implied NOT NULL, maria json alias, decimal default, AFTER-last no-shift);
   - sqlmode readback→bits table incl. composites;
   - FIFO pairing incl. exec-reject (marker-marker) and desync detection;
   - corpus rewrite verification acceptance/rejection;
   - queue rename lifecycle.

8. **Smoke** `e2e/smoke.sh`:

   ```sh
   #!/bin/sh
   set -eu
   cd "$(dirname "$0")/.."
   go build -tags ddlfuzz -o build/ddlfuzz-e2e ./cmd/ddlfuzz-e2e
   e2e/up.sh
   trap 'e2e/down.sh' EXIT
   ./build/ddlfuzz-e2e --state state --cases 100 --smoke
   ```

   `--smoke`: run 100 cases/engine, then assert: every case either exec-rejected or was matched
   (no matcher timeouts); all markers paired; stats written; **zero unexplained divergences** —
   i.e. every finding filed (if any) has a class from §6/§7 and the process still exits 0 with a
   summary line `smoke: mysql cases=100 rejects=N findings=M; mariadb …` (findings are data, not
   smoke failures — the smoke gate is harness integrity, not parser perfection). Exit non-zero on
   harness errors only. Record measured cases/s in the output.

## Acceptance checks

1. `cd flow && go build -tags ddlfuzz ./...` — shim (incl. the two additions) compiles.
2. `cd tools/ddlfuzz && go vet -tags ddlfuzz ./e2e/... ./cmd/ddlfuzz-e2e && go test -tags ddlfuzz ./e2e/`
   — unit suite green.
3. `e2e/up.sh && e2e/health.sh` green on darwin/arm64; `docker compose -p ddlfuzz-e2e ps` shows
   both containers healthy; no port conflict with a concurrently running
   `ancillary-docker-compose.yml` stack (3306-3308 untouched).
4. `e2e/smoke.sh` passes end-to-end (100 cases/engine) and prints a measured rate ≥ 20
   cases/s/engine (conservative floor; expected far higher).
5. Plumbing exercised: smoke summary shows every palette entry hit ≥ 1 executed case per engine
   (worker cycles palette round-robin for the first |palette| cases to guarantee it).
6. Kill-resume: `kill -9` the binary mid-smoke, restart with same `--state`; it re-creates
   schemas, resumes cleanly, `e2e-queue/processing` items requeue; no duplicate-finding
   explosion (dedup by `<sig>`).
7. Manual seeded checks (one-off, then encode as unit tests where possible):
   - `ALTER TABLE fixture ADD COLUMN n1 int AFTER ` + "`" + `first` + "`" → matched, no findings;
   - a deliberately broken applier rule (temporary local edit) produces exactly one finding with
     `lane:"e2e"`, full meta (delta + event hex), and a valid `<sig>` directory — then revert.

## Risks & fallbacks

- **MariaDB 13.0-rc vs 13.1-alpha oracle skew** — divergences may be genuine server drift.
  Mitigation: `server_image` in meta; disposition path is ledger-with-citation. If 13.1 tags
  appear on Docker Hub mid-run, do NOT switch mid-run (stability over freshness); note for the
  next run.
- **Server-generated binlog events in fuzz schemas** (e.g. engine-internal DDL) desyncing
  pairing — markers bound the blast radius to one case; harness error surfaces it immediately.
  If a recurring benign server-generated event class appears, add it to an explicit skip-list in
  `matcher.go` with a comment citing the observed event, never a blanket skip.
- **Nullability reconciliation gaps** (implicit NOT NULL beyond PRI/serial) — will surface as
  `e2e-col-attr` findings; extend the reconciliation table only with a doc citation, else fix
  the parser. No silent widening.
- **go-mysql flavor quirks on MariaDB 13-rc** (unknown event types) — syncer surfaces errors;
  matcher treats unknown event types as ignorable (only QueryEvent consumed), syncer errors as
  harness errors → restart. If StartSync(file/pos) breaks on 13-rc specifically, fallback:
  MariaDB GTID (`StartSyncGTID` with `SELECT @@gtid_binlog_pos`) — isolated to matcher startup.
- **Exec-reject rate too high** (constrained generator still producing invalid DDL) — lane
  throughput is 20-50× oversized; even 90% rejects leaves millions of validated cases. The
  rejects file feeds profile tightening; no design change needed.
- **tmpfs memory pressure** (two servers + binlogs in RAM) — binlog on tmpfs grows with query
  text only (~100B/case → ~5GB/72h/engine worst case). Mitigation: `binlog_expire_logs_seconds=86400`
  plus a 6-hourly `FLUSH BINARY LOGS` + `PURGE BINARY LOGS BEFORE NOW() - INTERVAL 12 HOUR`
  issued by the stats ticker (matcher lag is bounded to seconds by throttling, so purging
  >12h-old logs is safe).

## Effort

- Shim additions + compose + scripts: 0.5 day
- Worker/matcher/pairing (steps 3, 6): 1.5 days
- Applier + checks + reconciliations (step 4): 1 day
- Rewrite, queue, stats, rejects, smoke (steps 5, 7, 8): 1 day

Total ≈ 4 days.

## Contract issues

1. **Layout addition**: 00's layout lists only `cmd/ddlfuzz/`. This plan adds
   `cmd/ddlfuzz-e2e/` (thin main; lane logic stays in `e2e/` per 00). Alternative — an `e2e`
   subcommand inside `cmd/ddlfuzz` — was rejected to keep 20's binary free of container-facing
   deps and independently restartable by the supervisor. Additive, no existing contract text
   violated; requesting ratification.
2. **Shim contract addition**: 00 defines only `FuzzDDLSignature`. This lane requires
   `FuzzSQLModeFromStatusVars` and `FuzzParseForE2E` (exact signatures/JSON in Interfaces) in the
   same build-tagged `ddlfuzz_export.go`. Without them the plumbing check (status-var walker on
   real bytes) and structured semantic comparison are impossible — the signature string is too
   lossy to diff against an info_schema delta.
3. **State-dir additions**: `state/e2e-stats.json`, `state/e2e-exec-rejects.jsonl`,
   `state/e2e-live-accepted.jsonl`, `state/e2e-queue/{pending,processing,done}/` are not in 00's
   state layout. All additive; supervisor (40) consumes stats and queue, 20's replay consumes
   live-accepted.
4. **meta.json extension**: e2e findings add keys (`class`, `info_schema_delta`,
   `binlog_event_hex`, `status_vars_hex`, `sql_mode_name`, `submitted_text`,
   `binlog_text_differs`, `server_image`) beyond 00's enumerated meta fields. Additive; the 00
   schema's required fields are all still populated (`oracle_digest` set to `null` for
   e2e-native findings — there is no oracle in this lane's loop).
5. **New finding classes**: 00 doesn't enumerate classes. This plan defines
   `e2e-statusvar-walk`, `e2e-sqlmode-mismatch`, `e2e-plumbing-sig`, `e2e-panic`,
   `e2e-query-rewrite`, `e2e-parse-error-live-accept`, `e2e-missed-column-effect`,
   `e2e-col-attr`, `e2e-position-missed`, and `oracle-reject-live-accept` (filed via 20's
   replay). Component 20's descriptor function must incorporate the class string so e2e findings
   dedup separately from fast-lane ones.
