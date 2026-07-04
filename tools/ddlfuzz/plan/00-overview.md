# ddlfuzz — differential fuzzing harness for the hand-rolled MySQL/MariaDB DDL parser

Master plan and **binding cross-component contracts**. Component plans (10–40) must conform to
everything in this file; deviations are contract-change proposals, recorded in the component doc's
"Contract issues" section, not silently applied.

Target under test: `flow/connectors/mysql/ddl_lexer.go` + `ddl_parser.go` (see
`docs/mysql-ddl-parser-writeup.md`, the authoritative description of the parser and its safety
property). Ground-truth servers: `~/Code/mysql-server` (tag mysql-9.7.0) and
`~/Code/mariadb-server` (main, 13.1.0-alpha).

## Correctness contract

A fuzz case is `(statement bytes, sqlMode uint64, engine ∈ {mysql, mariadb})` — post-binlog
statement text.

- **Server rejects** the input → unreachable (binlog invariant: binlogged statements executed
  successfully). Only requirement: `parseQueryEvent` returns within budget, no panic.
- **Server accepts** → our signature must **exactly equal** the oracle-derived signature. Any
  mismatch in any direction, and any panic/hang/OOM on any input, is a bug. **No severity tiers**:
  misparsing binlog DDL is treated as fatal in production; every divergence gets fixed, ledgered
  (with citation), or parked (escalation file). Nothing is "tolerable noise".

## Hard constraints

1. **Nothing outside `/Users/ilia/Code/peerdb` is ever created, modified, or deleted.** The server
   checkouts are read-only ground truth. All builds are out-of-source under
   `tools/ddlfuzz/build/` (gitignored).
2. **Servers are black boxes.** Allowed oracle outputs: (a) parse verdict + the statement-effect
   digest below; (b) standard SanitizerCoverage (`-fsanitize-coverage=inline-8bit-counters`)
   bitmaps. Forbidden: custom lexer instrumentation / token-boundary peeking; mining
   `mysql-test`/`mariadb-test` suites for corpus. Allowed exception: extracting the
   keyword/operator vocabulary from the servers' lexer symbol tables as a mutation dictionary —
   a bag of tokens, nothing more (no yacc production transcription).
3. Generator knowledge comes from **public documentation** (dev.mysql.com reference manual,
   MariaDB KB) and **our own tests** (~700 statements in `flow/connectors/mysql/*_test.go`:
   ≈230 tidb-diff corpus entries + ≈480 unit-table cases).
4. Resource envelope: one macOS host (darwin/arm64), 14 cores, Docker available. The final run is
   **72h unattended**; every component must be crash-restartable from on-disk state, and no
   component (including the fix agent) has authority to stop the run.

## Directory layout

```
tools/ddlfuzz/
  plan/                 # these docs
  oracle/
    proto/              # single-header C++ frame protocol + digest serialization (shared)
    mysql/              # MySQL oracle driver sources + build script
    mariadb/            # MariaDB oracle driver sources + build script
  build/                # gitignored: out-of-source server builds, oracle/fuzz/lane/super binaries
  cmd/ddlfuzz/          # Go driver: fuzz / golden / replay / minimize subcommands
  cmd/ddlfuzz-e2e/      # slow-lane binary (thin main; logic in e2e/)
  internal/...          # Go packages (generator, oracle client, compare, corpus, ...)
  e2e/                  # slow lane: compose file + fixture/matcher code
  supervisor/           # ddlsuper source + fix-agent prompt template
  seeds/                # committed seed corpus (seeds.jsonl + per-fix <sig>.sql/<sig>.meta.json)
  state/                # gitignored run state (layout below)
  go.mod                # module github.com/PeerDB-io/peerdb/tools/ddlfuzz
```

`go.mod` uses `replace github.com/PeerDB-io/peerdb/flow => ../../flow`. Keeping fuzz deps out of
`flow/` is deliberate (faster flow rebuilds).

## Contract: access to the parser under test

`parseQueryEvent` and the signature reduction are unexported in package `connmysql`. The flow
module gains one build-tagged file, `flow/connectors/mysql/ddlfuzz_export.go`:

```go
//go:build ddlfuzz

package connmysql

// FuzzDDLSignature parses and reduces to the comparison signature (grammar below).
// err is parseQueryEvent's error. Panics are NOT recovered here; the caller contains them.
func FuzzDDLSignature(query []byte, sqlMode uint64, isMariaDB bool) (string, error)
```

The fuzz binary builds with `-tags ddlfuzz`. The signature rendering inside is a port of
`ddlDiffSignature`/`ddlDiffSpecSig`/`ddlDiffColSig` from `ddl_parser_tidb_diff_test.go` (same
output grammar, see below). `QkindFromMysqlColumnType` is already exported for the oracle side.

## Contract: signature grammar

Identical to `ddl_parser_tidb_diff_test.go` (that file is normative):

```
statement list:  <stmt> [" | " <stmt>]...
alter:           "alter " [schema "."] table "{" <spec> ["; " <spec>]... "}"   (specs may be empty)
rename:          "rename " old ">" new ["," " " ...]                            (idents schema-qualified as written)
spec col:        "col " col [", " col]...
spec change:     "chg " oldname " " col
spec rename col: "ren " old ">" new
spec drop:       "drop " name
col:             name "=" (qkind | "ERR") ["(" prec "," scale ")" if qkind==numeric] [" nn"]
position:        " @pos" appended to a spec when FIRST/AFTER present
```

qkind = `QkindFromMysqlColumnType(typeStr, true, 0)`. The fuzzer builds the oracle-side signature
from the digest below through the same reduction, then **canonicalizes both sides before
comparing** (component 20 rule R13: flatten multi-column specs, `MODIFY c` ≡ `CHANGE c c`,
class-bucketed spec order, statement-level position flag, numeric-param default substitution) —
the servers' `Alter_info` provably loses spec grouping and inter-class order, so raw
signature-string equality would produce guaranteed false divergences. Component 20 owns all
reconciliation rules.

## Contract: oracle wire protocol (v1)

One oracle process = one engine, single-threaded, frames on stdin/stdout, logs on stderr.
All integers little-endian.

```
frame:      u32 body_len | body
body:       u8 msg_type | payload            (response echoes msg_type)

msg 1 PARSE_BATCH
  request:  u32 count; count × { u64 sql_mode; u32 stmt_len; u8 stmt[stmt_len] }
  response: u32 count; count × { u32 digest_len; u8 digest_json[digest_len] }
msg 2 GET_COVERAGE
  request:  (empty)
  response: u32 n; u8 counters[n]            (cumulative inline-8bit counters, never reset)
msg 3 HELLO
  request:  (empty)
  response: u32 len; u8 json[len]            {"engine":"mysql","server_version":"9.7.0","protocol":1}
```

Statement bytes are passed to the server parser verbatim (embedded NULs included). `sql_mode` is
installed on the THD before parsing. A crashed oracle is respawned by the client; the in-flight
batch is bisected to find the crashing input, which becomes a finding.

## Contract: digest JSON schema

```jsonc
{"verdict":"reject","error":"<server error, prefixed with errno if available>"}
{"verdict":"accept","stmts":[ ... ]}          // one entry per statement the server would execute
                                              // for this event text, in order
// stmt kinds:
{"kind":"other"}                              // any non-actionable statement (preserves order/count)
{"kind":"alter_table","schema":"","table":"t","specs":[ ... ]}   // specs may be empty
{"kind":"rename_table","pairs":[{"old_schema":"","old_table":"a","new_schema":"","new_table":"b"}]}
// spec ops (column-relevant only; index/constraint/option specs are omitted entirely):
{"op":"add","cols":[<col>...],"has_position":false}
{"op":"modify","cols":[<col>],"has_position":false}
{"op":"change","old_name":"a","cols":[<col>],"has_position":false}
{"op":"drop","old_name":"a"}
{"op":"rename_col","old_name":"a","new_name":"b"}
// col:
{"name":"c","type_str":"decimal(10,2) unsigned","not_null":true,"params_written":[10,2]}
```

`type_str`: information-schema style, lowercase canonical base name, params **as the server
recorded them at parse time** (both engines fill type-param defaults during parse — `int` ≡
`int(11)` on MariaDB, bare `DECIMAL` ≡ `decimal(10,0)` — so written-vs-defaulted is not
recoverable; the load-bearing `tinyint(1)` case IS exact on both engines), `" unsigned"` suffix
when unsigned/zerofill. It must round-trip through `QkindFromMysqlColumnType` (load-bearing
pieces: base before `(`, trailing `" unsigned"`, `tinyint(1)`). `params_written` carries those
same recorded numeric params, `null` where the engine records none; the written-vs-default
reconciliation is component 20's (Reconciliation decisions §D2). Schema/table/column names are
the decoded identifier text (quoting removed).

Sentinel current-db: the MariaDB parser rejects unqualified table names without a current db, so
the MariaDB oracle installs `peerdb_ddlfuzz_nodb` as session db and maps it back to `""` in the
digest (MySQL distinguishes written qualification via `is_fqtn`, no sentinel needed). Generators
and mutators MUST never emit the sentinel identifier.

Multi-statement note: servers execute one statement per binlog event, but the parser under test
continues after `;` following an actionable statement — and stops at the first benign head.
Comparison rule (owned by component 20): **truncate the digest's `stmts` at the first
`kind=="other"` entry (exclusive)** and compare our signature against the truncated reduction;
the case is skipped entirely (unreachable-by-invariant, no-panic check only) iff
`len(stmts) > 1 && stmts[0].kind == "other"`.

## Contract: state directory

```
state/
  corpus.db                              # retained inputs in SQLite (sql, engine, sql_mode, origin)
  coverage/{mysql,mariadb}.sancov        # OR-accumulated oracle bitmaps (sole retention signal)
                                         # (optional coverage/go.covdata/ only if -cover build used)
  findings/<sig>/repro.sql               # minimized input bytes
  findings/<sig>/meta.json               # {sig, engine, sql_mode, lane:"fast"|"e2e", our_sig,
                                         #  our_error, oracle_digest, status:"open"|"fixed"|
                                         #  "ledgered"|"parked", discovered_at, minimized}
  ledger.jsonl                           # accepted divergences; each entry REQUIRES a public-doc
                                         # or server-source citation and a justification
  parked.list                            # one <sig> per line; persists across runs; suppressed at
                                         # dedup layer, never re-picked by the fix agent
  escalations/<sig>.md                   # written when a signature is parked
  escalations/run-*.md                   # run-level (non-sig) escalations; prefix can't collide
  attempts/<sig>.jsonl                   # fix-attempt log (agent transcript refs, outcomes)
  attempts/<sig>.attempt<N>.*            # per-attempt stream.jsonl / last.txt / diff (40)
  report.md                              # heartbeat + final report
  stats.json                             # fast-lane heartbeat, 30s cadence (20 writes, 40 reads)
  e2e-stats.json                         # e2e heartbeat, 10s cadence (30 writes, 40 reads)
  e2e-queue/{pending,processing,done}/   # post-fix confirmation handoff (40 → 30, rename claims)
  e2e-exec-rejects.jsonl                 # generator-refinement feed (30 writes)
  e2e-live-accepted.jsonl                # oracle cross-check feed (30 writes, 40 rotates+replays)
  findings/index.jsonl                   # append-only finding log for cheap tailing (20 writes)
  coverage/history/edges.csv             # hourly edge counts for the plateau assessment (40)
  seed, inflight.jsonl                   # fuzzer PRNG seed + in-flight batch journal (20)
  groups.json, last_good_commit, untracked.baseline, BLOCKED, spend.json,
  supervisor.{log,pid}                   # supervisor state (40)
  merge/{request,ack,claimed,result}.json # merge-staged single-slot handoff (43)
  merges.jsonl, merge-inflight.json       # accepted merge log + crash-recovery journal (43)
  run-start, selfrestart.json, RESTART_REQUIRED # run deadline + supervisor handoff markers (43)
```

Oracle build manifests live beside binaries as `build/oracle-*.manifest.json` and record the
source hash used to validate staged oracle handoff (43).

`<sig>` = first 12 hex chars of sha256 over the canonical divergence descriptor (component 20
defines the descriptor; requirement: stable under re-discovery, insensitive to incidental
identifier spellings).

## Components

| Doc | Component | Provides | Consumes |
|---|---|---|---|
| `10-oracle-mysql.md` | MySQL 9.7 in-process parse oracle | oracle binary (protocol v1, digest, SanCov) | server checkout (RO) |
| `11-oracle-mariadb.md` | MariaDB 13.1 in-process parse oracle | same | server checkout (RO) |
| `20-skeleton-first.md` | Landing commit for 20 (do first) | Go module + real export shim + frozen interface stubs so 30/40 can compile | flow module |
| `21-fuzzer.md` | Go differential driver | `ddlfuzz` binary: fuzz/golden/replay/minimize; generator, mutators, oracle-SanCov corpus, comparison + reconciliation, dedup, findings; oracle process pool | oracle binaries, export shim, state dir |
| `30-e2e-lane.md` | Live-binlog slow lane | e2e findings (lane:"e2e"), plumbing + semantic validation | containers, flow module, state dir |
| `40-supervisor.md` | 72h unattended run loop | supervisor process, fix-agent (headless `codex exec`) cycle, gate, parking/escalation, restarts, report | everything above |
| `43-merge-staged.md` | Safe staged merge coordination | `ddlsuper merge-staged`, merge slot protocol, staged oracle handoff, supervisor self-restart | supervisor, git worktrees, oracle manifests |

## Cross-component requirements

- **Golden validation before fuzzing** (owned by 20, blocks the run): replay all existing test
  cases (~700 statements: ≈230 tidb-diff corpus + ≈480 unit cases) through both oracles; every
  mismatch is reconciled (oracle harness fix or documented expectation fix) before any fuzzing
  throughput counts.
- **Oracle changes always re-run golden validation** (enforced by 40's gate).
- **Preflight on every fuzzer restart** (40): replay all `findings/*/repro.sql`; `fixed` must
  pass, `ledgered`/`parked` may diverge, anything else blocks the restart.
- **Core budget** during the 72h run: ~10 oracle processes (5 per engine), 1-2 Go driver +
  generation, ~2 e2e containers + workers + matcher, remainder for the fix agent when active.
- **Charset scope**: byte-safe client charsets only (utf8/utf8mb4/latin1/ascii). Generators and
  mutators must not emit bytes ≥ 0x80 except as whole valid UTF-8 sequences. Non-byte-safe
  transcoding is explicitly out of scope (writeup future-work item 1).
- **Verification gate** (used by 40, defined once): from `flow/`:
  `go build ./... && go vet ./connectors/mysql/ && go test ./connectors/mysql/ -run
  'TestDDL|TestProcessRenameTableQueryMetric|TestClassifyOnlineSchemaMigrationTool' -count=1 &&
  golangci-lint run ./connectors/mysql/...` plus `go build -tags ddlfuzz ./...` (shim compiles)
  and `go test ./...` in `tools/ddlfuzz`. Never run the whole connmysql test package without a
  live DB (non-DDL tests need one).
- Commits: on branch `parser-wip`, no Claude co-author trailer.

## Reconciliation decisions (authoritative; supersede conflicting text in component docs)

Resolved after all five component plans were written. Where a component doc's "Contract issues"
section conflicts with this list, this list wins.

- **D1 — Spec canonicalization (10-C1, 11-issue-2 → 20 rule R13).** Both servers flatten
  `ADD (a,b)` to per-column entries, cannot distinguish `MODIFY c` from `CHANGE c c`, and lose
  inter-class spec order. Component 20 canonicalizes BOTH signatures before comparing: flatten
  multi-column `col`/`chg` specs to per-column specs; rewrite `chg X` whose new column name is `X`
  into `col` form; bucket specs per statement in class order [col/chg (source order), ren, drop];
  lift `@pos` to a statement-level flag (consumer semantics: `hasPositionShiftingDdlChanges` is an
  OR). The oracles' own emission orders (10: adds,drops,renames; 11: adds,renames,drops) need NOT
  match — bucketing normalizes both.
- **D2 — Numeric params (11-issue-1, 20-issue-3).** `params_written` is server-recorded (defaults
  filled at parse); "as written" is unachievable on both engines. 20's R2: during canonicalization
  our side substitutes the engine default for `-1` (decimal precision 10, scale 0); the oracle
  side renders `params_written` verbatim. `tinyint(1)` remains exact on both engines.
- **D3 — Multi-statement rule (20-issue-1).** Truncate-at-first-`other` is the general rule
  (contract text above updated); 30/40 reuse 20's reducer.
- **D4 — Finding-class vocabulary.** Union of 20's fast-lane set {`sig_mismatch`, `we_error`,
  `panic`, `timeout`, `oracle_crash`, `oracle_timeout`} and 30's e2e set (`e2e-statusvar-walk`,
  `e2e-sqlmode-mismatch`, `e2e-plumbing-sig`, `e2e-panic`, `e2e-query-rewrite`,
  `e2e-parse-error-live-accept`, `e2e-missed-column-effect`, `e2e-col-attr`,
  `e2e-position-missed`) plus `oracle-reject-live-accept`. Closed set; 20's descriptor `class`
  field accepts exactly these.
- **D5 — `replay` exit codes (40-C5 vs 20).** 0 = reconciled/fixed; 10 = still diverges (the JSON
  carries `class`, including `panic`/`timeout`); 11 = finding malformed/unreadable or oracle
  won't start; 1 = internal error. Panics/hangs are divergences (10), not 11. JSON stdout shape
  is 20's.
- **D6 — e2e handoff paths (30 vs 40).** 30's design wins: queue at
  `state/e2e-queue/{pending,processing,done}/` with rename-claim lifecycle and 30's payload/result
  schemas; stats at `state/e2e-stats.json` (10s, 30's per-engine schema); supervisor reads queue
  depth by listing `pending/`. Lane binary built from `cmd/ddlfuzz-e2e/` to `build/ddlfuzz-e2e`.
- **D7 — Oracle cross-check.** 20 gains `replay --from <jsonl> --expect-accept` (oracle reject of
  a live-accepted statement → finding `oracle-reject-live-accept`); 40 runs it ~6-hourly after
  atomically rotating `e2e-live-accepted.jsonl`.
- **D8 — Seeds format (40-C7 vs 20).** `tools/ddlfuzz/seeds/` holds BOTH the generated bulk
  `seeds.jsonl` and per-fix `<sig>.sql` + `<sig>.meta.json` pairs; 20's loader reads both.
- **D9 — Sentinel db.** Contract text above; `peerdb_ddlfuzz_nodb` excluded from all generators,
  mutator dictionaries, and fixture vocabularies.
- **D10 — Fix agent is headless `codex exec`** (user direction; 40 has the verified invocation).
- **D11 — Test counts.** The seed pool is ~700 statements (≈230 diff-corpus + ≈480 unit rows);
  all count-dependent thresholds updated in 20.
- **D12 — `stats.json`.** 20 emits 40's C6 core fields (`ts` RFC3339, `execs_total`,
  `execs_per_sec`, `corpus_count`, `edges`, `oracle_restarts`, `findings_emitted_total`) plus
  additive extras (`run_seed`, `class_counts`, `suppressed`); 40 must tolerate unknown fields.

## Exit

The run ends at the 72h clock (or on operator signal) — not on a quality bar. There is no
pass/fail gate. At the end the supervisor writes a final report with these **metrics** (all
informational, for the human deciding whether the parser is done or the campaign should be
re-run — none blocks anything):

- SanCov edge count on both oracles and its trajectory (plateaued = no new edges in the final 12h,
  a signal that exploration is saturating); public-doc construct-checklist coverage (20).
- Findings tally: open / fixed / ledgered / parked, with each parked signature's escalation file
  linked.
- Fixes committed with regression tests (the durable artifacts); optional shutdown-distilled
  corpus; confirmation that the rig re-runs end to end from `tools/ddlfuzz/`.

A clean campaign is one that ends with open≈0 and edges plateaued, but a run that ends with open
findings still queued or coverage still climbing is a *successful run that found more work*, not a
failure — record it and decide whether to extend or fix-and-rerun.
