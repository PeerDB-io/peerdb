# 50 — Make e2e findings reproducible by the fix agent (offline, in-memory)

Conforms to `00-overview.md`. Problem: the codex fix loop parks ~all e2e-lane findings as
`did-not-reproduce`, because (1) `ddlfuzz replay <sig>` is **parse-only/fast-lane** and structurally
cannot reproduce an our-parser-vs-live-server divergence, and (2) most e2e findings on disk are
**under-recorded** (`submitted_text` and `info_schema_delta` are `null` for several classes), so there
is nothing to reproduce from. This component makes e2e findings self-contained and teaches `replay`
to reproduce them **offline from the recorded meta — no containers, no live DB, no oracle**, reusing
the exact comparison logic the live lane already runs.

Scope: `tools/ddlfuzz/` + `supervisor/prompt.tmpl` only. **No parser changes. No server/oracle
builds. No live containers.** Validate with `go test` and synthetic findings.

## Background (verified in this tree)

- `internal/replay/replay.go:replayOne` reads only `engine`+`sql_mode`, runs the fast-lane
  (`exec` worker + `oracle.SingleDigest` + `compare.Diff`), and is **lane-blind**. e2e findings
  reconcile there → exit 0 → prompt step (a) → `did-not-reproduce` → parked after 3 tries.
- The e2e comparison logic is pure and DB-free-capable, in `e2e/checks.go`:
  `safeSQLModeFromStatusVars` (wraps `connmysql.FuzzSQLModeFromStatusVars`), `FuzzParseForE2E`,
  `applyPredicted(before snapshot, parsed) predicted`, `compareSemantics(exp, parsed, actual delta)`,
  `compareColumnAttrs`, plus the plumbing checks. These consume recorded data, not a live conn —
  BUT `compareSemantics` reads `exp.Before`/`exp.After` snapshots for reconciliations (e.g.
  `column_key=='PRI'` implied NOT NULL), which are **not** in finding meta today.
- `e2e/checks.go:recordE2EFinding` / `fallbackRecordFinding` write meta inconsistently: the semantic
  path stores `info_schema_delta`+`status_vars_hex`; the plumbing/rename/`missed-column` paths leave
  `submitted_text`/`info_schema_delta` `null`. Confirmed on live findings (only `e2e-position-missed`
  carried full context; `e2e-col-attr`/`e2e-missed-column-effect`/`e2e-plumbing-sig`/
  `e2e-query-rewrite` did not).
- `internal/replay` reaches `FuzzParseForE2E`/`FuzzSQLModeFromStatusVars` through the flow shim.
  The `go build ./...` gate MUST stay green.

## Deliverables

### 1. Uniform, complete e2e finding capture (`e2e/checks.go`)

Every e2e finding — through **all** paths (semantic applier, plumbing checks, rename, panic) — must
persist a complete, self-describing reproduction record in `findings/<sig>/meta.json`. Required keys
(additive; `00` §meta already tolerates extras):

```jsonc
{
  "lane": "e2e",
  "class": "<one of the e2e-* classes>",
  "engine": "mysql|mariadb",
  "sql_mode": <u64>,                 // the mode used for the check (from status vars)
  "sql_mode_name": "ANSI_QUOTES,...",
  "submitted_text": "<the DDL the worker submitted>",   // NEVER null for a reproducible finding
  "binlog_query": "<qe.Query bytes as string>",         // the executed/binlogged form
  "binlog_text_differs": <bool>,
  "status_vars_hex": "<hex of qe.StatusVars>",          // for plumbing/sqlmode repro
  "info_schema_delta": { "added":[...], "dropped":[...], "changed":[...] },
  "before_snapshot": { "<col>": <colRow>, ... },        // NEW — needed by compareSemantics reconciliations
  "after_snapshot":  { "<col>": <colRow>, ... },        // NEW — column_key/type/ordinal ground truth
  "our_sig": "...", "our_error": "...",
  "server_image": "mariadb:13.0.1-rc",
  "status": "open", "discovered_at": "...", "minimized": false
}
```

- `colRow` = the `snapshot.go` row struct serialized (name, ordinal, column_type, is_nullable,
  column_key, numeric_precision/scale). Serialize `sql.NullInt64` as a plain nullable int.
- Unify the two writers so a single helper populates the full record from a `findingInput` carrying
  `Submitted`, `SQLMode`, `SQLModeName`, `BinlogQuery`, `StatusVars`, `Before`, `After`, `Delta`.
  No finding path may leave `submitted_text`/`before_snapshot`/`after_snapshot` empty. Plumbing-only
  classes (`e2e-plumbing-sig`, `e2e-sqlmode-mismatch`, `e2e-statusvar-walk`) still store
  `submitted_text`+`binlog_query`+`status_vars_hex` (delta/snapshots may be empty for those).
- Keep the descriptor/`<sig>` computation unchanged (identity is class+shape+engine+mode).

### 2. DB-free reproduction core (`internal/e2echeck`, new package)

Extract the pure comparison logic so both the live lane and `replay` share one implementation.

```go
package e2echeck

// Input is reconstructed from finding meta (§1) — no live DB.
type Input struct {
    Engine        string
    IsMariaDB     bool
    SQLMode       uint64
    Submitted     string
    BinlogQuery   string
    StatusVarsHex string
    Before, After Snapshot          // decoded from before_snapshot/after_snapshot
    Delta         Delta             // decoded from info_schema_delta
    Class         string            // the recorded class (guides which check to re-run)
}

type Result struct {
    Reconciled bool
    Class      string // e2e-* class if diverges, else ""
    Shape      string
    Detail     string
}

// Reproduce re-runs the same check the live matcher ran, entirely in memory.
func Reproduce(in Input) (Result, error)
```

- `Reproduce` dispatches by `Class`:
  - plumbing (`e2e-statusvar-walk`/`e2e-sqlmode-mismatch`/`e2e-plumbing-sig`): decode
    `StatusVarsHex` → `FuzzSQLModeFromStatusVars`; compare `FuzzDDLSignature(BinlogQuery, mode)` vs
    `FuzzDDLSignature(Submitted, SQLMode)` — exactly the live plumbing check.
  - `e2e-query-rewrite`: `binlog_text_differs` re-derived from `BinlogQuery != Submitted`.
  - semantic (`e2e-missed-column-effect`/`e2e-col-attr`/`e2e-position-missed`): `FuzzParseForE2E`
    → `applyPredicted(Before, parsed)` → `compareSemantics(Before/After, parsed, Delta)` — the moved
    functions. All reconciliations (PRI-implied NOT NULL via `After` column_key, MariaDB json alias,
    decimal default, AFTER-last no-shift) must be preserved **verbatim** — move the code, don't
    reimplement.
  - alignment anomalies (`e2e-missing-event`/`e2e-unexpected-event`/`e2e-exec-reject-applied`,
    30 §4): not offline-reproducible — they assert the presence/absence of a live binlog event;
    `Reproduce` rejects them (unsupported class), so disposition is by ledger/skip-list, not
    replay.
- **Move** `applyPredicted`, `compareSemantics`, `compareColumnAttrs`, `compareSetMap`,
  `compareBoolSet`, snapshot/delta types into `internal/e2echeck`; `e2e/checks.go` calls them from
  there so the live lane and offline repro are provably identical.
### 3. Lane-aware `replay` (`internal/replay/replay.go`)

- In `replayOne`, read `meta["lane"]`. If `"e2e"`: build `e2echeck.Input` from meta and call
  `e2echeck.Reproduce`; map to the existing `Result`/exit-code contract:
  `Reconciled → ExitOK(0)`; diverges → set `Class`/`Shape`, `ExitDiverged(10)`; missing/undecodable
  required meta → `ExitMalformed(11)`. **Do not** touch the fast-lane path for non-e2e findings.
- Stdout JSON keeps 20's `replay <sig>` shape (`oracle_digest` may be null for e2e).
- No new CLI flags. `40`'s fixloop invocation is unchanged.

### 4. Fix-agent prompt (`supervisor/prompt.tmpl`)

Add an e2e branch to the root-cause tree (step b). Keep step (a) `replay {SIG}` as-is — it now
reproduces e2e findings too (except the alignment-anomaly classes above, which replay cannot
decide). e2e disposition buckets:
- **Parser bug** (`e2e-missed-column-effect`, `e2e-col-attr`, `e2e-position-missed`): our parser's
  extracted effect ≠ the server's executed effect (from `info_schema_delta`/snapshots in meta). Fix
  in `flow/connectors/mysql/`, add regression test, seed. (The `missed-column-effect` "benign but
  schema changed" case is the highest-severity — never make an actionable statement silently benign.)
- **Harness bug** (`e2e-plumbing-sig`, `e2e-sqlmode-mismatch`, `e2e-statusvar-walk`,
  `e2e-query-rewrite`): status-var walker / signature-plumbing issue → fix under `tools/ddlfuzz/`
  (shim or e2e); do NOT edit the parser for these unless the walker feeds a genuinely wrong mode
  that a parser change must handle.
- **Server skew / genuine divergence**: live `mariadb:13.0.1-rc` differs from the `13.1` behavior our
  side models → **ledger with citation** (MariaDB KB / server-source), per `30` §skew. Reproduced
  offline findings that are pure skew resolve via the ledger, not a code change.
- Verification for e2e buckets: `ddlfuzz replay {SIG}` must exit 0 after a parser/harness fix; for a
  ledgered skew it may stay 10 (the ledger entry is the resolution). No live containers required in
  the attempt.

### 5. Tests

- `internal/e2echeck`: table-driven `Reproduce` over synthetic `Input`s — one per class, covering a
  known-divergence and a reconciled case (PRI NOT NULL, maria json alias, decimal default, AFTER-last
  no-shift, status-var mode round-trip, query-rewrite).
- `internal/replay`: a synthetic `findings/<sig>/` with full e2e meta (§1) → `replay <sig>` returns
  10 with the right class, and a reconciled fixture returns 0. (Write meta to a temp state dir.)
- `e2e`: keep existing tests green (the moved functions are re-exported/called from `e2echeck`).
- Acceptance (from `tools/ddlfuzz/`): `go build ./... && go vet ./... && go test ./...`
  green; `go build -o build/ddlfuzz ./cmd/ddlfuzz` and `-o build/ddlfuzz-e2e ./cmd/ddlfuzz-e2e`
  build.

## Notes / non-goals

- **Old findings can't be retro-reproduced**: findings filed before §1 lack `before/after_snapshot`
  (and some lack `submitted_text`). Offline repro works for findings that already have the needed
  meta (e.g. the `e2e-position-missed` we observed) and for all **future** findings after §1 ships.
  Re-queuing (unpark) is done by the coordinator after merge; dataless old findings will re-park
  until the running e2e lane re-discovers them with complete meta — that's expected.
- No attempt to reproduce the *live server's execution* beyond replaying the recorded delta; this
  deliberately does not detect 13.0-rc-vs-13.1 skew that only shows against the current live server
  (the live e2e lane remains the ground-truth backstop). See discussion in the campaign notes.
- Do not run `ddlsuper`, the live e2e lane, oracle builds, or golden in this task.
