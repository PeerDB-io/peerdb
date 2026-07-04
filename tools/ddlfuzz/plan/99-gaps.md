# 99 — implementation audit: remaining gaps

Known gaps in the implemented rig + the parser under test against plans 00–52, at `23fad9d7`
(2026-07-04, `ddlfuzz-staged`). Structure, state layout, wire protocol, exit codes,
canonicalization rules, and the gate are faithful to plan; the items below are what diverges.
`P` = parser, `R` = result integrity, `S` = supervisor, `G` = plan gaps; the low-priority tail
is at the end. Item IDs are stable across audits, so the numbering is not contiguous.

## P — parser under test (production impact)

- **P1 (MED).** `ddl_lexer.go:39-40` `ddlWordIs` uses `strings.EqualFold` (Unicode, not ASCII);
  `strings.ToUpper` keyword switches at `ddl_parser.go:469,532,722` likewise. `DROP ıNDEX`
  (column named `ıNDEX`, U+0131 folds to I) is skipped as a benign DROP INDEX → dropped column
  with no error; `ADD ſpatial INT` (U+017F→S) same. Both reproduced. Fix: byte-wise ASCII
  compare, reject ≥0x80 before keyword match.
- **P2 (MED).** `ddl_parser.go:594-612` — the RENAME COLUMN branch returns without consuming a
  trailing no-comma partition clause: `ALTER TABLE t RENAME COLUMN a TO b REMOVE PARTITIONING`
  (valid MySQL, oracle-verified) errors, and every spec in the statement is lost. Loud, but the
  asymmetry is arbitrary: after ADD/DROP specs the clause is consumed by remainder skipping, and
  the table-RENAME spec has a dedicated tail consumer (`consumeAlterTableRenameTail`,
  `ddl_parser.go:631-642`) — only RENAME COLUMN-final is affected.
- **P3 (LOW).** `ddl_lexer.go:121-126,330-351` — dollar quoting is keyed on engine only (the
  lexer has no server-version input), so it applies to all MySQL versions; on 8.x sources `$x$`
  is a legal identifier → unterminated-string error (loud) or, with a second `$x$`, wrong
  tokenization without an error.
- **P4 (LOW).** `ddl_lexer.go:78-80,100-101` — `--` comment detection accepts ASCII
  space/control bytes only; latin1 NBSP (0xA0) after `--` starts a comment server-side but not
  here. Reproduced: yields a spurious `@pos` (`ADD c INT --<NBSP> FIRST` parses the
  commented-out `FIRST`).
- **P5 (LOW).** `ddl_parser.go:533,556,656,417-449` — `DROP PARTITION p0, add` places partition
  names at spec-head; names colliding with spec keywords error. Always loud for binloggable
  statements, never silent.
- Verified clean: no panic/hang paths (all loops advance, lookahead bounded); lexer semantics per
  writeup; attribute region rules; identifier case preserved end to end (parser byte-exact,
  oracles forced to `lower_case_table_names=0`, case anchors in golden); shim reductions ≡
  tidb-diff reductions, `fuzzDDLSigIdent` ≡ `compare.sigIdent`.

## R — result integrity (findings/verdicts can be wrong)

- **R5 (MED).** `compare.go:513,524` embed counts/kind-lists in `stmt_count(...)`/
  `spec_count(...)` shapes (plan says bare dimension names; `col_kind`/`col_params` at :549,:552
  embed values likewise) → one bug fans out to many sigs and can consume the 200
  `max-open-findings` cap (`internal/fuzzcmd/main.go:60`), after which genuinely new findings
  are suppressed. The cap check also runs before `findings.Record`
  (`internal/fuzzcmd/fuzzloop.go:801-807`), so at cap even `times_seen` stops updating.
- **R6 (MED).** Live/offline check duplication: `safeSQLModeFromStatusVars`/`safeDDLSignature`/
  `safeParseForE2E` exist in both `e2e/checks.go:251-287` and
  `internal/e2echeck/reproduce_ddlfuzz.go:160-197`, with divergent panic handling — plan 50 §2
  says move, don't reimplement, so the copies can drift apart. Mode selection is shared
  (`ExpectedEventSQLModeRelevant`, `internal/e2echeck/sqlmode.go`, keyed by the
  `expected_relevant` meta field), but findings whose meta lacks that field fall back to the
  full `sql_mode` (`reproduce_ddlfuzz.go:59-62`) — for plumbing-sig that is the actual binlog
  session mode, not the expectation — and `reproducePanic` uses the unmasked mode
  (`reproduce_ddlfuzz.go:106`, fallback :100-102) → those cases can reconcile differently live
  vs offline.
- **R7 (MED).** `e2e/queue.go:101-106` — pending items for engines this process doesn't run are
  claimed and completed as `exec-reject` ("unknown engine") instead of left for the right lane.

## S — supervisor hazards (data loss / stall / lifecycle)

- **S4 (MED).** `validateReplayResolution` (`fixloop.go:1168-1196`): `res, err := RunReplay`
  (:1169) is shadowed by `meta, err := loadFindingMeta` (:1180) inside the `ledgered` branch, so
  the guard at :1190 checks the wrong (nil) error and the replay exit is never validated for
  `ledgered` outcomes — an agent can ledger a finding whose replay exits 0 (should have been
  `fixed`) or errors. The meta-status and ledger-citation checks do run; the `fixed` branch
  (:1171) validates correctly.
- **S5 (MED).** Torn last line in `attempts/<sig>.jsonl` (crash mid-append): `SelectFinding`
  discards the load error (`fixloop.go:584`) → selects the sig as 0-attempts; `FixOnce` errors
  on the same load (:771-774) before doing anything → repeated selection of one sig with no
  park and no progress on others. Preflight's drift classifier surfaces unreadable attempt
  records (`preflight.go:293-295`), but only at startup and only when HEAD has drifted.
- **S6 (MED).** In-run git-drift handling never attempts cleanup: `ensureAttemptGitReady`
  failure writes `run-git-drift.md` and returns (`fixloop.go:797-800`), retried and the file
  rewritten every 30s tick. Preflight classifies drift via attempt records
  (`preflight.go:244-319`: failed-attempt residue is reset, human commits kept, ambiguity
  blocks), but a dirty tracked tree blocks the repo-state step (`preflight.go:94-96`) before the
  classifier runs — so a crash-dirty tree needs a human even when a reset to
  `last_good_commit` would heal it.
- **S7 (MED).** `fix-once` (`main.go:107-132`) takes no supervisor lock → concurrent with `run`
  it races git reset/commit/`last_good_commit`/`spend.json` (`AddSpend` is unlocked
  read-modify-write, `fixloop.go:1490-1499`). The merge CLI is lock-disciplined
  (`mergecli.go:44,311-322`), which makes `fix-once` the only unserialized git/state writer — it
  can also race an inline merge.

## G — plan divergences (scope/contract, not crashes)

- **G1.** Generator residuals vs plan 21 (the grammar-table generator, `internal/gen/`,
  otherwise covers the construct list): the 86-construct checklist (`gen/checklist.go`) is
  coarser than plan 21's ~150 enumerated constructs (`plan/21-fuzzer.md:1098` — e.g. partition
  management ops, `ON UPDATE CURRENT_TIMESTAMP`, `ZEROFILL`, `INT1/2/3`, NATIONAL CHAR,
  `UNION=(...)`) and coverage is not reported at runtime (no consumer of `gen.Checklist`; the
  stats writer `internal/fuzzcmd/fuzzloop.go:833-868` has no checklist metric), so 00 §Exit's
  construct-checklist metric is unreportable; benign heads are a subset of plan 21's list
  (`gen/tables.go:270-279` — no UPDATE/DELETE/GRANT/XA/TRUNCATE/FLUSH). Byte-safety +
  sentinel-db constraints are respected and fuzz-tested.
- **G2.** Digest schema grew alter-side `new_schema`/`new_table` + `OracleSig` rename-split
  (`digest.go:17-18`, `compare.go:78-97`) — referenced by plan 12:180 but not folded into 00's
  digest contract (`plan/00-overview.md:140`) or 21's R9.
- **G7.** e2e decimal unwritten-param reconciliation is skipped rather than default-checked
  (`e2echeck/compare.go:229,235` gate on `>=0`); `internal/compare` defaults them inline
  (`compare.go:487-494`) — no shared helper.
- **G8.** `fallbackRecordFinding` (`e2e/checks.go:391-426`) writes contract-shaped findings dirs
  with ad-hoc sigs (sha256 over `OurSig`, identifier-sensitive, no dedup vs parked/ledger) on
  Record failure (invoked at :360).

## Low priority

Low likelihood or low blast radius; none with verdict-integrity impact.

- **R8.** `e2e/checks.go:237-246,358-367` — if `findings.Record` and the fallback both fail
  (e.g. disk full), `findingCount==0` → a diverging replay completes `confirmed-fixed`.
- **R9.** `e2e/rewrite.go:65-76` — old→new column map iteration can double-substitute when an
  old column set contains a fixture target name (`x→c_int` before/after `c_int→c_tiny` is
  order-dependent); word-boundary replace (:312-334) also matches inside string
  literals/comments. The shape check (:39,:88, names off) catches neither. Corrupts
  intent/dedup, not verdicts.
- **R10.** `compare.go:836-841` `sigIdent` doesn't quote an ident literally named `@pos`;
  `parseSpec` (:1026-1029) strips the suffix → round-trip corrupts → false `parse_our_sig`
  mismatch.
- **R11.** `internal/digest/digest.go:67-77` `Reset` doesn't clear per-statement
  `Kind/Schema/Table` (inert while oracle JSON always sets them); pooled reuse could carry
  stale fields into a digest → fabricated divergence. Near-unreachable today only because
  `Release` is called solely on the decode-error path (:61; the pool is also ineffective for
  the same reason).
- **S8.** Shutdown mid-attempt rolls back safely (background context with a 2-minute cap,
  `fixloop.go:1113-1117`; outcome recorded as `aborted`), but the `aborted` record counts
  toward the 3-attempt budget (`BudgetExhausted` counts all records, `fixloop.go:352-357`) — a
  shutdown burns a slot — and codex is SIGKILLed on cancel with no SIGTERM grace
  (`fixloop.go:1025,1029`, `proc.go:111-119`).
- **S9.** Restart backoff index never resets on healthy uptime (`nextBackoff`,
  `main.go:622-632`) and hot-restarts count toward the crash-loop breaker (6 exits within a
  30-minute window, `main.go:298-305`); after 5 exits every restart, including post-fix
  hot-restarts, waits 300s. `degraded` is never cleared once set (:305,:418).
- **S10.** `status.go:409-429` `lastAgentMessage` returns "" for transcripts >256KiB
  (`tailFile` cuts mid-line, :469-488; `ParseCodexJSONL` aborts on the first bad line,
  `fixloop.go:378-380`); the same abort loses token accounting for killed attempts
  (`fixloop.go:840-860` — `AddSpend` undercounts).
- **S11.** Lock acquisition is retried for 15s (`main.go:143,217-229`), but final failure
  still writes `state/BLOCKED` (`main.go:144-147`) → status/watch report BLOCKED for a healthy
  running campaign until a later preflight clears it (`preflight.go:74`).
- **S12.** `park.go:108-136` — the flap trigger's `!previouslyKnown` half is consumed on
  first sighting even when the single replay flakes (exit 11/1/timeout; `replayReproduces`
  requires exactly exit 10, :150-153); only `reopened_count>0` can re-trip it.
- **S13.** A budget-exhausted sig whose process dies between the 3rd attempt record and
  `parkBudgetExhausted` (reachable only from inside `FixOnce`, `fixloop.go:786,880,994`) stays
  `open`, is silently skipped by selection (:584-587), and gets no escalation file.
- **S15.** Commit hygiene is prompt-only (`prompt.tmpl:100`): no trailer validation
  (plan 40 step e). The rollback deletion plan's `Kept` files are dropped (`fixloop.go:1109`;
  the merge service's wrapper drops them too, `mergesvc.go:59-62`) instead of being noted in the
  attempt record.
- **D6.** e2e stale-`processing/` recovery runs only at startup (`e2e/queue.go:60-86`, called
  once at :91); items orphaned mid-run (worker exit at the `--cases` target, `worker.go:93-96`;
  the matcher panic path files `e2e-panic` but never calls `completeQueueItem`,
  `matcher.go:418-440`) wait for the next process restart.
- **G4.** `Case.Seed` is drawn after generation (`internal/fuzzcmd/fuzzloop.go:406,416`) — not
  PCG state — and there is no `replay --seed` (`internal/fuzzcmd/main.go:136-140`), so
  meta.json `seed` can't regenerate a case; repro.sql is the reproduction contract and makes it
  mostly moot. A run-level seed is persisted (`-seed`, `main.go:83,252-271`; `run_seed` in
  stats) but doesn't enable per-case regeneration.
- **G9.** Oracle stderr logs unbounded (`oracle/oracle.go:57` O_APPEND, no 8MiB rotation; also
  the `-single` and `-replayall` logs, `oracle.go:130`, `replay.go:211`).
- **G10.** The wire client trusts oracle-supplied count/length before validating: `roundTrip`
  allocates up to 4GiB from an unvalidated frame length (`wire/wire.go:131-138`) and
  `ParseBatch` pre-allocates from the unvalidated count (:56-58, checked only afterward at
  :71-73) — a corrupted frame becomes a huge-alloc panic instead of treat-as-crash; harmless
  for a local same-repo child process, with no verdict corruption. `internal/run` holds only
  the Case type + enums while orchestration lives in `internal/fuzzcmd/fuzzloop.go` (contra
  layout intent).
- `e2e/fixture.go:217-229` `fallbackFreshName` latent infinite loop (palette name and the single
  deterministic alternate both taken → recomputes the same name).
- `e2e/runner.go:104-115,164` + `stats.go:219`: ticker and final `stats.Write` race the same
  fixed `.tmp` path; `smoke.sh:7` shares the live campaign's state dir, and `compose.yml`'s
  fixed host ports collide with a live campaign (running it concurrently drops the `fuzz_w<k>`
  databases).
- `e2e/health.sh:5-11` awk false-passes on compose <2.21 single-line JSON output.
- Stats drift: `execs_per_sec` in stats.json is a since-start average
  (`internal/fuzzcmd/fuzzloop.go:825-831,840`; only the stderr line is an interval rate, :279);
  `suppressed` conflates cap-suppressed with ledger/parked re-seen (:803,:820,:857).
- `findings.Record` re-reads `parked.list` + full `ledger.jsonl` and rewrites meta on every
  occurrence (`findings.go:75,197-261,89-149`) under `recordMu`
  (`internal/fuzzcmd/fuzzloop.go:799-800`) — serializes oracle-proc completion on hot sigs;
  plan said in-memory + 60s/SIGHUP re-read.
- `golden` spawns a fresh oracle process per seed×engine (`golden.go:140` →
  `oracle.SingleDigest`, `oracle.go:128-133`; parallel worker pool) and aborts the whole run on
  the first oracle error (:89-93). (Plan 42 defers batching; the abort is the defect.)
- `sancov.Merge` (`sancov/sancov.go:47-49`) builds `[]uint64` over an 8-byte-misaligned base
  (the counters slice starts at offset +5 into the read buffer, `wire.go:77-91`) — works on
  arm64/amd64, technically invalid unsafe.
- `replay <sig>` arg joined into paths unvalidated (`internal/fuzzcmd/main.go:191-194`,
  `replay.go:508-510`) — `../` escapes the findings dir (local CLI, minor).

## Verified clean (audited, no defect)

Wire framing + crash-respawn/desync handling; SQLite corpus store (parameterized SQL, identity
hash, budget); corpus and fix-seed loading into mutation bases and golden; sancov
OR-accumulation logic; D1–D5/D7 rules and the Diff decision table 1–8; descriptor construction
(sorted keys, mode mask, sha256-12); D4 closed class set enforced; state-dir layout and atomic
temp+rename writes; `DeleteNewUntracked` path policy given a real `before` set (`..`, abs,
`.git/`, out-of-root, state/build/worktrees/staged exclusions); attempt lifecycle ordering
(rebuild before `last_good_commit` advances; rollback on a background context; `confirmFixed`
gated on `replayValidatesFinding`); diff-boundary enforcement (test append-only, seeds
add-only, flow/ scope, flow+compare cross-edit rejection); preflight HEAD-drift classification;
merge-staged slot protocol (exclusivity, claim-by-rename, inflight journal + recovery);
verification gate ≡ plan + plan-51 step 7; startup preflight steps 1–10; plan-52
flap/soft-freeze semantics incl. budget off-by-one; queue rename-claim atomicity; binlog
position capture ordering; live-accepted rotation; sql_mode bit positions; shim/compare quoting
parity.
