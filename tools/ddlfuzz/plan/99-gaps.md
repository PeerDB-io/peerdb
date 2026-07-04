# 99 — implementation audit: gaps and defects

Audit of the implemented rig + the parser under test against plans 00–52, 2026-07-03 (campaign
live at time of audit). Five independent review passes: plan conformance, fast lane, supervisor,
e2e lane, parser. Headline findings spot-verified by hand. Structure, state layout, wire
protocol, exit codes, canonicalization rules, and the gate are faithful to plan; the defects
below are what diverged. `P` = parser, `R` = result integrity, `S` = supervisor, `D` = dead/no-op
safety machinery, `G` = plan gaps, `L` = low tail.

## Fix-first shortlist

1. **P1** — revert MariaDB identifier lowercasing (parser regression; a real bug is parked).
2. **S1** — ignored `GitUntrackedSet` error voids rollback's data-loss protection.
3. **S2** — `last_good_commit` advanced before rebuild succeeds; failure wedges the fix loop.
4. **S5** — e2e wedge detector is dead code (type-assertion always fails).
5. **R1** — confirm-fixed replays run under the wrong sql_mode; existing verdicts for
   empty-mode findings are untrustworthy.

## P — parser under test (production impact)

- **P1 (HIGH).** `flow/connectors/mysql/ddl_parser.go:162` `normalizeTableIdent` lowercases all
  schema/table idents when `isMariaDB` (introduced by `feb2407a` to appease the oracle). Linux
  MariaDB defaults `lower_case_table_names=0`: `ALTER TABLE Users ...` parses to `users`, misses
  `TableNameMapping` (`cdc.go:865`) → **actionable DDL silently dropped**. Case-only renames
  (`ALTER TABLE t RENAME T`) vanish via `ddlRenamePairIsNoop` (`ddl_parser.go:366`) — parked
  `e9bd81d40423` is this genuine bug suppressed; open `7f5636632ad9`, `41bee5c7f43c` are the same.
  `strings.ToLower` also mangles latin1 bytes to U+FFFD. Plan 21:502 says oracles must not case
  fold — fix the MariaDB oracle (runs with macOS lctn=2), revert the parser change, unpark.
- **P2 (MED).** `ddl_lexer.go:39` `ddlWordIs` uses `strings.EqualFold` (Unicode, not ASCII);
  `strings.ToUpper` switches at `ddl_parser.go:461,524,683` likewise. `DROP ıNDEX` (column named
  `ıNDEX`, U+0131 folds to I) skips as benign DROP INDEX → silent dropped column; `ADD ſpatial
  INT` same. Fix: byte-wise ASCII compare, reject ≥0x80 before keyword match.
- **P3 (MED).** `ddl_parser.go:339` — trailing no-comma partition clause after a RENAME spec
  errors on valid MySQL DDL: `ALTER TABLE t RENAME COLUMN a TO b REMOVE PARTITIONING` (open
  `3f58c52e9113`). Loud, but all specs in the statement are lost. After ADD/DROP specs the same
  clause is silently eaten — asymmetry only bites RENAME-final.
- **P4 (MED).** `ddl_parser.go:683` paren benign list lacks MariaDB `VECTOR`: `ADD (VECTOR INDEX
  v (emb))` fabricates column `VECTOR` (open `3361cbb7892b`, `9882aa4b0b0d`).
- **P5 (LOW).** `ddl_lexer.go:121,330` — dollar quoting applied to all MySQL versions; on 8.x
  sources `$x$` is a legal identifier → unterminated-string error (loud) or, with a second `$x$`,
  silent wrong tokenization.
- **P6 (LOW).** `ddl_lexer.go:78` — `--` comment detection is ASCII-space only; latin1 NBSP
  (0xA0) after `--` starts a comment server-side, not here → possible spurious `@pos`.
- **P7 (LOW).** `ddl_parser.go:413` — `DROP PARTITION p0, add` puts partition names at spec-head;
  names colliding with spec keywords error. Always loud, never silent.
- Verified clean: no panic/hang paths (all loops advance, lookahead bounded); lexer semantics per
  writeup; attribute region rules; shim reductions ≡ tidb-diff reductions, `fuzzDDLSigIdent` ≡
  `compare.sigIdent`. Writeup is stale on bare `ALTER ... RENAME` (now emits `ddlRenameTable`).
- Parked-list audit: `e9bd81d40423` = real bug (P1). `0c216d4aff9d` looks oracle-side (digest
  misses rename under ORACLE mode). Rest consistent with harness artifacts.

## R — result integrity (findings/verdicts can be wrong)

- **R1 (HIGH).** `e2e/worker.go:216` `paletteEntry`: queue item with `SQLModeName==""` (weight
  3/7 of palette) falls through to palette-by-seq, and queue items run `seq==0` →
  `runOne` substitutes `time.Now().UnixNano()` → confirmation replays under a random mode (incl.
  ORACLE/MSSQL on MariaDB). All `done/<sig>.json` verdicts for empty-mode findings tested the
  wrong condition. `item.SQLMode` (uint64) is dead — never consulted.
- **R2 (HIGH).** `e2e/matcher.go:218` `alignExpect` pairs any non-marker event with the next DDL
  expectation, no text anchor. One extra binlog event (e.g. driver-errored but server-applied
  statement; `worker.go:161` records exec-reject, pushes no expectation) shifts pairing by one →
  artifact `e2e-query-rewrite`/`e2e-plumbing-sig` findings and wrong queue verdicts until a
  marker re-anchors; dropped expectations are silent. The plan-30 loud marker/control mismatch
  branches (`matcher.go:269-279`) are unreachable. `checks_test.go:146` passes only because it
  bypasses `alignExpect`.
- **R3 (HIGH).** `cmd/ddlfuzz/fuzzloop.go:374` bisection cap compares `depth` to
  `2*bitLen(current sub-batch)` — trips at 3–8 cases always, so a deterministic single-input
  oracle crash never isolates to 1 case, and all crash findings share shape
  `"state-dependent batch"` (`fuzzloop.go:399`) → distinct crash bugs dedup to one sig per
  engine/mode. Plan meant a total-resubmission counter.
- **R4 (HIGH).** Plan 21 step 3's state-dependent guard (full batch fails, both halves pass →
  file whole batch as `oracle_crash`) unimplemented — that case files **nothing**
  (`fuzzloop.go:376-380`); crash evidence is an `oracle_restarts` bump only.
- **R5 (MED).** `internal/compare/compare.go:833` rename pairs re-parse idents:
  `rename a.b>c` (schema-qual) and ``rename `a.b`>c`` (dotted table) canonicalize identically —
  a parser mis-split of a quoted dotted name reconciles as a non-finding. `parseAlter` keeps the
  qual verbatim (`compare.go:800`); only rename is exposed.
- **R6 (MED).** `compare.go:348,359` embed counts/kind-lists in `stmt_count(...)`/
  `spec_count(...)` shapes (plan says bare dimension names) → one bug fans out to many sigs,
  burns the 200 `max-open-findings` cap, then suppresses genuinely new findings. Cap check also
  runs before `findings.Record` (`fuzzloop.go:456-464`) so at cap even `times_seen` stops.
- **R7 (MED).** Live/offline check drift: `safeSQLModeFromStatusVars`/`safeDDLSignature`/
  `safeParseForE2E` copy-pasted between `e2e/checks.go:253` and
  `internal/e2echeck/reproduce_ddlfuzz.go:153` with real divergence — live plumbing compares
  under `exp.SQLModeRelevant` (`checks.go:121`), offline repro uses full `in.SQLMode`
  (`reproduce_ddlfuzz.go:99`, fallback at :70) → mode-sensitive statements can reconcile
  differently live vs offline. Plan 50 §2 said move, don't reimplement.
- **R8 (MED).** `e2e/queue.go:88` — pending items for engines this process doesn't run are
  claimed and completed as `exec-reject` ("unknown engine") instead of left for the right lane.
- **R9 (LOW).** `e2e/checks.go:239,359` — if `findings.Record` AND the fallback both fail (disk
  full), `findingCount==0` → a diverging replay completes `confirmed-fixed`.
- **R10 (LOW).** `e2e/rewrite.go:48` — old→new map iteration can double-substitute when an old
  column set contains a fixture target name (`x→c_int` before/after `c_int→c_tiny` is
  order-dependent); word-boundary replace also hits string literals/comments. Shape check
  (names off) can't catch either. Corrupts intent/dedup, not verdicts.
- **R11 (LOW).** `compare.go:663` `sigIdent` doesn't quote an ident literally named `@pos`;
  `parseSpec` (`compare.go:847`) strips the suffix → false `parse_our_sig` mismatch.
- **R12 (LOW).** `internal/digest/digest.go:67` `Reset` doesn't clear `Kind/Schema/Table`; pooled
  reuse could leak stale fields into a digest → fabricated divergence. Near-unreachable today
  only because `Release` is called solely on the decode-error path (pool is also ineffective).

## S — supervisor hazards (data loss / wedge / lifecycle)

- **S1 (HIGH).** `supervisor/fixloop.go:728` `beforeUntracked, _ := GitUntrackedSet(...)` —
  error discarded. On a transient failure (30s RunTimeout under gate load) `before` is empty and
  a failed attempt's rollback (`fixloop.go:1000-1010` → `DeleteNewUntracked`, `git.go:214`)
  deletes **every** untracked file under `flow/` + `tools/ddlfuzz/` (except state/, build/),
  including `untracked.baseline`-protected pre-campaign files (baseline is NOT consulted at
  delete time). This is the already-experienced data-loss class. Fix: hard-fail the attempt if
  the snapshot fails.
- **S2 (HIGH).** `fixloop.go:854` writes `last_good_commit` = new HEAD before
  `rebuildAndHotRestart` (:861). Rebuild/rename failure → outcome `failed` → rollback to the
  **old** lastGood while the state file points at the reset-away HEAD → every later attempt fails
  HEAD-drift (`fixloop.go:960`) forever; plan 51:130 names this hazard. Sibling statuses from
  `applySuccessfulStatuses` (:858) also survive the rollback (metas claim fixes whose commits
  were reset away).
- **S3 (HIGH).** Shutdown mid-attempt: rollback runs on the cancelled ctx (`fixloop.go:788`), so
  `git reset --hard` is killed instantly (`proc.go:138`), possibly mid-checkout; untracked
  cleanup never runs; error swallowed. Next start blocks preflight on dirty tree — human
  required. Also records `failed` (burns a budget slot; plan says `timeout`) and SIGKILLs codex.
  Use `context.Background()` for rollback (as `composeDown` already does, `main.go:157`).
- **S4 (HIGH).** No orphan detection on restart: children get own pgids (`proc.go:42`), pid-flock
  auto-releases, `runCommand` only deletes `current-attempt.json` (`main.go:129`). An orphan
  codex keeps editing the repo during the new run's preflight/gate; orphan fuzzer/e2e
  double-write state.
- **S5 (MED).** `main.go:568` `statsStale` asserts timestamp is `string`; `e2e-stats.json`
  `updated_at` is int64 (`e2e/stats.go:131`) → always false → e2e wedge restart (`main.go:334`)
  can never fire. Also code uses 5min vs plan 30 §9's 60s, and the `matcher_last_event_at` rule
  is absent. Fast lane OK (RFC3339 string).
- **S6 (MED).** `fixloop.go:1036-1046` shadowed `err` makes ledgered-replay validation dead: the
  `RunReplay` error/exit is never checked for `ledgered` outcomes — an agent can ledger a
  finding whose replay exits 0 (should have been fixed) or errors.
- **S7 (MED).** `confirmFixed` (`fixloop.go:1066`) lacks the `replayValidatesFinding` gate that
  `autoResolveReconciled` (:1091) has, contradicting the comment at :1113 — live-only e2e
  regressions without offline capture re-marked `fixed` every 30s tick, forever.
- **S8 (MED).** Torn last line in `attempts/<sig>.jsonl` (crash mid-append): `SelectFinding`
  ignores the load error (:583) → selects the sig as 0-attempts; `FixOnce` errors on the same
  load (:698) before doing anything → livelock on one sig, no park, no progress on others.
- **S9 (MED).** Git-drift handling (`fixloop.go:724`) never attempts cleanup (plan 40: cleanup,
  then escalate + 10min retry); implementation escalates + retries every 30s, rewriting
  `run-git-drift.md` each tick. With S3/S4, any crash-dirty tree needs a human even when
  `rollbackAttempt` would heal it.
- **S10 (MED).** `fix-once` (`main.go:84`) takes no supervisor lock → concurrent with `run` it
  races git reset/commit/`last_good_commit`/`spend.json` (`AddSpend` unlocked).
- **S11 (LOW).** Restart backoff index never resets on healthy uptime and hot-restarts count
  toward the crash-loop breaker (`main.go:197-244`); after 5 lifetime exits every post-fix
  hot-restart costs 300s; `degraded` is never cleared once set (:237,:350).
- **S12 (LOW).** `status.go:379` `lastAgentMessage` returns "" for transcripts >256KiB
  (`tailFile` cuts mid-line; `ParseCodexJSONL` aborts on first bad line, `fixloop.go:377`); same
  abort loses all token accounting for killed attempts (`fixloop.go:763` — `AddSpend`
  undercounts).
- **S13 (LOW).** Failed lock acquisition writes `state/BLOCKED` (`main.go:120`) → status/watch
  show BLOCKED for a healthy running campaign until next preflight clears it.
- **S14 (LOW).** `park.go:173` flap trigger `!previouslyKnown` is consumed on first sighting even
  when the one replay flakes (exit 11/1/timeout; `replayReproduces` requires exactly 10) — only
  `reopened_count>0` can re-trip.
- **S15 (LOW).** Budget-exhausted sig whose process dies between 3rd attempt record and
  `parkBudgetExhausted` (`fixloop.go:892`) stays `open` forever, silently skipped by selection
  (:571), no escalation file.
- **S16 (LOW).** `gzipOldAttempts` (`main.go:524`) gzips transcripts that
  `writeEscalation`/`extractDiagnosis` (`park.go:249,284`) later read ungzipped → escalations
  after low-disk compression say "diagnosis: unavailable".
- **S17 (LOW).** Commit hygiene prompt-only: no trailer validation (plan 40 step e);
  `plan.Kept` (files kept by deletion policy) dropped at `fixloop.go:1008` instead of noted in
  the attempt record.

## D — dead / no-op safety machinery

- **D1.** Both minimizers are no-ops. Fast lane: `minimize.FastLanePredicate`
  (`internal/minimize/minimize.go:38`) returns always-false (stub) — `ddlfuzz minimize` burns a
  ddmin and exits 0 unchanged. E2E: `tryMinimize` predicate is `bytes.Equal(candidate, stmt)`
  (`e2e/checks.go:344,397`) — tautology. Also the comma ddmin stage joins parts with no
  separator (`minimize.go:52` vs :128 — `ADD a INTDROP c`) so multi-spec reduction is
  structurally impossible. Fast lane never calls minimize opportunistically (plan 21 step 15).
- **D2.** Fatal path deadlocks instead of exit 3: on `<-fatal` (`fuzzloop.go:170-188`) the loop
  breaks then `wg.Wait()` — ctx never cancelled, producers (`:216`, `for ctx.Err()==nil`) and
  surviving oracle goroutines run forever; stats go stale; recovery only via supervisor's 5-min
  watchdog, not the plan's exit(3) contract.
- **D3.** "Preflight on every fuzzer restart" (00 §cross-component: replay all repros, fixed must
  pass) unimplemented — `FuzzerManager.Run` (`main.go:196`) restarts with backoff only;
  `preflightReplayAll` runs solely as fix-attempt step 4f.
- **D4.** `state/inflight.jsonl` (00 state contract, 21 step 11 crash journal) does not exist —
  zero grep hits. An input that kill-9s/OOMs the fuzzer mid-batch is never re-encountered. Also
  no in-process 60s drain on SIGTERM (`fuzzloop.go:165` relies on supervisor SIGKILL, skipping
  final flush).
- **D5.** Per-fix seeds never loaded: `internal/seed/seed.go:32` `LoadDir` reads only
  `seeds.jsonl`; the 25 committed `<sig>.sql`/`<sig>.meta.json` pairs (D8, 40-C7) feed nothing —
  fix-agent regression seeds never enter corpus or golden. (Plan 52 §6 rationalized this,
  contradicting binding 00-D8.)
- **D6.** `corpus.db` never read back into fast-lane mutation bases on restart
  (`fuzzloop.go:112`; `corpus.RandomSQL` used only by e2e `rewrite.go:21`) — SanCov-retained
  corpus is write-only after the first restart; the step-10 coverage feedback loop is inert
  across restarts.
- **D7.** Plan 21 step 6 hang containment absent: `internal/exec/exec.go:52` leaks an abandoned
  goroutine per timeout, no abandoned>8 exit(3), no RSS ceiling (only `debug.SetMemoryLimit`,
  `main.go:240`); `curSeq`/`curDead` vestigial. A parser-spin input class recurring from a
  retained base pins CPUs for hours.
- **D8.** No startup guard against a non-`ddlfuzz`-tagged binary (`internal/exec/parser_stub.go`
  errors on every input → every accepted case becomes `we_error`, instantly saturating the
  findings cap). One `DefaultParser(nil,0,false)` probe at startup would fail fast.
- **D9.** e2e stale-`processing/` recovery runs only at startup (`e2e/queue.go:74`); items
  orphaned mid-run (worker exit after `--cases`, matcher panic path files `e2e-panic` but never
  `completeQueueItem`, `matcher.go:242`) wait for the next process restart.

## G — plan divergences (scope/contract, not crashes)

- **G1.** Generator/mutators are a small fraction of plan 21 steps 12–13: single-statement
  ALTER/RENAME only; no benign heads (~5%), `;`-chains, SET STATEMENT, NULs, comment/quoting
  zoo, `IF EXISTS`/`WAIT n`, multi-pair RENAME; fixed 4-table/6-col vocab (`gen.go:58`);
  `mutate.Crossover` dead (`mutate.go:139`); sql_mode-toggle operator missing. Makes 00 §Exit's
  construct-checklist metric unreportable. (Byte-safety + sentinel-db constraints ARE respected.)
- **G2.** Digest schema grew alter-side `new_schema`/`new_table` + `OracleSig` rename-split
  (`digest.go:17`, `compare.go:70`) — ratified de facto by e3c92a3a/plan 52, never folded into
  00's schema or 21's R9.
- **G3.** Extra skip rules beyond "complete set R1–R13": `ContainsSkippedVersionComment`
  (`compare.go:122,172`) and `EquivalentQueryText` whitespace/`;`/version-comment tolerances
  (`e2echeck/query_text.go:7`) — while the plan's one enumerated exception
  (`/* generated by server */` suffix) is NOT implemented.
- **G4.** `Case.Seed` is drawn after generation (`fuzzloop.go:249,258`) — not PCG state; no
  `replay --seed`. meta.json `seed` can't regenerate a case (repro.sql makes it mostly moot).
- **G5.** Plan 30 §9 binlog purge (6-hourly FLUSH/PURGE) unimplemented (`runner.go:107`) —
  tmpfs binlogs accumulate against the 24h expiry window.
- **G6.** e2e palette weighting (`''`×3) and backlog hysteresis (pause >256 / resume <64) not
  implemented (`worker.go:216`, `worker.go:99` — resumes at ≤256).
- **G7.** e2e decimal unwritten-param reconciliation skipped rather than default-checked
  (`e2echeck/compare.go:229` gates on `>=0`; no shared helper with `internal/compare`).
- **G8.** `fallbackRecordFinding` (`e2e/checks.go:409`) writes contract-shaped findings dirs with
  non-descriptor ad-hoc sigs (identifier-sensitive, no dedup vs parked/ledger) on Record failure.
- **G9.** Oracle stderr logs unbounded (`oracle/oracle.go:57` O_APPEND, no 8MiB rotate; also
  `-single`/`-replayall` logs).
- **G10.** Wire client trusts oracle-supplied count/length before validating
  (`wire/wire.go:56`, :131) — corrupted frame → huge-alloc panic instead of treat-as-crash;
  `n<0` checks at :65/:87/:103 dead. `internal/run` holds only the Case type while orchestration
  lives in `cmd/ddlfuzz/fuzzloop.go` (contra layout intent).

## L — low tail

- `e2e/fixture.go:231` `fallbackFreshName` latent infinite loop; :223 ORACLE/MSSQL fallback
  emits unquoted `spelled name`/`new`​`tick` fresh names (guaranteed exec-rejects, seq%16 ∈
  {13,14}).
- `e2e/runner.go:107,167` + `stats.go:166`: ticker and final `stats.Write` race the same fixed
  `.tmp` path; `smoke.sh` shares the live campaign's state dir + compose project (running it
  concurrently drops `fuzz_w<k>` databases).
- `e2e/health.sh:5` awk false-passes on compose <2.21 single-line JSON output.
- `queue.go:101` pending-gauge readback is a racy no-op.
- Stats drift: failed-batch cases never counted in `execs_total` (`fuzzloop.go:350`);
  `execs_per_sec` is a since-start average, not interval EWMA; `suppressed` conflates
  ledger/parked with capped re-seen.
- `findings.Record` re-reads `parked.list` + full `ledger.jsonl` and rewrites meta per
  occurrence (`findings.go:75,197`) under `recordMu` — serializes oracle-proc completion on hot
  sigs; plan said in-memory + 60s/SIGHUP re-read.
- `golden` spawns a fresh oracle process per seed×engine (`golden/golden.go:64`) and aborts the
  whole run on one oracle error (:66). (Plan 42 defers batching; the abort is the defect.)
- `sancov.Merge` (`sancov/sancov.go:49`) builds `[]uint64` over an 8-byte-misaligned base —
  works on arm64/amd64, technically invalid unsafe.
- `replay <sig>`/`minimize <sig>` args joined into paths unvalidated (`main.go:246`,
  `replay.go:509`) — `../` escapes findings dir (local CLI, minor).
- Dead code: `resetFixture`, `isFixtureRename`, `splitTopLevelFallback`, runner/worker `done`
  channel, `matcher.go:311` overwritten `queries`, `replay.statusOf`, `seed.go:333` default
  branch; `fuzzloop.go:112` ignores `seed.LoadDir` errors silently; `-exec-workers` parsed,
  unused.
- Plan-text staleness (code right, doc wrong): plan 40's `go build` lines lack `-tags ddlfuzz`
  (code has it, `preflight.go:133`); plan 40's embedded prompt template predates D5/42/50/52
  (shipped `prompt.tmpl` is current); go.mod carries `go-mysql`/`modernc.org/sqlite` vs 20/21's
  "stdlib+flow only" (both necessitated by planned features); writeup stale on bare
  `ALTER ... RENAME`.

## Verified clean (audited, no defect)

Wire framing + crash-respawn/desync handling; SQLite corpus store (parameterized SQL, identity
hash, budget); sancov OR-accumulation logic; D1–D5/D7 rules and the Diff decision table 1–8;
descriptor construction (sorted keys, mode mask, sha256-12); D4 closed class set enforced;
state-dir layout and atomic temp+rename writes; `DeleteNewUntracked` path policy given a real
`before` set (`..`, abs, `.git/`, out-of-root, state/build exclusions); verification gate ≡ plan
+ plan-51 step 7; startup preflight steps 1–10; plan-52 flap/soft-freeze semantics incl. budget
off-by-one; queue rename-claim atomicity; binlog position capture ordering; live-accepted
rotation; sql_mode bit positions; shim/compare quoting parity.
