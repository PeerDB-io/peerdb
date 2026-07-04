# 42 — Batched `replay --all`: fast preflight regression guard

## Problem

`preflightReplayAll` (supervisor/fixloop.go) shells `ddlfuzz replay <sig>` once per finding.
Each invocation spawns a ddlfuzz process, and inside it `replayOne` calls `oracle.SingleDigest`,
which forks the C++ oracle, does the Hello handshake, parses ONE statement, and tears down.
~168 findings × two process lifecycles, sequential ≈ 1–2 minutes of pure overhead per fix.
The fuzzer diffs ~400K cases/s using the same oracle because it keeps a warm `oracle.Client`
(`Start` once) and streams `ParseBatch` batches. Batch the replay the same way.

## Protocol fact that shapes the design

`wire.Conn.ParseBatch` serializes `SQLMode` per case (u64 before each SQL blob); the engine is
fixed per oracle *binary* (oracle-mysql vs oracle-mariadb, checked at Hello). The fuzzer already
sends mixed-mode batches (`chooseMode` per case). Therefore: **group findings by engine only**,
one warm client per engine, per-case sql_mode needs no sub-grouping.

## Changes

### 1. `internal/replay/replay.go` — batched all-findings runner

- Factor `replayOne`'s e2e-lane branch (the `offlineE2EReproducible` block, meta→`e2echeck.Reproduce`
  →Result) into a helper, e.g. `replayE2EOne(sig, engine string, mode uint64, meta map[string]any) (Result, int)`,
  called by both `replayOne` and `RunAll` — one implementation, zero drift.
- New `func RunAll(ctx context.Context, cfg Config, w io.Writer) int`:
  - Enumerate `<state>/findings/` dirs with 12-char names — same filter as today's `Run` all branch.
  - Per finding, classification identical to `replayOne`:
    - `readFinding` error or engine ∉ {mysql, mariadb} → malformed (counts toward exit 11).
    - `offlineE2EReproducible(meta)` → `replayE2EOne` in-process (no oracle; already fast).
    - else fast lane: queue `run.Case{SQL: sql, SQLMode: mode, Engine: engineID, Origin: run.OriginReplay}`
      into the per-engine group, remembering the finding index.
  - Per engine group (mysql → cfg.MySQLOracle, mariadb → cfg.MariaDBOracle):
    - Parser side: `ddllexec.NewWorker(0, cfg.CaseDeadline, nil).RunBatch(cases)` — same
      construction as `replayOne`.
    - Oracle side: ONE `oracle.NewClient(engine, bin, cfg.Timeout)`; `Start(ctx,
      <state>/log/oracle-<engine>-replayall.log)`; `ParseBatch` in chunks of ≤256 cases so each
      wire call stays well under `cfg.Timeout`; `defer Close()`. No process outlives the command.
    - Verdict per case: `compare.Diff(c, res.Sig, res.Err, res.Panic, digest)` → `Result` fields
      exactly as `replayOne`'s fast path (OurSig, OurError, OracleDigest raw, Class/Shape on divergence).
    - **Fallback for identical crash semantics**: if `Start` fails or a chunk's `ParseBatch`
      errors (oracle crash/timeout — possible since repros include oracle-killers), close the
      client and run the plain `replayOne` for every finding in that chunk (and restart the warm
      client for subsequent chunks). Old path semantics: a crashing case is malformed for THAT
      finding only; the fallback reproduces that exactly.
  - Aggregation — same rules as today's `Run("")` branch, but status comes from the meta already
    in hand (`meta["status"]`, empty → `open`) instead of re-reading via `statusOf`:
    - per-finding code 11 → `Summary.Malformed` (sig appended)
    - status `fixed` && !reconciled → `Summary.FixedRegressed`
    - counters `FixedOK`/`Ledgered`/`Parked`/`Open`, `Checked`.
  - Exit: 11 if any malformed, else 10 if any fixed regressed, else 0 — unchanged mapping.
  - Output: stdout = one machine-readable summary JSON line. Extend `Summary` with
    `Regressions []Result` carrying the full per-regression detail (sig, our_sig, our_error,
    class, shape, oracle_digest) — `FixedRegressed []string` stays for compatibility. Stderr =
    one human line per regression: `regressed <sig>: class=<c> shape=<s> ours=<sig|error>`.
- Rewire `Run(ctx, cfg, "", w)`'s all branch to call `RunAll` (bare `replay` = `replay --all`).
  `replayOne` stays as-is for the single-sig path and the fallback.
- Test seam: pass the oracle interaction through a tiny interface (satisfied by `*oracle.Client`),
  e.g. `type digestBatcher interface { ParseBatch(ctx, []run.Case) ([]*digest.Digest, [][]byte, error) }`
  with a `var newBatcher = func(engine, bin string, timeout time.Duration) ...` hook (or an
  optional field on an internal runner struct) so unit tests can inject a fake without the C++ binary.

### 2. `cmd/ddlfuzz/main.go` — CLI surface

- `replay` gains `-all` bool flag. `replay --all` → `RunAll`; bare `replay` (no sig) unchanged in
  meaning, now batched; `replay --all <sig>` → usage error (exit 2). `--from/--expect-accept`
  unchanged.

### 3. `supervisor/fixloop.go` — `preflightReplayAll` rewire

- Replace the `ScanFindings` + per-sig `RunReplay` loop with ONE
  `RunTimeout(ctx, cfg.DDLDir, 10*time.Minute, nil, cfg.DDLfuzzBin, "replay", "--all")`.
- exit 0 → nil. Nonzero → error. For a precise message, parse the stdout summary JSON:
  exit 10 → `fixed finding(s) regressed: <FixedRegressed>` (+ class/shape from `Regressions`);
  exit 11 → `finding(s) cannot be evaluated: <Malformed>`; parse failure/other → generic error with
  `resultOutputTail`. Put the parsing in a small pure helper so it unit-tests without processes.
- Pass/fail meaning preserved: `fixed` must reconcile (any non-0 outcome for a fixed finding
  blocks — exit 10 via FixedRegressed, exit 11 via Malformed); `ledgered`/`parked`/`open` may
  still diverge (10 per-finding is fine) but must be evaluable (malformed blocks). One accepted
  corner-case difference: a findings dir whose meta.json is unreadable was silently *skipped* by
  `ScanFindings` but files as malformed (blocking) in replay-all — this matches what bare
  `ddlfuzz replay` already reports today and the guard's intent (unevaluable findings block).
- Do NOT touch the single-sig `RunReplay` callers: `validateReplayResolution`, `confirmFixed`,
  `autoResolveReconciled`, `applySuccessfulStatuses`, park.go.

### 4. `supervisor/prompt.tmpl` — codex pre-commit regression check

In step (c) VERIFY, after the `replay {SIG}` requirement, add: also run
`tools/ddlfuzz/build/ddlfuzz replay --all` (fast batched regression check across ALL findings;
uses the same rebuilt binary the `replay {SIG}` check already needs). If it exits nonzero /
reports `fixed_regressed`, the fix broke another finding — revise before committing. State
explicitly it is READ-ONLY over other findings' state, so running it does not violate the
"never touch other findings" guardrail. Match the existing prompt tone/format.

## Out of scope

- No changes to the parser, `internal/compare`, oracle C++, `internal/e2echeck`, or golden.
- `golden` batching: follow-up only. `ddlfuzz golden` must keep printing `irreconcilable:0`.

## Test plan

- Unit (`internal/replay`): `RunAll` on a temp state dir with a fake batcher —
  fixed+reconciled / fixed+regressed (correct sig in FixedRegressed + Regressions detail) /
  open+diverging (counted, exit 0) / ledgered + parked counting / malformed meta (exit 11) /
  e2e-lane finding routed through the offline reproducer (reuse fixtures style from
  `replay_e2e_ddlfuzz_test.go`) / batch-error fallback path (fake batcher errors once → verdicts
  identical to per-case path).
- Unit (supervisor): the preflight failure-message helper over fabricated `Result`s (exit 10
  with summary JSON, exit 11, garbage stdout).
- `go build ./... && go vet ./... && go test ./...` in tools/ddlfuzz.
- `go build -o build/ddlfuzz ./cmd/ddlfuzz && ./build/ddlfuzz golden` → `irreconcilable:0`.
- Equivalence (manual, with real findings fixture): per-sig `replay <sig>` loop vs `replay --all`
  over the same ~168 findings — identical per-finding pass/regress verdicts; record wall-time
  speedup.
