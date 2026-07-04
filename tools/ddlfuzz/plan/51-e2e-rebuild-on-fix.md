# 51 — Rebuild & restart the e2e lane on successful fixes (supervisor)

Follow-up fix plan for `tools/ddlfuzz/supervisor/`. Conforms to `40-supervisor.md` and
`00-overview.md`; deviations listed in **Contract issues**. Executor: implement exactly this; no
open decisions. Line numbers refer to the tree at branch point `ddlfuzz-e2e-rebuild-on-fix`.

## Problem

On a successful fix the supervisor rebuilds `build/ddlfuzz` and hot-restarts the fast-lane fuzzer
(`rebuildAndHotRestart`, `supervisor/fixloop.go:1044`, called from the success path at
`fixloop.go:790`), but never rebuilds `build/ddlfuzz-e2e` or restarts the e2e lane. After a fix
that changes code compiled into the e2e binary (parser in `flow/connectors/mysql`, or the
e2e/e2echeck harness), the live lane keeps executing the stale pre-fix binary, re-surfaces the
just-fixed divergence, and re-opens the finding (C8). The fix loop then re-selects it, the fresh
driver's `replay` reconciles (exit 0), the agent prints `RESULT: failed <sig> did-not-reproduce`,
and after 3 such attempts the genuine, committed fix is mislabeled `parked`.

Evidence: finding `bc2d0d42f7ca` (e2e-missed-column-effect) — attempt 1 `outcome=fixed`, commit
`96c7bdd1` (edits `internal/e2echeck/compare.go`), replay exits 0; meta ended `status=parked`
because attempts 2–3 were did-not-reproduce after the stale lane re-opened it.

Two changes, both confined to the supervisor:

1. **Primary** — rebuild `build/ddlfuzz-e2e` and hot-restart the lane when a successful fix
   touches sources compiled into it.
2. **Secondary (robustness)** — a re-opened, previously-fixed sig whose replay reconciles is
   re-marked `fixed` (confirm-fixed) instead of burning attempt budget and getting parked.

## 1. Primary: e2e rebuild + hot-restart

### Trigger predicate

`touchesE2EBinary(paths []string) bool` in `supervisor/gate.go`, next to `touchedOracleEngines`
(`gate.go:98`); input is the already-computed `GitTouchedPaths` result (`paths` at
`fixloop.go:745`, in scope at the success path). True iff any path has one of these prefixes /
exact names:

```
flow/
tools/ddlfuzz/e2e/
tools/ddlfuzz/cmd/ddlfuzz-e2e/
tools/ddlfuzz/internal/
tools/ddlfuzz/go.mod        (exact)
tools/ddlfuzz/go.sum        (exact)
```

Everything else — notably `tools/ddlfuzz/oracle/**` (C++ oracle sources, rebuilt separately via
`RebuildOracles`), `tools/ddlfuzz/seeds/**`, `tools/ddlfuzz/supervisor/**`, `tools/ddlfuzz/plan/**`,
`docs/**` — does NOT trigger. This satisfies the requirement that oracle-only fixes never bounce
the e2e lane.

Rationale for a static superset rather than the exact import closure: the true closure
(`go list -deps ./cmd/ddlfuzz-e2e`) is `flow/connectors/mysql` + ~20 transitive
`flow/**` packages + `e2e` + `cmd/ddlfuzz-e2e` + `internal/{compare,corpus,digest,e2echeck,exec,
findings,gen,run}`. Freezing that list is brittle (an agent fix adding one import edge
silently defeats the trigger — exactly this bug again), and running `go list` in the success path
adds a toolchain call for no benefit. The superset's only cost is a spurious lane restart on a
fast-lane-only `internal/**` change (e.g. `internal/mutate`): ~seconds of lane downtime, compose
stack untouched, corpus/state retained. A false negative reintroduces the bug. The predicate is a
pure function → unit-testable.

### `E2EManager.HotRestart`

Add to `supervisor/main.go`, mirroring `FuzzerManager.HotRestart` (`main.go:259`):

```go
func (m *E2EManager) HotRestart(ctx context.Context, newBin string) error {
	m.mu.Lock()
	p := m.proc
	m.mu.Unlock()
	if p != nil {
		p.StopGracefully(60 * time.Second)
	}
	return os.Rename(newBin, m.cfg.E2EBin)
}
```

- Lane down or disabled (`proc == nil`, incl. disk-watchdog `SetDisabled`): the rename still
  happens — this is the required no-op-ish behavior. The `Run` loop (`main.go:310`) execs
  `cfg.E2EBin` fresh on every relaunch, so the next start (or re-enable) picks up the new binary
  with no further coordination.
- Lane up: `StopGracefully` makes `Run`'s waitLoop observe `p.Done()`, break, back off (5s first),
  `composeUp` (idempotent `up -d --wait` on a running stack), and relaunch onto the renamed
  binary. No `composeDown` — the docker services are owned state and stay up.
- The restart increments `restarts` and counts toward the 6-in-30min crash-loop breaker, same as
  fuzzer hot-restarts do today. Accepted: fix cadence is serialized 45-min attempts, so
  fix-driven restarts cannot trip the breaker on their own.
- Atomic rename pattern is identical to the fuzzer path: build to `build/ddlfuzz-e2e.new`, then
  `os.Rename` over `cfg.E2EBin` (same directory, atomic).

### Rebuild helper + call site

Add to `supervisor/fixloop.go`, beside `rebuildAndHotRestart`:

```go
func rebuildAndHotRestartE2E(ctx context.Context, cfg Config, e2e *E2EManager) error {
	newBin := filepath.Join(cfg.BuildDir, "ddlfuzz-e2e.new")
	res, err := RunTimeout(ctx, cfg.DDLDir, 10*time.Minute, nil,
		"go", "build", "-tags", "ddlfuzz", "-o", newBin, "./cmd/ddlfuzz-e2e")
	// error wrapping identical to rebuildAndHotRestart
	if e2e == nil {
		return os.Rename(newBin, cfg.E2EBin)
	}
	return e2e.HotRestart(ctx, newBin)
}
```

Success path (`fixloop.go:790`) becomes:

```go
if err := rebuildAndHotRestart(ctx, cfg, skipFuzzer, restarter); err != nil {
	outcome, detail = "failed", err.Error()
} else {
	if touchesE2EBinary(paths) {
		if err := rebuildAndHotRestartE2E(ctx, cfg, e2e); err != nil {
			logf("e2e rebuild after %s failed: %v", sig, err)
			_ = writeRunEscalation(cfg, "run-e2e-rebuild-failed.md", err.Error())
		}
	}
	if outcome == "fixed" {
		_ = enqueueE2EConfirmation(cfg, finding)
	}
}
```

Ordering is load-bearing: the e2e rebuild/restart runs BEFORE `enqueueE2EConfirmation`, so the
C11 confirmation replays on the fixed binary instead of the stale one reporting `still-diverges`.
The fast-lane fuzzer hot-restart is unchanged.

**e2e rebuild failure is non-fatal to the attempt** (log + `escalations/run-e2e-rebuild-failed.md`,
continue): at this point the fix is committed, gate-green, `last_good_commit` already advanced and
statuses applied — flipping `outcome` to `failed` would trigger the rollback at `fixloop.go:797`
to the OLD lastGood while `state/last_good_commit` points at the new HEAD, wedging the loop on
HEAD-drift. (The fuzzer rebuild branch has this hazard today; do not extend it.) With the gate
addition below, the e2e link is verified before success is declared, so failures here are
environmental only; the secondary safeguard (§2) bounds the fallout of a stale lane meanwhile.

### Gate addition

The gate must compile the e2e binary: `go test ./...` alone does not build test-less main
packages like `cmd/ddlfuzz-e2e`. Append a step to `gateSteps` (`gate.go:31`):

```go
{Name: "ddlfuzz go build", Dir: cfg.DDLDir, Timeout: 10 * time.Minute,
	Args: []string{"go", "build", "./..."}},
```

This makes an agent commit that breaks the e2e binary a `gate_failed` attempt (rolled back)
instead of a post-success rebuild surprise. Also append the equivalent line to the agent's step
(c) gate text in `supervisor/prompt.tmpl` so the agent pre-runs the same gate
(`cd ../tools/ddlfuzz && go build ./... && go test ./...`).

### Plumbing

- `FixOnce(ctx, cfg, sig, skipFuzzer, restarter *FuzzerManager, e2e *E2EManager, siblings, logf)` —
  new `e2e` param (`fixloop.go:633`).
- `RunFixLoop(ctx, cfg, restarter *FuzzerManager, e2e *E2EManager, logf)` (`fixloop.go:596`);
  `runCommand` passes the existing `e2e` manager (`main.go:160`).
- `fixOnceCommand` (`main.go:104`) passes `nil` for both managers; with `--skip-fuzzer` and/or nil
  managers the helpers degrade to build+rename only — matching current `skipFuzzer` semantics.

## 2. Secondary: confirm-fixed instead of re-park

A sig re-opened by C8 rediscovery whose fix is already committed must not burn attempt budget.

### Helpers (`supervisor/fixloop.go`)

```go
func priorFixEvidence(meta FindingMeta, records []AttemptRecord) bool
	// meta.FixedBy != ""  (sibling fixed via applySuccessfulStatuses)
	// || any records[i].Outcome == "fixed"  (this sig was the fixed primary)

func confirmFixed(ctx context.Context, cfg Config, f Finding, logf func(string, ...any)) bool
	// res, err := RunReplay(ctx, cfg, f.Sig)
	// if err == nil && res.ExitCode == 0:
	//     f.Meta.Status = "fixed"           // FixedBy preserved as-is
	//     writeFindingMeta(f.MetaPath, f.Meta); logf("confirm-fixed %s", f.Sig); return true
	// return false                          // exit 10 = genuine regression → normal attempt path
```

No attempt record is appended and `RecordGroupFix` is NOT called (no new fix commit, so the flap
detector's `fix_count` must not move). Ledgered findings are out of scope: C8 never re-opens them
and their replay exits 10 anyway.

### Call sites

1. **Selection sweep** — in `RunFixLoop` (`fixloop.go:614`), after `ScanFindings` and **before**
   `ApplyFlapScanAndPark`/`SelectFinding` (`fixloop.go:621`): for every finding with
   `Meta.Status ∈ {"open", "parked"}` and `priorFixEvidence` (load its attempts jsonl), run
   `confirmFixed`; on success update the in-memory `findings[i].Meta.Status` too so the flap scan
   and `SelectFinding` (`fixloop.go:535`) in the same tick see it. Running before the flap scan is
   required — otherwise a stale-lane re-open of a twice-fixed group parks the whole group.
   Including `"parked"` retro-heals already-mislabeled findings (e.g. `bc2d0d42f7ca`) on the next
   supervisor tick: meta flips back to `fixed`; the `parked.list` entry and `escalations/<sig>.md`
   are deliberately left in place (the list is consumed by 20's dedup — suppression of a fixed sig
   is harmless, and rewriting the list would touch 20's contract). `preflightReplayAll`
   (`fixloop.go:990`) then holds the sig to the strict `fixed` bar (exit 0), which we just
   observed.
2. **Attempt short-circuit** — in `FixOnce`, after `LoadAttemptRecords` (`fixloop.go:649`) and
   **before** the `BudgetExhausted` park at `fixloop.go:654`: if `priorFixEvidence &&
   confirmFixed(...)` → `return nil`. Ordering before the budget check is what prevents the
   bc2d0d42 outcome (budget already burned by did-not-reproduce attempts ⇒ park) and covers the
   `fix-once` CLI path, which bypasses `RunFixLoop`.

**No change to `BudgetExhausted` (`fixloop.go:315`)** or to attempt-record semantics:
did-not-reproduce attempts are prevented from being launched, not exempted after the fact. The
sweep is bounded: replay typically takes seconds (10-min hard timeout), runs only for findings
with fix evidence, and each sig stops matching once re-marked `fixed` — re-flips cease entirely
once §1 keeps the lane fresh.

## 3. Tests (supervisor package, no live system)

Extend `supervisor/supervisor_test.go` style (tempdir cfg fixtures):

- `TestTouchesE2EBinary` — table test: `flow/connectors/mysql/ddl_parser.go` → true;
  `tools/ddlfuzz/internal/e2echeck/compare.go` → true; `tools/ddlfuzz/e2e/matcher.go` → true;
  `tools/ddlfuzz/cmd/ddlfuzz-e2e/main.go` → true; `tools/ddlfuzz/go.mod` → true;
  `tools/ddlfuzz/oracle/mysql/digest.cc` → false; `tools/ddlfuzz/seeds/x.sql` → false;
  `tools/ddlfuzz/supervisor/fixloop.go` → false; `docs/x.md` → false; empty list → false.
- `TestE2EHotRestartLaneDown` — `E2EManager` with nil proc, `cfg.E2EBin` in `t.TempDir()`; write a
  dummy `ddlfuzz-e2e.new`; `HotRestart` renames it over `E2EBin` and returns nil.
- `TestConfirmFixed` — fabricate `state/findings/<sig>/meta.json` + attempts jsonl; point
  `cfg.DDLfuzzBin` at a `t.TempDir()` shell stub. Cases: (a) `FixedBy` set, stub exits 0 → meta
  re-marked `fixed`, no attempt record appended; (b) prior attempt `outcome=fixed`, stub exits 0 →
  same; (c) stub exits 10 → status unchanged; (d) no fix evidence → replay stub never invoked
  (stub writes a marker file; assert absent).
- `TestPriorFixEvidence` — pure predicate table.

Not unit-testable: the live relaunch onto the renamed binary, compose idempotence, and the
end-to-end stale-binary scenario. Coordinator smoke (scratch state dir, never the live campaign):
start `ddlsuper run` with short `DDLFUZZ_HOURS`, drive one `fix-once` whose commit touches
`internal/e2echeck` on a synthetic finding, then assert `build/ddlfuzz-e2e` mtime advanced,
`supervisor.log` shows the e2e restart, the lane PID changed, and `e2e-stats.json` `updated_at`
resumes ticking.

## 4. Acceptance

From `tools/ddlfuzz/` (all must stay green):

```
go build ./... && go vet ./... && go test ./...
go build -o build/ddlsuper ./supervisor
go build -o build/ddlfuzz ./cmd/ddlfuzz
go build -o build/ddlfuzz-e2e ./cmd/ddlfuzz-e2e
```

Plus `cd flow && go build ./...` unchanged.

## 5. Scope guard

- Touch only `tools/ddlfuzz/supervisor/**` (incl. `gate.go`, `prompt.tmpl`) — no changes to the
  parser, oracles, driver (`cmd/ddlfuzz`, `internal/**`), or e2e runtime (`e2e/**`,
  `cmd/ddlfuzz-e2e`).
- Ownership boundaries per plan 40 hold: the lane *process* is already 40's to start/restart;
  this plan only exercises that ownership on one more edge (post-fix). Oracle processes, compose
  contents, and 20/30 contracts (C3–C11) are untouched; `parked.list` format and dedup semantics
  unchanged.
- No new config knobs; no state-dir schema changes beyond the new run-level escalation file name
  `run-e2e-rebuild-failed.md` (fits the existing `run-*.md` convention).

## Contract issues

1. Gate gains a 7th step (`go build ./...` in `tools/ddlfuzz/`) — plan 40 calls its
   gate "verbatim from the overview"; this is an additive extension, mirrored into the agent
   prompt's step (c).
2. Plan 40 §Fix cycle step 5 ("rebuild `build/ddlfuzz` … hot-restart handshake") is extended with
   the conditional e2e rebuild/restart described here; C10's build command is reused unchanged.
