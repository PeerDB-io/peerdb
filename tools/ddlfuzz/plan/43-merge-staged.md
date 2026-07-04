# 43 — merge-staged: sneaking changes into a live campaign (`ddlsuper merge-staged`)

Component plan. Conforms to `00-overview.md` and extends `40-supervisor.md`. Executor: implement
exactly what is written here; there are no open design decisions.

## Goal

Let a human (plus feature agents) develop changes while a campaign runs, roll them up onto one
branch, and land them into the campaign tree at a safe moment — deterministically coordinated with
the fix loop (the sole git authority), never racing it. Unmergeable/invalid work errors out
cleanly, the campaign tree rolls back to `last_good_commit`, and the human debugs in the staging
worktree and retries.

## Topology

```
tools/ddlfuzz/worktrees/<feature>/   # gitignored; feature branches; agents work here freely
tools/ddlfuzz/staged/                # gitignored worktree, branch ddlfuzz-staged (NEVER parser-wip;
                                     # git forbids double-checkout, but enforce in CLI too)
/Users/ilia/Code/peerdb              # campaign tree, parser-wip, owned by the fix loop
```

- Human/agents merge feature branches + current campaign head into `ddlfuzz-staged` with **normal
  merges** (merge commits fine; agent commits and manual commits stay separate parents).
- The campaign tree only ever does **`git merge --ff-only ddlfuzz-staged`**: it never performs a
  real merge, never conflicts, never leaves a partial state. All conflict resolution happens in
  `staged/`.
- One-time setup (implementation step 1):
  `git worktree add -b ddlfuzz-staged tools/ddlfuzz/staged parser-wip`.
- Both dirs are already in `tools/ddlfuzz/.gitignore` (`staged/`, `worktrees/`) — ignored files
  never appear in the `??` set, which is what protects them from `DeleteNewUntracked` and the
  drift check. Belt-and-braces: also add both prefixes to the excluded list in
  `DeleteNewUntracked` (`supervisor/git.go:214`).

## The single-slot protocol (`state/merge/`)

Exactly one merge may be in flight. A second concurrent `merge-staged` is an error (two sessions
rolling up into the same staged branch = operator mistake, surface it).

```
state/merge/
  request.json    # owned by the CLI. Created O_CREATE|O_EXCL — this create IS the arbitration.
                  # {id, pid, phase:"hold"|"submitted", restart:bool, created_at,
                  #  expect_staged_head?}          (expect_staged_head set at submit)
  ack.json        # owned by the supervisor. {id, acked_head} — written at a quiescent point
  claimed.json    # rename target: supervisor claims by rename(request.json -> claimed.json)
  result.json     # owned by the supervisor. {id, result, new_head, detail,
                  #  supervisor_restart:"none"|"restarting"|"required"}
```

Rules (each is load-bearing):

- **Exclusivity**: second waiter gets `EEXIST` on create → exit 6, printing the incumbent's
  id/pid/age. No other lock.
- **One writer per file.** The CLI rewrites `request.json` (tmp+rename) ONLY for the
  `hold → submitted` transition. It never rewrites after submitting (else a rewrite racing the
  supervisor's claim-rename would resurrect a phantom request).
- **Cancel = unlink `request.json`.** Unlink-vs-claim-rename: exactly one wins, atomically.
  ENOENT on unlink → it was claimed; the CLI reports "claimed — it will merge or roll back on its
  own", waits for id-matched `result.json`, exits 7. Race-applied is acceptable by design.
- **Keep-hold cancel** (`merge-cancel --keep-hold`): unlink first (same race rules), then create a
  FRESH `request.json` (O_EXCL, **new id**, phase `hold`). Never rewrite in place — the new id
  forces a re-ack, and the unlink-before-create closes the phantom window. Use case: "one more
  change" — the quiescent window survives while staged is amended.
- **Hold adoption**: `merge-staged` hitting `EEXIST` on a phase-`hold` request whose owner pid is
  dead rewrites it in place with its own id/pid instead of failing busy — this is how a fresh
  invocation attaches to the window a keep-hold cancel left behind. Rewriting a hold is claim-safe
  (the supervisor claims only `submitted` requests); a `submitted` or live-owner slot is never
  adopted.
- **Expiry** (supervisor-side, each fix-loop tick, unclaimed `request.json` only): recorded pid
  dead, OR `created_at` older than 6h (pid-reuse backstop). Expired → clear the slot dir, log.
  Once claimed, expiry never applies — service runs to completion or rollback.
- **Cleanup**: the CLI removes the slot contents after reading its result. Supervisor clears
  orphaned results (`result.json` >24h old, waiter pid dead).
- `id` = random 12-hex per invocation. Every consumer matches on it; a supervisor that restarted
  mid-service answers the id it journaled, and a stale result is never mistaken for a fresh one.

## CLI: `ddlsuper merge-staged`

Blocking; run it in a shell the roll-up agent spawns. Flags: `--no-restart` (see Self-restart),
`--ack-timeout` (default 2h — covers one worst-case in-flight attempt incl. gate),
`--result-timeout` (default 2h). Companion subcommand: `ddlsuper merge-cancel [--keep-hold]` (for
canceling from another terminal). `ddlsuper status` gains a merge-slot line.

Sequence:

1. Sanity: `staged/` worktree exists, is on `ddlfuzz-staged`, tracked-clean; campaign repo on
   `parser-wip`.
2. **Dry-run conflict check, before bothering the loop** (git 2.50 present, `merge-tree` fine):
   `git -C staged merge-tree --write-tree <campaign-head> ddlfuzz-staged` — conflicts → print
   conflicting paths, exit 2. No hold taken, no side effects.
3. **Mode select**: try non-blocking flock on `state/supervisor.pid`.
   - Acquired (no supervisor running) → **inline mode** (below), holding the flock throughout.
   - Held → slot protocol: create `request.json` phase `hold`, wait for id-matched `ack.json`.
4. On ack (carries pinned `acked_head`): `git -C staged merge <acked_head>` — a normal merge.
   Conflicts despite step 2 (an attempt landed between check and ack and conflicts) →
   `git merge --abort`, unlink request (release hold), exit 2. Staged is untouched.
5. Rewrite `request.json` → phase `submitted` + `expect_staged_head` = staged HEAD.
6. Wait for id-matched `result.json` (poll 2s; also probe supervisor liveness — flock probe; dead
   with no result → exit 5 with "rerun to take over inline; recovery is automatic").
7. Print result verbatim (incl. `supervisor_restart`), clean the slot, exit per table.
8. SIGINT/SIGTERM at any wait → cancel (unlink, or report-if-claimed), exit 7.

Exit codes: `0` merged (prints new head) · `2` conflicts / not-ff / stale — restage and retry ·
`3` gate/golden/stale-oracle failed post-merge (campaign tree rolled back) · `4` replay
regression (rolled back) · `5` no ack / no result / supervisor died · `6` slot busy · `7`
canceled · `1` internal.

## Supervisor side: servicing at safe points

All servicing runs in the fix-loop goroutine — mutual exclusion with attempts is by construction.
Hook: top of every `RunFixLoop` tick (`supervisor/fixloop.go:632`), BEFORE the deadline-3h guard
and before `SelectFinding`; plus one extra check immediately after `FixOnce` returns (don't wait
out a tick).

Per tick:
1. Slot expiry scan (rules above).
2. `request.json` phase `hold`, ack absent or id-mismatched → write `ack.json`
   `{id, acked_head: HEAD}`. While an unexpired hold or unserviced submit exists: **do not start
   new attempts** (the fuzzer and e2e lane are never paused — they run the previously built
   binary).
3. Phase `submitted` → claim (rename to `claimed.json`) and service:

   a. Deadline guard: `time.Until(cfg.Deadline) < 1h` → result `deadline_too_close`, no git action.
   b. Quiescence (reuse `ensureAttemptGitReady`): HEAD == `last_good_commit`, tracked clean,
      untracked ⊆ baseline. Fail → result `git_drift`.
   c. Staleness: `git rev-parse ddlfuzz-staged` == `expect_staged_head`, and it must contain
      `acked_head` (`git merge-base --is-ancestor`). Fail → result `stale_request`.
   d. Journal `state/merge-inflight.json` `{id, last_good, expect_staged_head}`.
   e. `git merge --ff-only ddlfuzz-staged` — cannot fail after (c); treat failure as `internal`.
   f. Validation (mirrors the FixOnce success path, `fixloop.go:822-876`, minus
      `forbiddenTouchedPaths` — staged work is human-authorized and may touch anything):
      - `RunGate` (nice -n 10). Fail → rollback, result `gate_failed` + output tail.
      - Diff touches `tools/ddlfuzz/oracle/` → oracle-binary handoff (next section). Fail →
        rollback, result `stale_oracle_binaries` or `golden_failed`.
      - `preflightReplayAll`. Fail → rollback, result `replay_regressed` (strict: a merge that
        regresses a `fixed` finding is rejected; reopening findings invites flap-detector noise
        and muddies attribution).
   g. Success bookkeeping: `last_good_commit` ← new HEAD; refresh `untracked.baseline`; append
      `state/merges.jsonl` `{id, prev_head, new_head, ts, restart, touched_roots}`;
      `rebuildAndHotRestart` (fuzzer); `touchesE2EBinary` → `rebuildAndHotRestartE2E`; diff
      touches `supervisor/` → self-restart flow (below). Clear journal, write `result.json`.
   h. Rollback (any (f) failure) = `GitResetHard(last_good)` + `DeleteNewUntracked` + clear
      journal + result. The staged branch is never touched — debug there, restage.

Crash recovery (new preflight step, before the repo-state check): `merge-inflight.json` present →
`git reset --hard <journaled last_good>`, clear `claimed.json`, write `result.json`
`{id, result:"crash_recovered"}`, remove journal. Deterministic and retryable. Resume-mode
preflight (below) additionally re-scans the slot: unclaimed request whose `expect_staged_head`
still matches → service normally; mismatched → result `stale_request`.

Supervisor shutdown (deadline or INT/TERM) drains the slot: pending/unserviced request → result
`shutting_down`.

## Oracle binaries: copy from staged, verify by manifest

Assumption (per operator): oracles are only rebuilt manually. The C++ oracle builds are the only
expensive artifacts — Go binaries need no copying (`GOCACHE` is shared with the staged worktree,
so post-merge gate/rebuilds hit a warm cache when staged pre-built the same code).

- **Manifest**: `oracle/*/build.sh` gains a final step invoking
  `DDLFUZZ_ROOT=<this checkout> go run ./supervisor oracle-manifest --engine <e> --out
  build/oracle-<e>.manifest.json` (single implementation of the hash — in Go — so builder and
  verifier can't drift; `DDLFUZZ_ROOT` is pinned because ddlsuper's root discovery resolves any
  cwd under the main repo — including the staged worktree — to the main checkout; a missing
  manifest is treated as stale). Manifest:
  `{engine, source_hash, built_at, server_tag}`. `source_hash` = sha256 over
  `relpath \0 sha256(content) \n` for every regular file under `tools/ddlfuzz/oracle/proto/` and
  `tools/ddlfuzz/oracle/<engine>/`, sorted by relpath — worktree contents as-is (staged builds may
  be dirty), not git state.
- **Merge servicing**, when the merged diff touches `oracle/`: recompute `source_hash` over the
  post-merge campaign tree per engine touched; compare to `staged/tools/ddlfuzz/build/`'s
  manifests. Mismatch/missing → fail (`stale_oracle_binaries`, names the engine — rebuild in
  staged and retry). Match → copy binary+manifest into campaign `build/`, HELLO smoke each copied
  oracle, then `RunGolden` (behavioral authority; already contractual after oracle changes).
- Diff does NOT touch `oracle/` → copy nothing, even if staged has newer binaries. Binaries only
  change in lockstep with a merge that explains them.
- The fix-agent path (`RebuildOracles` in FixOnce validation) is unchanged — this replaces the
  rebuild only for merged work.

## Self-restart (default ON; `--no-restart` opts out per request)

Trigger: merge success where the diff touches `tools/ddlfuzz/supervisor/`. The running process
executes the old image; the new code takes effect only via handover. `request.json.restart`
(default true; `--no-restart` → false) selects the path.

Prerequisites (implemented regardless of path):
- `state/run-start` (RFC3339) written by preflight on normal start; deadline = run-start +
  `DDLFUZZ_HOURS` everywhere (today's in-memory start time becomes a fallback only). Resume never
  rewrites it.
- `ddlsuper selfcheck`: parses config, opens the state dir, prints build stamp
  (`debug.ReadBuildInfo` vcs revision), exits 0. Cheap "starts and can read the world" probe.

**Restart path** (`restart:true`), executed at the end of servicing step (g), in this exact order:

1. `go build -o build/ddlsuper.new ./supervisor`; run `ddlsuper.new selfcheck` (fail → the merge
   STANDS — it validated — but result carries `supervisor_restart:"required"` + detail, marker
   written as in the no-restart path; never roll back a validated merge for a restart problem).
2. `mv build/ddlsuper.new build/ddlsuper` (rename-safe against the running image on macOS).
3. Write `result.json` with `supervisor_restart:"restarting"` — the CLI returns BEFORE the
   handover gap, never hangs on it.
4. Write `state/selfrestart.json` `{validated_head, id, ts}`.
5. Cancel monitor/ticker contexts; SIGTERM fuzzer and lane (existing ≤60s flush handshakes);
   compose stays up; sync `supervisor.log`.
6. `syscall.Exec(<abs BuildDir>/ddlsuper, os.Args, env + DDLFUZZ_RESUME=1)` from the main
   goroutine. Same PID, same terminal/tmux pane. Go's O_CLOEXEC drops the pid flock — reacquired
   in resume preflight; the one-instance guarantee holds because the PID never stops existing.

**Resume-mode preflight** (`DDLFUZZ_RESUME=1` + `selfrestart.json` present): reacquire flock;
cheap probes only (tools, disk, oracle HELLO); iff `git rev-parse HEAD` == `validated_head`, skip
golden and gate-on-HEAD (they passed on this exact commit minutes ago) — mismatch → full
preflight; read `state/run-start`, recompute the original deadline; `docker compose ps` healthy or
`up -d --wait`; relaunch fuzzer + lane; re-scan the merge slot; delete `selfrestart.json`. Fix-loop
downtime: minutes.

**No-restart path** (`restart:false`, or selfcheck failed): write `state/RESTART_REQUIRED`
`{validated_head, ts}`; result carries `supervisor_restart:"required"`; `ddlsuper status` and the
hourly report show a standing banner. The old supervisor keeps running — it remains fully correct
against the merged tree, merely not-new. Next manual `ddlsuper run`: preflight sees the marker,
HEAD matches → same fast resume path, delete marker.

## Inline mode (no supervisor running)

`merge-staged` holding the `supervisor.pid` flock runs the pipeline itself: crash recovery first
(same journal logic), then quiescence (base = HEAD; warn if HEAD != `last_good_commit`; require
`parser-wip` + tracked-clean), ff-merge, full validation (gate / oracle handoff / replay-all),
bookkeeping (`last_good_commit`, baseline, `merges.jsonl`), rebuild `build/ddlfuzz`,
`build/ddlfuzz-e2e`, and — if `supervisor/` touched — `build/ddlsuper` (no exec: nothing is
running; the next `run` picks it up). No hold/ack, no slot files. Release the flock at exit.

## Fix-agent guardrails

`prompt.tmpl` never-touch list gains `tools/ddlfuzz/worktrees/` and `tools/ddlfuzz/staged/`
(codex has workspace-write over the whole repo; ignored paths are invisible to rollback, so a
stray write there would silently persist).

## Implementation steps

1. **Setup + housekeeping**: create the `staged/` worktree (`git worktree add -b ddlfuzz-staged
   tools/ddlfuzz/staged parser-wip`); `DeleteNewUntracked` excluded prefixes; `prompt.tmpl`
   guardrail line. (gitignore entries already exist.)
2. **`supervisor/mergeslot.go`** — slot types + state machine: O_EXCL create, phase transition,
   cancel/claim rename semantics, expiry, cleanup, id matching. Pure functions where possible
   (testability), one `MergeSlot` struct over `state/merge/`.
3. **`supervisor/manifest.go`** + `oracle-manifest` subcommand — hash spec above; wire the
   build.sh invocation into both `oracle/*/build.sh` scripts (warn-and-skip when ddlsuper absent).
4. **`supervisor/mergesvc.go`** — servicing: quiescence/staleness checks, journal, ff-merge,
   validation reusing `RunGate`/`RunGolden`/`preflightReplayAll`/`rebuildAndHotRestart`/
   `rebuildAndHotRestartE2E`, oracle handoff (manifest verify, copy, HELLO), bookkeeping, result
   writing, rollback. Shared by fix-loop servicing and inline mode.
5. **Fix-loop hook** (`fixloop.go`): tick-top slot scan/ack/claim/service + post-`FixOnce` check;
   attempt-start deferral while hold active; shutdown drain.
6. **`supervisor/selfrestart.go`** + `selfcheck` subcommand + `state/run-start` persistence +
   resume-mode preflight branch in `preflight.go` + `RESTART_REQUIRED` marker handling + crash
   recovery preflight step (journal).
7. **`supervisor/mergecli.go`** — `merge-staged` and `merge-cancel` subcommands per the CLI
   section; `status` merge-slot line; report `## Merges` section from `merges.jsonl`.
8. **Update `00-overview.md`**: components table row for 43, state-dir additions (`state/merge/`,
   `merges.jsonl`, `run-start`, `selfrestart.json`, `RESTART_REQUIRED`, `merge-inflight.json`,
   `build/oracle-*.manifest.json`).
9. Commit on `merge-staged` branch, message `ddlfuzz: merge-staged coordination + self-restart`,
   no co-author trailer. (Land into the campaign via this very mechanism once bootstrapped — see
   Risks for the bootstrap note.)

## Acceptance checks

1. Unit (`supervisor/`): slot state machine — second-waiter EEXIST; cancel-vs-claim race (both
   orders); keep-hold = unlink+recreate with new id, re-ack forced; expiry (dead pid, 6h
   backstop, never-after-claim); cleanup; id mismatch ignored.
2. Unit: manifest hash — deterministic, content-sensitive, relpath-sorted, dirty-tree files
   included; Go verifier matches a build.sh-produced manifest byte-for-byte.
3. Unit: servicing decision table — `git_drift`, `stale_request`, `deadline_too_close`,
   `not_ff`-as-internal, rollback on each validation failure, journal lifecycle.
4. Integration (no codex): fake finding + stubbed long attempt → hold ack deferred until attempt
   completes; no new attempt starts while hold active; fuzzer monitor untouched throughout.
5. E2E smoke (short `DDLFUZZ_HOURS`, planted-bug rig from 40's checks): trivial staged commit →
   `merge-staged` exits 0, `last_good_commit` advanced, fuzzer hot-restarted, `merges.jsonl`
   entry, slot empty.
6. Conflict path: staged conflicting with campaign HEAD → exit 2 at the dry-run, slot never
   created; conflict injected between dry-run and ack → merge --abort, hold released, exit 2,
   loop resumes attempts.
7. Oracle path: touch oracle source in staged without rebuilding → `stale_oracle_binaries`,
   rolled back; with rebuild → binaries+manifest copied, HELLO + golden ran.
8. **Self-restart smoke** (the gating check for the exec path): staged commit bumping a
   supervisor string → merge succeeds, same PID after exec, flock reacquired, `run-start`
   deadline preserved, fuzzer/lane relaunched, slot drained, `selfrestart.json` gone, resume
   preflight skipped gate+golden (log assertion). Run this before trusting `restart:true` at all.
9. `--no-restart`: `RESTART_REQUIRED` written, old supervisor keeps running, next `run`
   fast-resumes and clears the marker.
10. Cancel: Ctrl-C during hold → slot cleared, attempts resume next tick; cancel after claim →
    exit 7, result observed, "race-applied" reported.
11. Crash recovery: SIGKILL supervisor between ff-merge and bookkeeping → restart resets to
    journaled `last_good`, `crash_recovered` result, retry merges cleanly. Inline mode: same
    merge with supervisor stopped.

## Risks & fallbacks

- **`syscall.Exec` on darwin from a multithreaded Go process**: believed fine (execve replaces
  all threads; Go coordinates via BeforeExec), but macOS+Go exec has had historical quirks — this
  is exactly what acceptance check 8 exists for. If it misbehaves: flip the default to the
  `RESTART_REQUIRED` path (one-line config change); the wrapper-process design (exit-42 →
  relaunch, `.prev` fallback) is the documented upgrade if auto-restart must be unattended-safe.
- **Bad-but-gate-green supervisor payload wedges after exec**: irreducible — the risk is the
  payload, not the handover. Bounded by: selfcheck smoke, the operator being present (merge-staged
  is interactive by nature), and state-dir durability (recovery = revert + rebuild + `run`, state
  intact). `--no-restart` exists precisely for changes the operator doesn't want to bet the run on.
- **Post-result gap**: the CLI returns at step 3 of the restart path, ~2min before the loop is
  back. A new hold request created in the gap sits unread until resume preflight re-scans the
  slot — handled, just latent. Not a correctness issue.
- **Ack latency**: worst case one full attempt (~1.5–2h incl. gate). Inherent to "wait for a safe
  moment"; staging work proceeds in parallel and `--ack-timeout` bounds the wait.
- **Bootstrap**: the first landing of this component predates the mechanism itself. Land it
  between campaigns (inline mode also doesn't exist yet → plain commit on parser-wip while no
  supervisor runs), or stop the campaign at a natural boundary.
- **merge-tree false-clean**: `merge-tree --write-tree` can report clean where the real merge
  hits index/checkout oddities; step 4's real merge is the authority and aborts cleanly — the
  dry-run is an optimization, not a guarantee.

## Contract issues

1. State-dir additions: `state/merge/{request,ack,claimed,result}.json`, `state/merges.jsonl`,
   `state/merge-inflight.json`, `state/run-start`, `state/selfrestart.json`,
   `state/RESTART_REQUIRED`. Build-dir additions: `build/oracle-<engine>.manifest.json`,
   transient `build/ddlsuper.new`. All additive.
2. `oracle/*/build.sh` (components 10/11) gain the manifest step — additive, warn-and-skip when
   ddlsuper is absent, so standalone oracle builds still work.
3. 40's "fix loop = sole git authority" is preserved: merge servicing runs inside the fix-loop
   goroutine; inline mode holds the same flock the supervisor's single-instance lock uses.
4. 40's preflight gains: crash-recovery step (merge journal), `run-start` persistence, resume
   mode. `40-supervisor.md` should be annotated by the reconciler; semantics here are
   authoritative for the new paths.
5. New `ddlsuper` subcommands: `merge-staged`, `merge-cancel`, `oracle-manifest`, `selfcheck`.
