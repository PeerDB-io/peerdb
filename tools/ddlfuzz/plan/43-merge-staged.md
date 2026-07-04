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

`merge-staged` holding the `supervisor.pid` flock runs the pipeline itself: orphaned-child
cleanup first (the `state/children.json` scan from 40's process model — a dead supervisor may
have left a codex/fuzzer/e2e group alive; unkillable ⇒ escalation + exit 1, no merge), then crash
recovery (same journal logic), then quiescence (base = HEAD; warn if HEAD != `last_good_commit`; require
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

## Acceptance protocol (executable)

Run by an agent **between campaigns**: stop the supervisor first, run the whole protocol, restart
the campaign at the end. Checks that need a live supervisor start their own short runs against the
REAL state dir (real corpus/findings keep accruing — that is fine and intentional). Working
directory `/Users/ilia/Code/peerdb`; `DDL=tools/ddlfuzz`, `SUPER=$DDL/build/ddlsuper`. Budget
~3-5h wall: every successful merge runs the full gate (first ~30-45min warms the go cache,
subsequent ~5-15min); supervised checks pay one full preflight (~20-40min).

Any assert failing: stop, write `state/escalations/run-acceptance.md` with the check id and
evidence, run cleanup D, and do NOT resume the campaign until understood.

### P — preconditions (abort if any fails)

- P1. No supervisor: flock free (`$SUPER status` shows alive=false; `pgrep -f 'ddlsuper run'` empty).
- P2. Campaign tree on `parser-wip`, tracked-clean; this component already landed on `parser-wip`.
- P3. Oracle binaries healthy (`$SUPER hello` both engines) and manifests present — if
  `$DDL/build/oracle-*.manifest.json` missing, run both `oracle/*/build.sh` once (incremental).
- P4. `docker info` ok; codex authed (only the supervised runs' preflight needs it).
- P5. Snapshot for cleanup: `ORIG_HEAD=$(git rev-parse parser-wip)`;
  `ORIG_LAST_GOOD=$(cat $DDL/state/last_good_commit)`; note `$DDL/state/untracked.baseline` mtime.

### S — setup

- S1. `cd $DDL && go build -o build/ddlsuper ./supervisor`.
- S2. Staged worktree (first time): `git worktree add -b ddlfuzz-staged $DDL/staged parser-wip`
  (branch already exists → `git worktree add $DDL/staged ddlfuzz-staged`, then
  `git -C $DDL/staged merge parser-wip`).
- S3. Automated suite: `cd $DDL && go build ./... && go vet ./... && go test ./... &&
  golangci-lint run ./supervisor/...` — covers slot state machine, cancel/claim races, adoption,
  expiry, manifest hash, servicing decision table, run-start semantics, restart degradation,
  untracked-deletion policy.
- S4. `$SUPER selfcheck` exits 0 and prints the build stamp.

Throwaway staged commit helper (used repeatedly; empty commits merge cleanly and pass the gate):
`git -C $DDL/staged commit --allow-empty -m "ddlfuzz-acceptance: <label>"`.

### A — inline mode (no supervisor; each merged check costs one gate)

- A1 **happy path**: throwaway staged commit; `$SUPER merge-staged` → exit 0,
  `result=merged new_head=<staged head>`. Assert: `git rev-parse parser-wip` == staged head;
  `state/last_good_commit` == new head; last `state/merges.jsonl` line carries the id and heads;
  `state/merge/` empty; `build/ddlfuzz` mtime advanced.
- A2 **busy slot + adoption**: write `state/merge/request.json`
  `{"id":"acceptbusy0001","pid":1,"phase":"hold","created_at":"<now>"}` (pid 1 = always alive) →
  `$SUPER merge-staged` exits 6 "slot busy". Rewrite it with `"pid":999999` (dead) → throwaway
  staged commit → `merge-staged` exits 0 (adopted the abandoned hold instead of failing busy).
- A3 **conflict**: commit `acceptance-scratch.md` with line "left" directly on `parser-wip`;
  commit the same file with line "right" in staged → `merge-staged` exits 2 at the dry-run,
  printing the merge-tree failure; assert staged has no in-progress merge
  (`git -C $DDL/staged status` clean, no MERGE_HEAD) and `parser-wip` untouched. Then resolve by
  merging parser-wip into staged by hand (pick one side), `merge-staged` → exit 0.
- A4 **stale oracle**: staged commit appending a `// ddlfuzz-acceptance` comment line to a
  `tools/ddlfuzz/oracle/mysql/*.cc` file, WITHOUT rebuilding → `merge-staged` → exit 3,
  detail names the mysql manifest (missing/stale or hash mismatch). Assert: `parser-wip` back at
  its pre-merge head; `state/last_good_commit` unchanged; `state/merge-inflight.json` absent.
  (This check pays a full gate before failing — expected.)
- A5 **oracle handoff** (expensive, run once): in the staged worktree run
  `tools/ddlfuzz/oracle/mysql/build.sh` (builds into `staged/tools/ddlfuzz/build/` and writes the
  manifest via `go run` with `DDLFUZZ_ROOT` = staged checkout) → `merge-staged` → exit 0. Assert:
  campaign `build/oracle-mysql` and its manifest mtimes advanced; recomputed hash matches:
  `DDLFUZZ_ROOT=$PWD go run ./supervisor oracle-manifest --engine mysql --out /tmp/m.json` from
  `$DDL` equals `source_hash` in `build/oracle-mysql.manifest.json`; `build/ddlfuzz golden`
  exits 0.
- A6 **crash recovery**: create a throwaway commit directly on `parser-wip` (simulates a
  half-merged tree); write `state/merge-inflight.json`
  `{"id":"acceptcrash001","last_good":"<pre-commit head>","expect_staged_head":"x"}` and a
  matching `state/merge/claimed.json`. Throwaway staged commit → `merge-staged` → exit 0.
  Assert: the fake commit is NOT in `parser-wip` history (recovery reset it before merging);
  `merge-inflight.json` absent; final head == staged head.
- A7 **deadline guard** (cheap, no campaign): throwaway staged commit →
  `DDLFUZZ_HOURS=0.75 $SUPER merge-staged` → exit 2, `result=deadline_too_close`, tree untouched.
  Clean up the staged commit only if skipping straight to D (otherwise B1 merges it).
- A8 **Ctrl-C on inline** (interrupt safety): throwaway staged commit; run `merge-staged`, SIGINT
  it ~30s into the gate. Assert: `parser-wip` back at pre-merge head (rollback ran on the
  cancelled context), and EITHER `merge-inflight.json` absent OR present-and-recovered by the
  next `merge-staged` (A6 behavior). Slot empty afterward.

### B — supervised mode (one short campaign serves B and C)

Start in tmux: `cd $DDL && DDLFUZZ_HOURS=6 DDLFUZZ_MAX_TOKENS=1 ./build/ddlsuper run`; wait for
"preflight OK". `DDLFUZZ_MAX_TOKENS=1` keeps the codex fix loop from launching attempts against
real open findings while fuzzing and merge servicing run normally (it writes
`escalations/run-spend-cap.md` — delete at cleanup). Record `SUPER_PID=$(pgrep -f 'ddlsuper run')`
and the deadline shown by `$SUPER status`.

- B1 **hold/ack/merge**: throwaway staged commit; `$SUPER merge-staged` from a second shell →
  ack within ~2 ticks (≤90s; watch `state/merge/ack.json` appear), then exit 0 `result=merged`.
  Assert: `supervisor.log` has "servicing merge request" and the fuzzer hot-restart lines;
  `stats.json` `ts` resumes advancing ≤3min after the merge (fuzzer restarted, not left down);
  `state/merge/` empty.
- B2 **Ctrl-C during hold**: start `merge-staged` with no new staged commit, SIGINT it while
  "waiting for ack" (or just after ack, before it submits). Assert: exit 7, `state/merge/` has no
  request within 5s (CLI canceled it — not the supervisor expiry), fix loop resumes (next tick
  logs nothing about the dead request).
- B3 **busy**: `merge-staged` in shell A (holding), `merge-staged` in shell B → B exits 6 naming
  A's id/pid. SIGINT A.
- B4 **keep-hold + adoption, live**: `merge-staged` in shell A; once `ack.json` exists,
  `kill -9` A (dead owner, hold orphaned). `$SUPER merge-cancel --keep-hold` → prints new hold id.
  Throwaway staged commit; `merge-staged` → adopts the hold (no exit 6) and completes exit 0.
- B5 **merge-cancel semantics**: with the slot empty, `$SUPER merge-cancel` → "nothing to cancel",
  exit 0. Fabricate a live-pid hold (pid 1) → `merge-cancel` → "merge request canceled", exit 7,
  slot empty.
- B6 **shutdown drain**: fabricate a hold with pid 1 and id `acceptdrain001`; Ctrl-C the
  supervisor → clean exit; assert `state/merge/result.json` has
  `{"id":"acceptdrain001","result":"shutting_down"}`. Clear the slot. Restart the campaign
  (`DDLFUZZ_HOURS=6 DDLFUZZ_MAX_TOKENS=1 ./build/ddlsuper run` — full preflight) for C.

### C — self-restart (the gating checks for restart-by-default)

- C1 **exec handoff**: staged commit touching `tools/ddlfuzz/supervisor/` (append a comment line
  to `selfrestart.go`); `$SUPER merge-staged` → exit 0 with `supervisor_restart=restarting`.
  Within ~3min assert ALL of: `ps -p $SUPER_PID` still shows `ddlsuper run` (same PID across
  exec); `supervisor.log` has "exec handoff to rebuilt ddlsuper" followed by "resume preflight
  accepted validated head <new head>"; `$SUPER selfcheck` build stamp == new `parser-wip` head;
  `state/selfrestart.json` absent; `$SUPER status` deadline UNCHANGED from the value recorded at
  B start (run-start survived); `stats.json` advancing again (fuzzer relaunched); `state/merge/`
  empty. This check is the go/no-go for keeping restart-by-default: if exec misbehaves here, flip
  the default (see Risks) before any real campaign uses it.
- C2 **--no-restart**: another supervisor-touching staged commit;
  `$SUPER merge-staged --no-restart` → exit 0 with `supervisor_restart=required`. Assert:
  `state/RESTART_REQUIRED` exists with the new head; same supervisor PID, no exec lines in the
  log; `$SUPER status` shows the restart banner. Then Ctrl-C the supervisor and rerun
  (`DDLFUZZ_HOURS=6 DDLFUZZ_MAX_TOKENS=1 ./build/ddlsuper run`) → log shows "resume preflight
  accepted" (no golden/gate re-run — "preflight OK" within ~3min), `RESTART_REQUIRED` cleared.
- C3 **resume marker invalidation**: while the supervisor is STOPPED (Ctrl-C after C2), write a
  bogus `state/RESTART_REQUIRED` with `"validated_head":"0000000000000000"` → `ddlsuper run` →
  log shows "resume marker head mismatch ... full preflight required", marker deleted, full
  preflight proceeds. Ctrl-C once "preflight OK" appears.

### D — cleanup & campaign resume

- D1. Stop any test supervisor (Ctrl-C, wait for clean exit; verify flock free).
- D2. Drop acceptance history: `git -C /Users/ilia/Code/peerdb reset --hard <keep-head>` on
  `parser-wip` and `git -C $DDL/staged reset --hard <keep-head>` on `ddlfuzz-staged`, where
  keep-head discards only the `ddlfuzz-acceptance:` / scratch commits (empty commits carried no
  files; A3-A5 commits did — verify `git status` clean after reset). Never `rm -rf` worktree
  parents; the staged worktree stays.
- D3. Realign artifacts with the reset tree: rerun both `oracle/*/build.sh` manifest steps (or
  full scripts — incremental) so `build/oracle-*.manifest.json` matches the tree again;
  `echo "$(git rev-parse HEAD)" > state/last_good_commit` after confirming the gate is green on
  the reset head (or restore `$ORIG_LAST_GOOD` if the reset head equals `$ORIG_HEAD`).
- D4. Clear residue: `state/merge/` contents, `state/merge-inflight.json`, `state/RESTART_REQUIRED`,
  `state/selfrestart.json`, `state/run-start` (next campaign writes a fresh one),
  `state/escalations/run-spend-cap.md`, `/tmp/m.json`, `acceptance-scratch.md` if it survived.
  Leave `state/merges.jsonl` (append-only log; stale entries are harmless history).
- D5. Final verification: branch `parser-wip`, tracked-clean; `$SUPER status` alive=false;
  `build/ddlfuzz golden` exit 0; `$SUPER hello` both engines.
- D6. Resume the real campaign: `DDLFUZZ_HOURS=<n> ./build/ddlsuper run` (full preflight — no
  resume markers remain).

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
