# 40 — Supervisor & agentic fix loop (`tools/ddlfuzz/supervisor/`)

Component plan. Conforms to `tools/ddlfuzz/plan/00-overview.md` (binding). Deviations/extensions are
listed in **Contract issues** at the end. Executor: implement exactly what is written here; there are
no open design decisions.

## Goal

A single command that a human runs and walks away from for 72 hours:

```
cd /Users/ilia/Code/peerdb/tools/ddlfuzz
go build -o build/ddlsuper ./supervisor && ./build/ddlsuper run
```

It preflights (repo state, tools, disk, builds, oracle HELLO, codex auth, golden, gate on HEAD,
e2e stack), then supervises the fast-lane fuzzer, the e2e lane, and a serialized `codex exec` fix
loop; it never stops the run and never waits for a human. Every failure path ends in "record +
continue". At the deadline (or on INT/TERM) it shuts everything down gracefully and writes a final
report. Sleep prevention is out of scope: the host runs Amphetamine.

Language decision: the supervisor is a **single Go binary** (`ddlsuper`) in the existing
`tools/ddlfuzz` module — no shell wrapper, preflight included. Rationale: process-group kill,
per-attempt wall timeouts, JSON parsing (stats, codex JSONL), atomic file writes, and concurrent
component monitors are all fragile in macOS bash 3.2 (no `setsid`, no `timeout`, no associative
arrays); the Go module already exists and component 20 is Go; one binary removes a moving part.

## Interfaces

### Consumed (imposed on components 10/11/20/30 — reconciler: verify these appear in their plans)

**From 10/11 (oracles):**
- C1. Oracle binaries at `tools/ddlfuzz/build/oracle-mysql` and `tools/ddlfuzz/build/oracle-mariadb`,
  built by `tools/ddlfuzz/oracle/mysql/build.sh` and `tools/ddlfuzz/oracle/mariadb/build.sh`
  (idempotent, exit 0 on success). The supervisor invokes these scripts when a fix attempt touches
  `tools/ddlfuzz/oracle/`.
- C2. HELLO smoke: supervisor speaks protocol v1 msg 3 directly (spawn binary, write HELLO frame,
  read response, expect `protocol:1` and the right `engine`, kill). ~40 LOC using the overview's
  frame spec; no dependency on 20's client package.

**From 20 (fuzzer/driver):**
- C3. `build/ddlfuzz fuzz --state tools/ddlfuzz/state` — long-running; owns the oracle process pool
  (spawn/respawn/bisect — the supervisor NEVER manages oracle processes after preflight, only their
  binaries). On SIGTERM: stop generating, flush corpus/coverage/findings to the state dir, exit 0
  within 60s (supervisor SIGKILLs the process group at 60s). All state-dir writes are atomic
  (temp file + rename) so a SIGKILL never leaves torn files.
- C4. `build/ddlfuzz golden` — exit 0 iff golden validation passes; nonzero otherwise; human-readable
  mismatches on stdout.
- C5. `build/ddlfuzz replay <sig>` — reads `state/findings/<sig>/{repro.sql,meta.json}`, replays
  through the current parser build and live oracles. Exit codes (00-overview Reconciliation
  decision D5, authoritative): `0` reconciled, `10` still diverges — **including panic/hang**,
  which are divergences carried in the JSON `class` field, not a separate code — `11` finding
  malformed/unreadable or its engine oracle won't start, `1` internal error. Stdout: one JSON
  object per 20's `replay <sig>` shape (`{sig, engine, sql_mode, our_sig, our_error,
  oracle_digest, reconciled, class, shape}`). (Earlier drafts of this doc mapped 11 to
  panic/hang; superseded by D5 — the supervisor treats exit 10 with `class∈{panic,timeout}` as a
  non-regression divergence during preflight, and exit 11 strictly as "cannot evaluate".)
- C6. `state/stats.json` written by the fuzz process every 30s (atomic rename), schema:

  ```jsonc
  {"ts":"2026-07-03T12:00:00Z",          // RFC3339; staleness > 5min ⇒ supervisor treats fuzzer as wedged
   "execs_total": 123456789,
   "execs_per_sec": 41000,
   "corpus_count": {"mysql": 8000, "mariadb": 7000},
   "edges": {"go": 4100, "mysql": 91000, "mariadb": 87000},   // monotone (OR-accumulated bitmaps)
   "oracle_restarts": {"mysql": 2, "mariadb": 0},
   "findings_emitted_total": 17}
  ```
- C7. Seed pickup: on startup, `ddlfuzz fuzz` loads committed seeds from `tools/ddlfuzz/seeds/`
  (`<name>.sql` + `<name>.meta.json` sidecar `{sql_mode, engine}`). Fix agents commit each fixed
  repro there.
- C8. Dedup layer suppresses sigs in `state/parked.list` (already in overview) AND does not re-open
  findings whose meta status is `fixed`/`ledgered` when re-discovered with the same sig (re-discovery
  of a `fixed` sig ⇒ 20 flips it back to `open` and bumps `rediscovered_count` in meta — the
  supervisor's flap detector consumes this).

**From 30 (e2e lane):**
- C9. Compose stack: `docker compose -f tools/ddlfuzz/e2e/compose.yml up -d --wait` / `down`.
  Supervisor owns up/down; 30 owns what's inside.
- C10. Lane process: `build/ddlfuzz-e2e --state tools/ddlfuzz/state` (built via
  `go build -o build/ddlfuzz-e2e ./cmd/ddlfuzz-e2e` from `tools/ddlfuzz/` — path per
  00-overview Reconciliation decision D6); runs until SIGTERM (clean exit ≤ 60s); writes
  `state/e2e-stats.json` (D6; 30's per-engine schema) every 10s. The supervisor treats it as an
  opaque heartbeat: it reads `updated_at` for staleness and sums per-engine counters for the
  report; it does not depend on exact field names beyond `updated_at`.
- C11. Confirmation queue (paths/lifecycle per 30-e2e-lane.md §8, authoritative — D6): supervisor
  drops `state/e2e-queue/pending/<sig>.json`
  (`{"sig","engine","sql_mode","sql_mode_name","session_sql_mode","statement"}`) after each
  parser fix; the lane
  claims via rename to `processing/`, replays through live binlog, writes
  `state/e2e-queue/done/<sig>.json` (`{"sig","result":"confirmed-fixed"|"still-diverges"|"exec-reject","details"}`).
  A `still-diverges` result is emitted by 30 as a NEW e2e-lane finding (normal path); `exec-reject`
  means live can't validate and the supervisor relies on fast-lane replay alone. Not handled
  specially otherwise.

**From the environment:** `codex` CLI ≥ 0.142 on PATH, authenticated (verified 0.142.5 locally),
`docker`, `jq`, `golangci-lint`.

### Provided

- `tools/ddlfuzz/build/ddlsuper` — supervisor binary and the single entry point
  (`run` = preflight + supervise, plus `hello`, `gate`, `fix-once`, `status`).
- `tools/ddlfuzz/supervisor/prompt.tmpl` — fix-agent prompt template.
- State-dir additions (extensions to the overview layout — see Contract issues #1):
  `state/stats.json` (20), `state/e2e-stats.json` (30), `state/e2e-queue/{pending,processing,done}/` (30),
  `state/groups.json`, `state/last_good_commit`, `state/untracked.baseline`, `state/BLOCKED`,
  `state/children.json`, `state/coverage/history/edges.csv`, `state/attempts/<sig>.attempt<N>.stream.jsonl`,
  `.attempt<N>.last.txt` and `.attempt<N>.diff` (supervisor), `state/supervisor.log`,
  `state/spend.json`.
- `state/report.md` heartbeat + final report (contractual location).

### codex CLI research results (codex-cli 0.142.5, verified locally by smoke runs; binding)

- Headless: `codex exec` — prompt read from **stdin** when the prompt arg is `-` (or absent);
  verified. `-C <dir>` sets the working root; no shell-level `cd` needed.
- Machine-readable output: `--json` prints JSONL **events** to stdout (verified stream:
  `thread.started`, `turn.started`, `item.completed` with
  `item:{type:"agent_message"|"command_execution"|"error"|...}`, `turn.completed`).
  `-o/--output-last-message <FILE>` writes the agent's final message to a file — the supervisor
  parses the `RESULT:` line from that file (fallback: last `agent_message` item in the JSONL).
- **Usage IS reported, cost is NOT**: each `turn.completed` event carries
  `usage:{input_tokens,cached_input_tokens,output_tokens,reasoning_output_tokens}` (verified).
  There is no dollar figure and **no budget/turn-limit flag** — spend accounting is honestly
  downgraded to token counts + attempt counts + wall time; the only hard per-attempt bound is the
  supervisor's 45-min process-group kill.
- Sandbox/approvals for unattended full-auto: `-s workspace-write` (writable: workspace root set by
  `-C`, `--add-dir` extras, `$TMPDIR`; full-disk read) + `-c approval_policy=never` (verified
  accepted; header shows `approval: never`). This is *stronger* than post-hoc guardrails: writes
  outside the repo are OS-blocked. Because Go/lint caches live outside the repo, the supervisor
  passes them as writable: `--add-dir "$(go env GOCACHE)" --add-dir "$(go env GOMODCACHE)"
  --add-dir ~/Library/Caches/golangci-lint` (queried once at startup). Network access for
  citation lookups: `-c sandbox_workspace_write.network_access=true`.
  `--dangerously-bypass-approvals-and-sandbox` exists as a fallback knob only (see Risks).
- Isolation: `--ignore-user-config` (skips `~/.codex/config.toml` — verified: removes the user's
  feature flags/MCP servers; auth still resolves via `CODEX_HOME`), `--ignore-rules` (no user or
  project execpolicy `.rules`). Sessions are persisted normally under `~/.codex` — deliberate
  (`--ephemeral` exists but is NOT used): attempt sessions stay resumable for post-run inspection
  via `codex exec resume`; our own transcript capture remains the primary record. There is no
  fully hermetic mode; `~/.codex` writes remain (Contract issues #8).
- Model: `-m "$DDLFUZZ_FIX_MODEL"`; with user config ignored the default resolved to `gpt-5.5`
  with `reasoning effort: none` (verified header) — the supervisor always pins
  `-c model_reasoning_effort=xhigh` (fixed, not configurable).
- Timeout/kill: codex has no timeout flag; `ddlsuper` enforces 45 min via SIGKILL on the process
  group (codex spawns tool subprocesses; pgroup kill reaps them).

Full invocation (executed by `ddlsuper`, its own process group, 45-min context timeout;
`$ROOT` = `/Users/ilia/Code/peerdb`):

```
codex exec -C "$ROOT" \
  -s workspace-write -c approval_policy=never \
  -c sandbox_workspace_write.network_access=true \
  --add-dir "$GOCACHE" --add-dir "$GOMODCACHE" --add-dir "$HOME/Library/Caches/golangci-lint" \
  --ignore-user-config --ignore-rules --skip-git-repo-check \
  -m "$DDLFUZZ_FIX_MODEL" -c model_reasoning_effort=xhigh \
  --json -o state/attempts/<sig>.attempt<N>.last.txt \
  - < rendered-prompt.txt > state/attempts/<sig>.attempt<N>.stream.jsonl 2> .stderr
```

Defaults: `DDLFUZZ_FIX_MODEL=gpt-5.5` (reasoning effort is always `xhigh`),
`DDLFUZZ_MAX_TOKENS=0` (run-wide cap on summed input+output tokens; `0` = unlimited; on exceed:
stop launching attempts, keep fuzzing, write `escalations/run-spend-cap.md`, continue),
`DDLFUZZ_ATTEMPT_TIMEOUT=45m` (the hard per-attempt bound).

## Design

### Ownership boundaries

| Thing | Owner | Supervisor's role |
|---|---|---|
| Oracle **processes** (pool, respawn, crash bisect) | 20 (fuzz process) | none after preflight |
| Oracle **binaries** (build/rebuild) | 40 | rebuild via build.sh when oracle sources change in a fix |
| `ddlfuzz fuzz` process | 40 | start/restart/SIGTERM-handshake/backoff |
| e2e compose stack | 40 (up/down) / 30 (contents) | health = `docker compose ps` all running |
| `ddlfuzz-e2e` lane process | 40 | start/restart/backoff |
| Fix agents | 40 | spawn serialized, timeout, validate, rollback |
| git / last_good_commit / rebuilds | 40 | sole authority |
| findings meta `status` field | 40 is authoritative | agent writes it, supervisor verifies/corrects |

### Process model (`ddlsuper run`)

Goroutines:
1. **fuzzer monitor** — runs `build/ddlfuzz fuzz`; restart on exit with backoff 5s→15s→45s→135s→300s
   (cap); breaker: ≥6 exits within 30min ⇒ write `escalations/run-fuzzer-crashloop.md`, keep
   retrying every 30min (never give up entirely — a later fix commit may repair it), mark degraded
   in report. Wedge detection: `stats.json` `ts` stale > 5min ⇒ SIGTERM, 60s, SIGKILL pgroup,
   restart (counts toward breaker).
2. **e2e monitor** — compose up + lane process; same backoff/breaker per sub-part; wedge
   detection per 30 §9 (`e2e-stats.json` `updated_at` stale > 60s after a 90s startup grace, or
   `matcher_last_event_at` lag > 300s while cases advance); breaker trip ⇒
   `escalations/run-e2e-crashloop.md`, compose down, retry every 30min. e2e death never affects
   the fast lane.
3. **fix loop** — serialized, one attempt at a time (below).
4. **tickers** — findings scan 30s; heartbeat report hourly; disk watchdog 10min; coverage history
   hourly; deadline clock 1min.

All child processes are started with `Setpgid: true`; kill is always `syscall.Kill(-pgid, sig)`.
Every spawned child is also recorded in `state/children.json` (pid, pgid, leader start time,
command; atomic rewrite; entry removed when the child is reaped, before `Done()` fires). Only the
`supervisor.pid` flock holder writes the registry (`fix-once` holds no flock and does not track);
`run` startup and the inline merge CLI scan it to kill process groups orphaned by a dead
supervisor before touching git. Identity is pgid liveness (POSIX forbids reusing a pid while it
names a live process group; a leader start-time mismatch against the recorded value is an anomaly
and blocks). Kill = SIGTERM to every matched group up front, ≤60s drain each, then SIGKILL, ≤30s;
a group that survives (and is not all-zombie) writes `escalations/run-orphans.md` and blocks the
run rather than proceeding. The self-restart exec needs no handoff marker: children are stopped
and deregistered before `syscall.Exec`, so the resumed image's scan is a no-op. A crash between
spawn and the registry write can leave one untracked orphan (window ~ms; accepted).
Supervisor logs to `state/supervisor.log` (append, line-buffered, with timestamps). No sleep
prevention: the host runs Amphetamine (out of scope); the supervisor tolerates wall-clock jumps
(deadline math uses absolute time, monitors are stateless per tick).

Deadline: `start + $DDLFUZZ_HOURS` (default 72). At `deadline − 3h` stop launching new fix
attempts (an in-flight attempt may finish + gate). At deadline: SIGTERM fix agent if any (record
attempt as `timeout`), SIGTERM fuzzer (flush handshake), SIGTERM lane, compose down, write final
report, exit 0. INT/TERM to the supervisor triggers the same path immediately.

Disk watchdog (state volume free space via statfs + `docker system df`): `<10GiB` ⇒ compose down +
kill lane (e2e is the big consumer), `escalations/run-disk-low.md`, continue fast lane; re-enable
e2e if free rises back above 20GiB. `<3GiB` ⇒ pause fix-agent launches until >5GiB. Never delete
corpus/findings/coverage.

### Startup sequence (inside `ddlsuper run`; all preflight is here — synchronous, before any
supervision, while the human is still at the keyboard)

Any preflight failure: print the reason, write `state/BLOCKED` (reason + failing command output),
exit 2 — the run never begins. `rm -f state/BLOCKED` on entry.

1. Acquire lock `state/supervisor.pid` (flock; refuse to double-run), then kill children orphaned
   by a previous supervisor via the `state/children.json` scan (see Process model; unkillable ⇒
   `escalations/run-orphans.md` + blocked).
2. Repo state: branch is `parser-wip`; tracked tree clean (`git status --porcelain` minus `??`
   lines is empty); record `state/untracked.baseline` (the `??` set).
3. Tools present: `codex`, `docker` (+ `docker info` succeeds), `jq`, `golangci-lint`, `go` on
   PATH. Resolve and cache `go env GOCACHE GOMODCACHE` for the codex `--add-dir` flags.
4. Disk: ≥ 40 GiB free on the state volume.
5. Builds: `go build -o build/ddlfuzz ./cmd/ddlfuzz` and `go build -o
   build/ddlfuzz-e2e ./cmd/ddlfuzz-e2e` (from `tools/ddlfuzz/`); oracle binaries: if
   `build/oracle-{mysql,mariadb}`
   missing, run the respective `oracle/*/build.sh`.
6. Oracle HELLO smoke on both binaries (protocol v1 msg 3, 10s timeout each).
7. codex auth smoke (cheap, bounded 120s): run the exact fix-agent invocation shape with
   `-s read-only` and prompt `Reply with the single word ok`; require exit 0 and `ok` in the
   `--output-last-message` file. Validates auth + flag compatibility before walking away.
8. **Golden — hard block at t=0**: `build/ddlfuzz golden` must exit 0. Failure here parks the whole
   run in the blocked state (this is the "fix oracles/expectations before starting" gate).
9. Gate green on HEAD (the full verification gate below); then `state/last_good_commit` ←
   `git rev-parse HEAD`.
10. e2e stack: `docker compose -f e2e/compose.yml up -d --wait`.
11. Print "preflight OK — supervising until deadline (${DDLFUZZ_HOURS}h); Ctrl-C = graceful
    shutdown; status: build/ddlsuper status. Tip: run inside tmux." Start fuzzer monitor, e2e
    monitor, tickers, fix loop.

Mid-run golden failures can only occur inside a fix attempt's gate (after an oracle change) ⇒
that's a gate failure of the attempt (rollback + record), never a run stop.

### Fix cycle (serialized; the fuzzer keeps running throughout — it executes the previously BUILT
binary, so working-tree edits are invisible to it until the supervisor rebuilds post-commit)

**Selection.** Every 30s scan `state/findings/*/meta.json` for `status:"open"`, excluding: sigs in
`parked.list`, sigs whose attempts file shows budget exhaustion (≥3 attempts or ≥2.5h cumulative
wall), sigs in parked groups. Sort by `discovered_at`. Group key:

```
group_key = sha256_12( class + "|" + shape )
class ∈ {panic, hang, error-diverge, sig-diverge}    // from meta: our_error/panic markers vs oracle verdict
shape = normalize(our_error)            if our_error != ""      // error-diverge, panic
      = normalize(firstDiffLine(our_sig, oracleSig(digest)))    // sig-diverge
normalize = lowercase; `...`/'...'/"..." contents → <id>; runs of digits → <n>; hex sigs → <sig>
```

Pick the oldest group; within it the oldest sig is the **primary**; up to 8 sibling sigs ride along
in the prompt as context. One fix often closes the whole group via the post-fix all-findings replay
(any open finding that now replays clean is auto-marked `fixed`, `fixed_by:<primary sig>`).

**Attempt.** Sequence (each step logged to `state/attempts/<sig>.jsonl` as it happens):
1. Pre-attempt git check: HEAD == `last_good_commit` and tracked tree clean and untracked == baseline
   ∪ known-supervisor-files. If not: run cleanup (below); if still dirty ⇒
   `escalations/run-git-drift.md`, pause fix loop (fuzzer unaffected), retry check every 10min.
2. Render `prompt.tmpl` (placeholders below), spawn `codex exec` as specified above, own pgroup,
   45-min timeout ⇒ SIGKILL pgroup, outcome `timeout`.
3. Parse results: `RESULT: (fixed|ledgered|failed) <sig>` line from the `--output-last-message`
   file (fallback: last `agent_message` item in the JSONL stream); token usage summed over all
   `turn.completed` events; nonzero codex exit or an `item.type=="error"` terminal event ⇒ treat
   as agent failure. No RESULT line ⇒ outcome `failed`.
4. **Independent validation** (never trust the agent) — run regardless of the agent's claim if the
   tree/HEAD changed; skip to rollback if the agent claimed `failed`:
   a. Save `git diff <last_good>..HEAD > state/attempts/<sig>.attempt<N>.diff` (before any rollback).
   b. Commit sanity: 1–3 new commits, none touching forbidden paths (`state/parked.list`,
      `.github/`, other findings' meta with status flips away from open, the three unrelated
      in-flight files from the writeup); both `flow/` and `tools/ddlfuzz/oracle/` touched in the
      same attempt ⇒ fail.
   c. Run the **gate** (below), `nice -n 10` so the fuzzer keeps its cores.
   d. `build/ddlfuzz replay <sig>`: must exit 0, OR the agent ledgered it (meta `ledgered` + a new
      `ledger.jsonl` line for this sig **with a non-empty citation field**; exit 10 is then OK).
   e. If the diff touches `tools/ddlfuzz/oracle/`: run `oracle/*/build.sh` for the affected engine(s),
      then `build/ddlfuzz golden` — must exit 0.
   f. Preflight-replay ALL findings: `fixed` must exit 0; `ledgered`/`parked` may exit 10;
      any `fixed` regressing, or any exit 11/1 ⇒ fail.
5. **On success:** `last_good_commit` ← new HEAD; supervisor (authoritatively) sets meta status
   (`fixed`/`ledgered`) on the primary and on every open finding that replayed clean in 4f
   (`fixed_by`); rebuild `build/ddlfuzz` to `build/ddlfuzz.new`
   (`go build -o ...` — the driver binary; the flow shim is compiled
   into it via the replace directive) and, if oracle sources changed, the oracle binaries are
   already rebuilt in 4e; **hot-restart handshake**: SIGTERM fuzzer → wait ≤60s for exit (SIGKILL
   pgroup at 60s) → `mv build/ddlfuzz.new build/ddlfuzz` → relaunch (fuzzer reloads retained
   corpus/coverage from state dir per C3); for parser fixes, enqueue
   `state/e2e-queue/pending/<sig>.json` (C11); append attempt record (outcome `fixed`/`ledgered`).
6. **On failure** (timeout / no-RESULT / gate / replay / golden / regression / forbidden-paths):
   rollback = `git reset --hard <last_good_commit>` (chosen over `git revert`: the loop is
   serialized so all commits past last_good belong to this attempt, the branch is local during the
   run, and reset leaves no noise commits; revert would accumulate junk history and can conflict),
   then delete untracked files that appeared during the attempt (current `git status --porcelain`
   `??` set minus `state/untracked.baseline`), **restricted to** `flow/` and `tools/ddlfuzz/`
   excluding `tools/ddlfuzz/state/` and `tools/ddlfuzz/build/`; new untracked files outside those
   roots are left alone + noted in the attempt record. Append attempt record with outcome +
   failure detail. Continue.
7. Budget check: if this was attempt 3 or cumulative wall ≥2.5h ⇒ **park** (below).

**Gate** (verbatim from the overview; each step with its own timeout, total cap 45min, all
`nice -n 10`):

```
cd /Users/ilia/Code/peerdb/flow
go build ./...                                                                  # 10m
go vet ./connectors/mysql/                                                      # 5m
go test ./connectors/mysql/ -run 'TestDDL|TestProcessRenameTableQueryMetric|TestClassifyOnlineSchemaMigrationTool' -count=1   # 10m
golangci-lint run ./connectors/mysql/...                                        # 10m
go build ./...                                                                  # 10m
cd /Users/ilia/Code/peerdb/tools/ddlfuzz
go test ./...                                                                   # 15m
```

### Parking & escalation (never stop)

Per-sig budget: 3 attempts or 2.5h cumulative attempt wall-time. On exhaustion:
1. Write `state/escalations/<sig>.md` (template below), from the attempts jsonl + saved diffs.
2. Append sig to `state/parked.list` (idempotent), set meta `status:"parked"`.
3. Tree is already at `last_good_commit` (rollback ran per-attempt). Continue.

`parked.list` persists across runs and is suppressed at 20's dedup layer (C8) and skipped by
selection — future runs never re-burn time on it.

**Flap detector:** `state/groups.json` = `{group_key: {sigs:[], fix_count:int, last_fix_ts}}`,
updated on every successful fix (fix_count++) and every findings scan. If a group with
`fix_count ≥ 2` gains a new open sig (or a `fixed` sig re-opens via C8 rediscovery), the group is
**parked**: every open sig in it goes through steps 1–2 above, and the group_key is recorded in
`groups.json` as `parked:true` so future sigs mapping to it are parked on arrival (escalation file
per sig, one shared "flapping group" header).

Escalation file template (`state/escalations/<sig>.md`):

```markdown
# Escalation <sig>            status: parked   parked_at: <ts>
group: <group_key> (class=<class>, shape=<shape>)   flap: <yes/no>
## Finding
- repro: findings/<sig>/repro.sql  (engine=<e>, sql_mode=<m>, lane=<lane>)
- our signature: <our_sig>   our error: <our_error>
- oracle digest: <oracle_digest json>
## Attempts
### Attempt <N>  (<start>–<end>, <outcome>, <in/out tokens>)
- diagnosis (last assistant messages, extracted from attempt transcript): <...>
- diff tried: attempts/<sig>.attempt<N>.diff (<+X/−Y lines over: file list>)
- gate/validation failure: <detail>
## Siblings
<sig list of same group>
```

Diagnosis extraction: `jq -r 'select(.type=="item.completed" and .item.type=="agent_message") | .item.text'`
over the attempt stream-jsonl (codex `--json` events), last 40 lines kept.

### Fix-agent prompt template (`tools/ddlfuzz/supervisor/prompt.tmpl`)

The shipped `supervisor/prompt.tmpl` is authoritative; the copy below is illustrative.

Placeholders `{NAME}` are substituted by `ddlsuper` with plain string replacement. `{SIBLINGS}` is
"none" or a bulleted list of `sig — class/shape — repro path`. `{PRIOR_ATTEMPTS_SUMMARY}` is "none"
or per-attempt `outcome + diagnosis excerpt + files touched`.

```text
You are an automated fix agent inside a 72-hour UNATTENDED differential fuzzing run against
PeerDB's hand-rolled MySQL/MariaDB DDL parser. You run headless; nobody will answer questions.
You MUST end your work by printing a RESULT line (exact format at the bottom). If you cannot
finish for any reason, print `RESULT: failed {SIG} <one-line reason>` — never end silently.
Your work is independently re-verified and rolled back if it does not hold up, so do not
overstate results.

Working directory: /Users/ilia/Code/peerdb (branch parser-wip). State dir:
/Users/ilia/Code/peerdb/tools/ddlfuzz/state (paths below relative to the repo root).

READ FIRST (authoritative context):
- docs/mysql-ddl-parser-writeup.md — the parser under test, its design and safety property.
- tools/ddlfuzz/plan/00-overview.md — harness contracts: signature grammar, oracle digest schema,
  correctness contract, verification gate.

THE FINDING (attempt {ATTEMPT_N} of 3 for this signature):
- sig: {SIG}
- repro bytes: tools/ddlfuzz/state/findings/{SIG}/repro.sql (engine, sql_mode, lane in meta)
- meta.json contents:
{META_JSON}
- our signature: see meta `our_sig`; our error: see meta `our_error`; the oracle's digest: see
  meta `oracle_digest`. A divergence means: the real server accepted this statement and derived
  the digest above, while our parser produced a different signature, an error, or a panic/hang.
- Sibling findings in the same divergence group (very likely the same root cause; your fix should
  close them too, but you only need to verify {SIG}):
{SIBLINGS}
- Prior attempts on this sig — do NOT repeat these approaches:
{PRIOR_ATTEMPTS_SUMMARY}

PROTOCOL — follow strictly, in order:

(a) REPRODUCE FIRST. Run:
      tools/ddlfuzz/build/ddlfuzz replay {SIG}
    Exit 0 = matches (does NOT reproduce): print `RESULT: failed {SIG} did-not-reproduce` and stop.
    Exit 10 = signature divergence, 11 = panic/hang, 1 = harness error. You may additionally write
    a one-off Go test in flow/connectors/mysql/ calling parseQueryEvent with the repro bytes and
    meta's sql_mode/engine to inspect the AST directly (delete the one-off before committing, or
    turn it into the regression test of step b1).

(b) ROOT-CAUSE into EXACTLY ONE bucket, then act:
    1. PARSER/LEXER BUG (our code wrong) →
       - Fix in flow/connectors/mysql/ (ddl_parser.go / ddl_lexer.go / related). Respect the
         parser's safety property (writeup): never make an actionable statement silently benign.
       - Append a regression case to the appropriate existing test file
         (flow/connectors/mysql/ddl_parser_*_test.go or ddl_lexer_test.go — match the file whose
         suite covers the construct; follow existing table-test style).
       - Add the repro as a committed corpus seed: tools/ddlfuzz/seeds/{SIG}.sql (exact repro
         bytes) + tools/ddlfuzz/seeds/{SIG}.meta.json  ({"sql_mode":<u64>,"engine":"mysql"|"mariadb"}).
    2. ORACLE-HARNESS BUG (our C++ oracle digest/driver wrong, parser fine) →
       - Fix under tools/ddlfuzz/oracle/ only. Rebuild: tools/ddlfuzz/oracle/mysql/build.sh and/or
         tools/ddlfuzz/oracle/mariadb/build.sh. Then re-run golden validation (MANDATORY):
         tools/ddlfuzz/build/ddlfuzz golden   — must exit 0.
    3. GENUINE ENGINE DIVERGENCE (both our parser and the oracle are faithful; the comparison
       expectation itself is wrong per real server behavior) →
       - Append ONE line to tools/ddlfuzz/state/ledger.jsonl:
         {"sig":"{SIG}","engine":"...","statement":"<repro, escaped>","reason":"<why this is
          legitimate engine behavior, 1-3 sentences>","citation":"<exact public-doc URL, e.g.
          https://dev.mysql.com/doc/refman/9.7/en/... or https://mariadb.com/kb/en/..., OR a
          server-source location like ~/Code/mysql-server/sql/sql_yacc.yy:9126>","date":"<ISO>"}
         The citation is REQUIRED and must actually support the claim. No citation = your attempt
         will be reverted.

(c) VERIFY — the full gate, all must pass (run it yourself; the supervisor re-runs it after you):
      cd flow && go build ./... && go vet ./connectors/mysql/ \
        && go test ./connectors/mysql/ -run 'TestDDL|TestProcessRenameTableQueryMetric|TestClassifyOnlineSchemaMigrationTool' -count=1 \
        && golangci-lint run ./connectors/mysql/... \
        && go build ./...
      cd ../tools/ddlfuzz && go test ./...
    And: tools/ddlfuzz/build/ddlfuzz replay {SIG} must now exit 0 (buckets 1/2). For bucket 3 it
    may still exit 10 — the ledger entry is the resolution.

(d) Update tools/ddlfuzz/state/findings/{SIG}/meta.json: set "status" to "fixed" (buckets 1/2) or
    "ledgered" (bucket 3). Touch no other finding's meta.

(e) COMMIT on branch parser-wip (you are already on it — verify with git branch --show-current):
      git add <exact files you touched> && git commit -m "ddlfuzz: fix {SIG} — <one-line summary>"
    ONE commit. NO Co-Authored-By trailer, no AI-attribution trailer of any kind, no push, no
    PRs. Note:
    tools/ddlfuzz/state/ and build/ are gitignored — meta/ledger edits won't (and must not) be in
    the commit; seeds/ and source/test edits will.

(f) Print, as the LAST line of your final message, exactly one of:
      RESULT: fixed {SIG}
      RESULT: ledgered {SIG}
      RESULT: failed {SIG} <one-line reason>

GUARDRAILS (violations are detected and your commit is reverted):
- Never modify BOTH flow/ (parser) and tools/ddlfuzz/oracle/ in the same attempt. If you believe
  both are wrong, fix the one you are most sure of and print RESULT accordingly; the other side
  will resurface as its own finding.
- Never run `go test ./connectors/mysql/` without the -run filter above — the full package needs a
  live database and will hang or fail. Bound ad-hoc test runs with -timeout 60s (the repro may hang).
- Never touch: tools/ddlfuzz/state/parked.list, state/stats.json, state/e2e/**, other findings'
  directories, anything under .github/, or anything outside /Users/ilia/Code/peerdb.
- Never touch these unrelated in-flight files: docs/mysql-clickhouse-charset-e2e.md,
  docs/temporal-history.md, flow/e2e/clickhouse_mysql_charset_test.go.
- Never delete or weaken an existing test/assertion to make the gate pass. Never ledger away a
  real parser bug: bucket 3 requires the citation to genuinely establish the server behavior.
- No git operations besides add/commit on the current branch: no push, no rebase, no branch
  switching, no reset of commits that predate your attempt, no PR/issue interactions.
- Do not remove existing code comments; keep the codebase's comment style (self-documenting code,
  no narration comments).
- Budget: you have a hard wall-clock limit of 45 minutes (the supervisor kills your process
  group at the deadline — there is no other bound, so pace yourself); work efficiently:
  reproduce, read the relevant parser/lexer region, fix minimally, test, commit, RESULT.
```

### Observability

Hourly heartbeat rewrites `state/report.md` in full:

```markdown
# ddlfuzz run report        started: <ts>   deadline: <ts>   now: <ts>   status: running|degraded|final
## Fast lane   (from state/stats.json; stale>5min flagged)
execs/s, execs_total, corpus counts, edges {go,mysql,mariadb}, edges Δ last hour, oracle restarts
## E2E lane    (from state/e2e-stats.json)
cases/min, matcher lag, confirm queue depth, confirmed ok/fail totals
## Findings
open N / fixed N / ledgered N / parked N   (+ per-group table for open)
## Fix agent
attempts total, fixed, ledgered, failed, timeouts; current attempt (sig, started); spend: token
totals (input/cached/output) + attempt wall-hours — from state/spend.json (codex reports token
usage per turn.completed event but no dollar cost; cap = DDLFUZZ_MAX_TOKENS if set)
## Components
fuzzer up/restarts, e2e up/restarts, disk free, breaker states
## Coverage history (state/coverage/history/edges.csv: ts,go,mysql,mariadb — appended hourly)
last 12h edge deltas per bitmap
```

Final report = same document with `status: final` plus: totals, escalations index (sig → one-line
reason), spend total, and the **end-of-run metrics** (informational, not pass/fail — the run
already ended on the clock; 00-overview §Exit): (1) plateau — edges at T vs T−12h from edges.csv,
flagged "plateaued" when zero new edges on both oracle bitmaps; (2) findings tally open / fixed /
ledgered / parked; (3) whether every parked sig has `escalations/<sig>.md`. Rendered as
numbers + a one-word status word (`plateaued`/`climbing`, `clean`/`work-remaining`) so the human
can decide extend-vs-rerun-vs-done at a glance. (Construct-checklist coverage is 20's report; the
supervisor links to it if 20 exposes it in stats — otherwise noted "see component 20".)

## Implementation steps

1. **Scaffold** `tools/ddlfuzz/supervisor/`: `main.go` (cobra-free, plain flag+subcommand switch),
   `config.go` (env-driven config struct with the defaults above), `preflight.go` (startup
   sequence steps 2–10), `proc.go` (pgroup spawn/kill, timeout runner), `hello.go` (protocol-v1
   HELLO smoke), `gate.go`, `git.go`, `fixloop.go`, `park.go`, `report.go`, `disk.go`,
   `prompt.tmpl`. Build: `go build -o build/ddlsuper ./supervisor` from `tools/ddlfuzz/` (add
   `package main` guard so `go test ./...` in the module stays green).

2. **`proc.go`** — the one primitive everything uses:
   ```go
   type Proc struct{ cmd *exec.Cmd; pgid int }
   func Start(dir string, stdout, stderr io.Writer, name string, args ...string) (*Proc, error)
       // SysProcAttr{Setpgid: true}; record pgid
   func (p *Proc) StopGracefully(term time.Duration) // SIGTERM pgroup; wait; SIGKILL pgroup at deadline
   func RunTimeout(ctx, dir string, timeout time.Duration, stdin io.Reader, name string, args ...string) (Result, error)
       // Result{ExitCode, TimedOut, Stdout, Stderr}; on timeout SIGKILL(-pgid)
   ```

3. **`hello.go`** — spawn oracle binary, write frame `u32(1)|u8(3)`, read `u32 len|u8 type|payload`,
   parse HELLO JSON, assert engine/protocol, `StopGracefully(2s)`. 10s overall timeout.

4. **`git.go`** — helpers: `Head()`, `IsCleanTracked()`, `UntrackedSet()`, snapshot/diff of
   untracked vs `state/untracked.baseline`, `ResetHard(sha)`, `DeleteNewUntracked(before, after
   sets, allowedRoots=[flow/, tools/ddlfuzz/], excluded=[tools/ddlfuzz/state/, tools/ddlfuzz/build/])`,
   `CommitsSince(sha) []Commit`, `TouchedPaths(shaRange)`. All via `git -C /Users/ilia/Code/peerdb`.

5. **`gate.go`** — the 6 gate steps with per-step timeouts and `nice -n 10`; returns first failure
   (step name + tail of output). Plus `RebuildOracles(engines)` (run build.sh scripts, 30m timeout
   each) and `RunGolden()` (`build/ddlfuzz golden`, 20m).

6. **`fixloop.go`** — implements Selection / Attempt / validation / rollback / hot-restart exactly
   as in Design. Attempt record appended to `state/attempts/<sig>.jsonl`:
   ```jsonc
   {"attempt":N,"sig":"...","started_at":"...","ended_at":"...","outcome":"fixed|ledgered|failed|
     timeout|gate_failed|replay_failed|golden_failed|regression|forbidden_paths|no_result",
    "detail":"...","tokens":{"input":0,"cached_input":0,"output":0},"commits":["sha"],
    "thread_id":"<from the thread.started event — sessions persist; resumable via codex exec resume>",
    "transcript":"attempts/<sig>.attempt<N>.stream.jsonl",
    "last_message":"attempts/<sig>.attempt<N>.last.txt","diff":"attempts/<sig>.attempt<N>.diff"}
   ```
   Prompt rendering: read `prompt.tmpl`, `strings.ReplaceAll` for each placeholder; `{META_JSON}` is
   the pretty-printed meta; siblings/prior-attempts as specified. Spend accounting: sum `usage`
   over all `turn.completed` events of the attempt JSONL into `state/spend.json`
   (`{"tokens":{"input":X,"cached_input":Y,"output":Z},"attempts":N,"attempt_seconds":S}`, atomic
   rename); enforce `DDLFUZZ_MAX_TOKENS` when nonzero (no dollar accounting — codex reports no
   cost).

7. **`park.go`** — budget check, escalation writer (template above; diagnosis via the jq-equivalent
   in Go: decode each stream-jsonl line, collect `agent_message` text), parked.list append (dedup),
   groups.json maintenance + flap detector as specified.

8. **`report.go`** — heartbeat + final report per the Observability section; `edges.csv` appender;
   findings counters by scanning meta files; all writes atomic (tmp+rename).

9. **`main.go` subcommands:**
   - `run` — startup sequence (preflight steps 1–10, any failure ⇒ `state/BLOCKED` + exit 2) +
     goroutines + deadline/signal handling (context cancellation tree).
   - `hello --oracle <path> --engine <e>` — preflight helper / manual smoke.
   - `gate` — run the gate once, exit status accordingly (used by preflight step 9 and by hand).
   - `fix-once --sig <sig> [--skip-fuzzer]` — run exactly one attempt cycle for a sig (dry-run tool).
   - `status` — print a terse digest of report.md + stats.json + findings counts.

10. **`preflight.go`** — implement startup-sequence steps 2–10 exactly as specified in Design
    (repo/branch/clean checks, tool probes incl. `docker info`, disk ≥ 40 GiB via statfs, builds,
    oracle build.sh fallback, HELLO smokes, the codex auth smoke — same invocation shape as the
    fix agent but `-s read-only`, prompt `Reply with the single word ok`, 120s timeout, assert
    `ok` in the last-message file — golden, gate, compose up). Each step logs pass/fail with
    timing; failures render the exact failing command + output tail into `state/BLOCKED`.

11. **Wire deadline shutdown + final report**; verify INT/TERM path with a 2-minute
    `DDLFUZZ_HOURS=0.03` smoke.

12. **Commit** everything under `tools/ddlfuzz/supervisor/` (plus .gitignore entries for
    `state/`, `build/` if 20 hasn't already) to `parser-wip`, message
    `ddlfuzz: supervisor + fix loop`, no co-author trailer.

## Acceptance checks

1. `go build ./... && go test ./...` in `tools/ddlfuzz` green with supervisor code added.
2. `ddlsuper hello` against both oracle binaries passes; against `/bin/cat` fails within 10s.
3. Unit tests (Go, in `supervisor/`): group_key normalization (error strings with differing
   identifiers/numbers collapse; different classes don't), budget arithmetic, untracked-set diff
   deletion policy (never deletes outside allowed roots / inside state/), attempt-record round-trip,
   flap detector (2 fixes + rediscovery ⇒ group parked), report rendering from fixture
   stats/e2e-stats/meta files, codex JSONL parsing (token-usage summation over `turn.completed`
   events, agent_message extraction, RESULT-line parsing) from a recorded fixture transcript.
4. **Fix-loop dry run (no fuzzer needed):** fabricate `state/findings/deadbeef0123/` with a real
   divergence: plant a synthetic parser bug first (see 5), run `ddlfuzz replay` to confirm exit 10,
   then `ddlsuper fix-once --sig deadbeef0123 --skip-fuzzer`. Pass criteria: agent transcript
   captured, a commit exists on parser-wip with the message convention and no co-author, gate
   re-ran green, replay exits 0, meta status `fixed`, seed committed at `seeds/deadbeef0123.sql`,
   attempt jsonl written, `last_good_commit` advanced. Then `git reset --hard` the test commit away.
5. **Synthetic planted bug** for 4 and 6: in `flow/connectors/mysql/ddl_parser.go`, break the
   `MIDDLEINT` synonym (map it to `smallint` instead of `mediumint`) — one-line, discoverable by
   any generator emitting type synonyms, fixable by a competent agent in one attempt, and covered
   by the signature comparison (qkind differs). Commit it as a throwaway commit on a scratch branch
   copy of parser-wip if running check 6 (the 1h run) — or directly on parser-wip and reset after.
6. **1h dry run:** `DDLFUZZ_HOURS=1 ./build/ddlsuper run` (from `tools/ddlfuzz/`) with the planted
   bug in place (committed, so gate-on-HEAD sees it as "good"; golden must still pass —
   MIDDLEINT is not in the golden corpus expectations… if it is, pick the equivalent one-liner on
   `FLOAT4`→`double` instead; executor: verify the planted bug keeps `ddlfuzz golden` green before
   starting, that's what makes it fuzz-discoverable rather than preflight-blocked). Pass criteria:
   preflight completes (incl. the codex read-only auth smoke); fuzzer runs and stats.json updates;
   the finding appears, fix loop repairs it, fuzzer hot-restarts, e2e confirmqueue entry appears;
   hourly report written; Ctrl-C at any point produces a clean shutdown + final report; the fix
   agent's writes stayed inside the repo + go caches (workspace-write sandbox; codex session
   files under `~/.codex` are expected and fine); the gate ran to green from inside
   the codex sandbox during the agent's own step (c) — this validates the `--add-dir` cache list;
   if it fails on sandbox denials, see the sandbox-friction fallback in Risks.
7. **Rollback path:** rerun 4 with `DDLFUZZ_ATTEMPT_TIMEOUT=30s` (agent cannot finish; supervisor
   pgroup-kills ⇒ outcome `timeout`) — attempt recorded, tree back at last_good, no stray
   untracked files, loop proceeds. Repeat 3 times ⇒ escalation file + parked.list entry appear,
   meta flips to `parked`.
8. **Crash-loop breaker:** point the fuzzer monitor at a binary that exits 1 immediately ⇒ after 6
   exits/30min the breaker trips, `escalations/run-fuzzer-crashloop.md` exists, e2e lane and fix
   loop unaffected, retries continue at 30min cadence.
9. **Wedge detection:** freeze stats.json (chmod the file or SIGSTOP the fuzzer) ⇒ restart within
   ~6min, logged.

## Risks & fallbacks

- **codex CLI flag drift** (flags verified against 0.142.5; codex updates frequently): preflight
  step 7 smoke-tests the exact invocation shape at t=0; any mid-run invocation failure is just a
  failed attempt (record + continue). Pin: do not run `codex update` during the run.
- **Sandbox friction**: workspace-write may deny writes the toolchain needs beyond the enumerated
  `--add-dir` caches (e.g. a linter cache in an unexpected path). Acceptance check 6 exercises the
  full gate from inside the sandbox; if an unattended run still hits denials, the defined fallback
  knob is `DDLFUZZ_CODEX_BYPASS=1` ⇒ swap `-s workspace-write ... --add-dir ...` for
  `--dangerously-bypass-approvals-and-sandbox` (reverting to post-hoc-only guardrail enforcement,
  same as the supervisor's validation layer assumes anyway). Denial symptoms are visible in the
  attempt transcript (`command_execution` items with permission errors) — the escalation file
  surfaces them.
- **Weaker spend enforcement than planned**: codex has no per-run dollar/turn budget flag and
  reports no cost, so per-attempt bounds are wall-clock (45 min pgroup kill) only, with token
  telemetry and an optional run-wide token cap (`DDLFUZZ_MAX_TOKENS`). A runaway-but-productive
  agent burns at most 45 min × 3 attempts per sig; that is the accepted ceiling.
- **Agent misbehaves despite guardrails** (edits oracle+parser together, weakens tests, bogus
  ledger): validation 4b/4d/4e catches the mechanical cases and reverts; a bogus-but-cited ledger
  entry survives the run and is reviewed by the human post-run via the final report's ledger index
  — acceptable, since ledgering never masks a panic (replay exit 11 always fails validation).
  Guardrails formerly duplicated by CLI-level permission flags now rest on the OS sandbox
  (write scope) plus the prompt text (git/behavioral rules) plus post-hoc validation.
- **Gate cost (~5–15min) serializes fixes**: acceptable; the fuzzer never pauses, and findings queue
  up. If attempt throughput becomes the bottleneck, lower `DDLFUZZ_ATTEMPT_TIMEOUT`, not the gate.
- **`git reset --hard` vs concurrent state writes**: state/ and build/ are gitignored, so resets
  never touch fuzzer output; the untracked-deletion policy explicitly excludes them.
- **Host sleep**: out of scope (Amphetamine); the supervisor tolerates wall-clock jumps
  (deadline math uses absolute time, monitors are stateless per tick).
- **e2e/docker disk blowups**: watchdog degrades e2e first; fast lane and fix loop survive on
  <10GiB; nothing under state/ is ever deleted.
- **Fuzzer can't restart after a bad-but-gate-green fix** (e.g. runtime-only breakage): crash-loop
  breaker records `run-fuzzer-crashloop.md`; the 30min retry loop means a subsequent fixing commit
  can revive it without human action. The supervisor does NOT auto-revert gate-passing commits.

## Effort

~2.5–3.5 days: proc/git/gate plumbing 0.5d; fix loop + validation + rollback 1d; parking/flap +
report 0.5d; preflight + hello 0.5d; dry-run hardening (checks 4–9) 0.5–1d.

## Contract issues

1. **State-dir additions** (now ratified in 00-overview's state layout + Reconciliation
   decision D6): `stats.json`, `e2e-stats.json`, `e2e-queue/{pending,processing,done}/`,
   `groups.json`, `last_good_commit`, `untracked.baseline`,
   `BLOCKED`, `spend.json`, `supervisor.log`, `supervisor.pid`, `children.json`,
   `coverage/history/edges.csv`,
   `attempts/<sig>.attempt<N>.{stream.jsonl,diff}`. All additive; no contracted paths change
   meaning. Also `escalations/run-*.md` for run-level (non-sig) escalations — the overview only
   specifies `escalations/<sig>.md`; the `run-` prefix cannot collide with 12-hex sigs.
2. **New top-level dir `tools/ddlfuzz/seeds/`** (committed corpus seeds from fixes) — not in the
   overview layout; imposed on 20 to load at startup (C7). Chosen over writing directly into
   `state/corpus.db` because fixes must ship committable artifacts and state/ is gitignored.
3. **Imposed CLI shapes on 20**: `replay <sig>` exit-code contract (0/10/11/1) and JSON stdout;
   `fuzz` SIGTERM-flush ≤60s + atomic writes + stats cadence (C3–C6, C8). The overview names the
   subcommands but not these semantics — reconciler should confirm 20's plan matches.
4. **Imposed on 30**: compose path, lane binary path/build command, e2e-stats schema, confirmqueue
   protocol (C9–C11). The overview gives 30 "containers, flow module, state dir" but no queue —
   the confirmation-queue direction (40 → 30 after fixes) is new.
5. **Imposed on 10/11**: oracle binary output paths `build/oracle-{mysql,mariadb}` and build.sh
   locations (C1).
6. **Overview end-of-run metric "corpus distilled into committed test cases"** (now a report
   metric, not a gate — 00-overview §Exit): partially automated here (per-fix regression tests +
   seeds/) plus 20's optional shutdown distillation; any further full-corpus distillation is
   post-run human/agent work — the final report notes it as a follow-up rather than asserting it.
7. **Fix agent runs on Codex, not `claude -p`**: the overview's component table names the fix
   agent as "`claude -p` cycle" — per user direction this component uses `codex exec` instead
   (flags verified above). The overview's mention should be updated by the reconciler; everything
   else about the cycle (serialized, gated, budgeted, parked) is unchanged.
8. **codex writes outside the repo vs "nothing outside the repo"**: the codex CLI keeps
   auth/log/session internals under `~/.codex` (sessions are intentionally persisted for post-run
   inspection, per user direction), and the fix agent's builds legitimately write the Go/lint
   caches under `~/Library/Caches` and `~/go` (explicitly whitelisted via `--add-dir`) — same
   caches our own gate runs write. Treated as out-of-scope tool internals, noted here for honesty.
