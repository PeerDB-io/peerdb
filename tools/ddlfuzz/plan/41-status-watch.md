# 41 — `ddlsuper status`/`watch`: live campaign dashboard

Follow-up plan for `tools/ddlfuzz/supervisor/`. Conforms to `40-supervisor.md` and
`00-overview.md`; deviations listed in **Contract issues**. Executor: implement exactly this; no
open decisions. Line numbers refer to the tree at `e589ee5c` (branch point of
`ddlfuzz-status-watch`).

## Problem

The only at-a-glance view is `ddlsuper status` (`supervisor/main.go:107`), which prints the first
80 lines of `state/report.md` — a file rewritten **hourly** (`ReportEvery: time.Hour`,
`config.go:95`). Everything actually decision-relevant is missing or stale:

- Open-findings groups render as opaque hash keys (`findingsSummary`, `report.go:202`), hiding the
  backlog shape (e.g. today: 37 open sigs = 7 groups, two dominating).
- The fix-agent line hardcodes `current attempt none` (`report.go:81`) — no visibility into which
  sig/attempt is running, elapsed vs the 45-min budget, or what the agent last said.
- No supervisor liveness, deadline countdown, or BLOCKED surfacing.
- Rates over time (execs/s, suppressed/s, edge growth, e2e case throughput) are only obtainable by
  tailing `state/supervisor.log` and eyeballing the fuzzer's 5s heartbeat lines.

Operating constraint (binding): the supervisor runs in the background and is killed/restarted by
agent sessions at will. The viewer must therefore be **pull-based and read-only over the state
dir** — never attached to the supervisor process, never taking the supervisor lock, and it must
survive (and clearly display) supervisor death and restart. All supervisor-side changes below are
additive state files; a viewer built to this spec keeps working against a supervisor from before
this plan (panes degrade to `n/a`).

## Design

### Commands

```
ddlsuper status [--json] [--no-color]        # one-shot snapshot, rendered live from state files
ddlsuper watch  [--interval 3s] [--no-color] # repaint loop, default 3s, min 1s
```

- `status` no longer reads `report.md` (which stays as the hourly historical record); it collects
  and renders fresh. `--json` marshals the full snapshot struct (incl. computed rates) for agent
  consumption.
- `watch` = the same collector+renderer on a ticker, drawn to the alternate screen. Both commands
  are strictly read-only: no lock acquisition, no state writes, safe to run in any number of
  terminals concurrently with a running (or dead) supervisor.

### New state files (all written by the supervisor, atomic rename via `atomicWriteFile`)

1. **`state/run.json`** — written once in `runCommand` (`main.go:126`) immediately after
   `AcquireSupervisorLock`:
   ```jsonc
   {"pid":4242,"started_at":"<RFC3339>","deadline":"<RFC3339>","run_hours":72,"fix_model":"gpt-5.5"}
   ```
   Fixes the viewer-side clock problem: `cfg.StartedAt`/`cfg.Deadline` are computed at process
   start (`config.go:93-94`), so a viewer process must read the *run's* times from disk.
   Liveness = `state/supervisor.pid` exists ∧ `syscall.Kill(pid, 0) == nil`.

2. **`state/samples.jsonl`** — appended every 5s by a new ticker case in `runReportTickers`
   (`main.go:439`), one flat-ish JSON line:
   ```jsonc
   {"ts":"<RFC3339>",
    "fuzz":{"execs_total":0,"suppressed":0,"corpus":{"mysql":0,"mariadb":0},
            "edges":{"go":0,"mysql":0,"mariadb":0},"restarts":{"mysql":0,"mariadb":0}},
    "findings":{"open":0,"fixed":0,"ledgered":0,"parked":0},
    "e2e":{"cases":{"mysql":0,"mariadb":0},"exec_rejects":{"mysql":0,"mariadb":0}},
    "queue":{"pending":0,"processing":0,"done":0},
    "spend":{"input":0,"cached_input":0,"output":0},
    "disk_free_bytes":0}
   ```
   Sources: `readFastStats` (`report.go:151`), `ScanFindings` counts, `readGenericJSON` +
   `collectCounters` on `e2e-stats.json` (`report.go:260-282`), `confirmQueueDepth` split by dir,
   `LoadSpend`, `FreeBytes`. Growth: 72h @ 5s ≈ 52k lines ≈ 20 MB — no rotation. Viewers read only
   the trailing 1 MiB. This is the single history source for all delta/rate display; it survives
   both supervisor and viewer restarts, unlike an in-viewer ring, and unlike parsing the fuzzer's
   heartbeat log lines it does not couple the viewer to component 20's stdout format.

3. **`state/current-attempt.json`** — written by `FixOnce` after prompt render, immediately before
   `runCodexAttempt` (`fixloop.go:741`); removed via `defer os.Remove` registered at the write
   site (covers every exit path); rewritten with `phase:"validate"` right after `ended :=
   time.Now()` (`fixloop.go:742`):
   ```jsonc
   {"sig":"...","attempt":2,"max_attempts":3,"group_key":"...","class":"...","shape":"...",
    "engine":"...","phase":"agent"|"validate","started_at":"<RFC3339>",
    "attempt_deadline":"<RFC3339>",        // started_at + cfg.AttemptTO
    "transcript":"attempts/<sig>.attempt<N>.stream.jsonl"}
   ```
   `runCommand` deletes any leftover file during startup (stale from a mid-attempt kill).
   Viewer staleness rule: supervisor pid dead ∨ `now > attempt_deadline + 5min` ⇒ render the
   attempt as `stale (killed mid-attempt?)` in yellow, never as running.

### Rate math (`supervisor/samples.go`)

All displayed deltas come from the samples series (viewer-side; `watch` re-reads the tail each
tick — 1 MiB / 3s is negligible):

- `rate(field, window)` = sum of **positive** consecutive deltas over samples within
  `[now−window, now]`, divided by the actual covered duration. Monotone-segment summation makes
  counter resets (fuzzer hot-restart zeroes `execs_total`/`suppressed`; lane restart zeroes e2e
  counters) contribute only their post-reset growth instead of a bogus negative.
- Windows: **1m** and **15m**. When the series covers less than the window, divide by covered
  duration and label the column header with it (`Δ4m`), never extrapolate. Fewer than 2 samples
  in window ⇒ `n/a`.
- NOW cells: `execs/s` comes straight from `stats.json` (`execs_per_sec`); rate rows with no
  instantaneous source (`suppressed/s`, the e2e per-minute rows) use a 45s window as "now".
- Sparklines: `▁▂▃▄▅▆▇█` over the last 30m, bucket count a parameter (the two-column layout uses
  20×90s cells), scaled to the window max; drawn for execs/s and summed e2e cases/min. Empty
  buckets render `·`.
- Monotone gauges (edges, corpus, findings counts) show plain deltas over the window
  (`+3`), same positive-sum rule.

### Layout (both commands render the identical frame; `status` prints it once)

Reference terminal: **130×30**. Structure rules, applied to every pane — no free-form run-on
lines of mixed numbers:

- Each pane is a **titled section**: pane name at the left of a dim `─` rule, freshness/summary
  pinned into the rule at the right.
- Pane bodies are one of exactly two shapes: a **label grid** (aligned label→value pairs, labels
  dim, values bold where headline) or a **fixed-column table** (dim uppercase column header row,
  one metric per row, numbers right-aligned within columns).
- Rates and gauges share one table shape: `METRIC | NOW | Δ1m | Δ15m`. Rate rows (`…/s`, `…/min`)
  show per-window rates in the Δ columns; gauge rows (edges, counts) show signed deltas (`+3`).
  Sparklines get their own `trend` grid row under the table.
- **Two-column regions**: at width ≥ 110, panes are paired side by side — each pane renders into
  a `colWidth = (width − 4) / 2` block and the blocks are zipped with a 4-space gutter, the
  shorter side padded with blank lines. The frame is three regions: header (full width), FAST
  LANE ∥ E2E LANE, FINDINGS ∥ (FIX AGENT stacked above EVENTS). Below 110 columns, the same panes
  stack single-column in that order.

At 130×30 (63-column panes, 29 rows used):

```
 ddlfuzz ──────────────────────────────────────────────────────────────────────────────────────────────────────── ● running ──
   supervisor   pid 4242 · up 3h12m       deadline   in 61h48m (Jul 6 07:24)      spend   4.1M in / 0.9M out
   started      2026-07-03 17:24          disk free  212 GiB

 FAST LANE ─────────────────────────────── stats 4s ago ──    E2E LANE ────────────────────────────────── hb 3s ago ──
   METRIC            NOW       Δ1m       Δ15m                   METRIC             NOW       Δ1m       Δ15m
   execs/s           405k      398k      391k                   cases/min          16.1      15.8      16.0
   suppressed/s      192       188       190                    exec-rejects/min   7.2       7.0       7.1
   edges mysql       20 987    +0        +3                     cases        25.1k my · 24.9k ma
   edges mariadb     10 416    +0        +0                     queue        0 pending · 0 processing · 12 done
   trend execs/s     ▂▃▅▆▆▇▆▅▆▇█▇▆▅▆▆▇▆▅▆                       trend cases  ▃▃▄▃▃▅▄▃▄▄▃▄▃▄▄▃▃▄▃▄
   corpus            77.6k my · 66.4k ma
   restarts          0 my · 0 ma

 FINDINGS ── open 46 · fixed 108 · parked 15 · +2/15m ──      FIX AGENT ──────────────── ● attempt 2/3 · 18m/45m ──
   CLASS|SHAPE                  SIGS  ATT  OLDEST MY/MA FLAGS    working on  4ca6ae028918 (sig_mismatch|rename_pairs,
   sig_mismatch|rename_pairs      13    2   4h12m   6/7            mysql) · phase agent
   sig_mismatch|stmt_kind         12    0   2h01m   5/7  flap    last msg    "The rename reconciliation drops the
   e2e-col-attr|unspecified        6    3   9h44m   2/4            schema qualifier when the target table is…"
   timeout|head=ALTER              2    1   1h20m   2/0          totals      31 att · 24 fixed · 1 ledg · 4 fail ·
   e2e-sqlmode-mismatch|unspec…    2    0     44m   1/1            2 timeout · 9.3 wall-h
   we_error|alter-table-parse…     1    1   3h05m   1/0
   sig_mismatch|spec_count         1    0     31m   0/1         EVENTS ──────────────────────────────────────────────
                                                                  00:38:12  fix committed for 53cb5d04 (rename rec…)
                                                                  00:38:40  fuzzer hot-restart ok
                                                                  00:40:02  confirm-fixed bc2d0d42f7ca
                                                                  00:46:04  rollback attempt 2 c8de48a483bd
```

Region notes: the header grid flows all pairs onto as few rows as the width allows. The findings
title rule drops `ledgered` first when the counts don't fit the column. In the right-hand
FINDINGS∥FIX-AGENT region, FIX AGENT grid values wrap onto a hanging-indent second line (sig line,
last-msg 2 lines max, totals) rather than truncating; EVENTS fills the rest of the region's
height. FLAGS abbreviates `flap-parked` → `flap` at column width < 70.

Pane specifics:

- **Header** — status pinned right in the rule: green `● running` (pid alive), red `● down` (pid
  dead/missing; the grid gains `last seen  <supervisor.log mtime ago>`), red `● BLOCKED` (file
  exists; grid gains a `blocked` row with the file's first line). Times from `run.json`; `n/a`
  when absent. Spend from `spend.json`.
- **Fast lane** — NOW column from `stats.json` directly; Δ/TREND from samples. Freshness in the
  title rule: `stats <age> ago`, dim normally, yellow `stale <age>` when > 5min (matches the
  wedge-detector threshold, `main.go:234`); when stale, NOW dims and Δ columns render from the
  last covered window. Below the table: the `trend` sparkline row, then label-grid rows for the
  non-rate gauges (corpus, oracle restarts).
- **E2E lane** — title-rule freshness from `updated_at` (unix seconds), yellow when > 60s.
  Per-engine counters from `collectCounters`; queue from the three dirs.
- **Findings** — title rule carries the status counts and `+N/15m` (findings whose
  `discovered_at` falls in the window — from metas, not samples, so it works one-shot). Table:
  aggregate open findings by `Finding.Group` (`GroupInfoForMeta`, `fixloop.go:196`); ATT =
  attempt records summed over the group's sigs (`LoadAttemptRecords`); OLDEST = max age of
  `discovered_at`; MY/MA = per-engine sig counts; FLAGS from `groups.json` (`LoadGroups`,
  `park.go:73`): `flap-parked` when `parked:true` (row magenta), `fixes:N` when `fix_count ≥ 1`.
  Sort by SIGS desc, cap 10 rows, `(+N more groups)` dim.
- **Fix agent** — title rule pins the live-attempt summary: `● attempt N/3 · <elapsed>/<budget>`
  colored by elapsed (<30m green, 30–40m yellow, >40m red), or `idle — <reason>` when no
  `current-attempt.json`: `deadline window` (within 3h of deadline, `fixloop.go:640`),
  `spend cap` (`run-spend-cap.md` exists), else `waiting for selection`. Grid rows: `working on`
  (sig, group, engine, phase), `last msg` = final `agent_message` text parsed from the trailing
  256 KiB of the attempt's `stream.jsonl` (reuse the event-decoding approach of
  `extractDiagnosis`, `park.go:284`, keeping the last message; truncate to 2 lines), `totals` =
  `attemptSummary` (`report.go:208`) + `LoadSpend` wall hours. Staleness rule from Design §3
  replaces the live summary with yellow `stale (killed mid-attempt?)`.
- **Events** — last 8 significant lines from the trailing 64 KiB of `supervisor.log`: drop lines
  prefixed `fuzzer: `/`e2e: ` (child-stdout relays via `logWriter`, `main.go:212`) entirely — the
  supervisor logs lifecycle events (exits, restarts, wedges) as its own unprefixed lines, and a
  keyword exception would false-match the fuzzer heartbeat's `restarts=` token. Timestamps
  reformatted to `HH:MM:SS` local, dim; message text plain.

### Color policy

Basic 16-color ANSI SGR only, via a ~20-line helper (no dependencies — the module currently has
none and stays that way). Enabled iff stdout is a character device (`os.Stdout.Stat()` mode
`ModeCharDevice`) ∧ `NO_COLOR` unset ∧ `TERM != "dumb"`; `--no-color` forces off; `--json` implies
off. Semantics — green: healthy/running; yellow: stale/warning; red: down/blocked/over-budget;
magenta: parked/flap; cyan: pane titles; bold: headline values; dim: rules, labels, column
headers, timestamps, truncation notes. Sparklines uncolored.

### `watch` loop mechanics

- Enter: `\x1b[?1049h\x1b[?25l` (alt screen, hide cursor). Each tick: build the frame in a
  `bytes.Buffer`, terminate every line with `\x1b[K`, emit `\x1b[H` + frame + `\x1b[J` (home +
  overdraw + erase-below — flicker-free, no full clears). Exit on SIGINT/SIGTERM:
  `\x1b[?25h\x1b[?1049l` then return (register via `signal.NotifyContext`).
- Terminal size via `TIOCGWINSZ` ioctl (raw `syscall`, darwin); fallback 130×30. Two-column at
  width ≥ 110 per the layout section, single-column stack below. Title rules extend to the pane
  width; table/grid columns are fixed and truncate their last cell (rune-safe, ANSI-aware —
  truncate the payload, keep the trailing `\x1b[K`). Height: if the frame exceeds terminal
  height, drop EVENTS lines first, then findings rows beyond 5, then the trend rows.
- Non-TTY stdout: degrade to plain sequential frames (no ANSI, separated by one blank line) so
  `ddlsuper watch | tee` still works.
- Interval: `--interval` parsed with `time.ParseDuration`, default 3s, floor 1s. Collection
  failures (missing files) render as `n/a` panes/cells, never crash the loop.

## Implementation steps

1. **`supervisor/samples.go`** (new): `SampleRecord` struct mirroring the JSON above;
   `CollectSample(cfg) SampleRecord` (readers listed in Design §2); `AppendSample(cfg,
   SampleRecord)` (open `O_APPEND`, single `json.Marshal`+newline write — the only non-rename
   state write, single-writer, line-atomic at this size); `ReadSamplesTail(cfg, maxBytes) []
   SampleRecord` (seek to `size−maxBytes`, discard the first partial line);
   `Rate(samples, extract func(SampleRecord) int64, window, now) (perSec float64, covered
   time.Duration, ok bool)` implementing positive-delta summation; `Sparkline(samples, extract,
   window, buckets) string`.

2. **`supervisor/main.go`** — `runReportTickers` (`main.go:439`): add `sample :=
   time.NewTicker(cfg.SampleEvery)` case calling `AppendSample(cfg, CollectSample(cfg))`.
   `runCommand` (`main.go:126`): write `run.json` after lock acquisition; delete stale
   `current-attempt.json`. Dispatch: `case "watch"` (`main.go:29`), pass `os.Args[2:]` to both
   `statusCommand` and `watchCommand`; update `usage` (`main.go:62`). `config.go`: add
   `SampleEvery time.Duration` default 5s.

3. **`supervisor/fixloop.go`** — `FixOnce`: write `current-attempt.json` (fields per Design §3)
   before `runCodexAttempt` (`fixloop.go:741`) with `defer os.Remove`; rewrite with
   `phase:"validate"` after `fixloop.go:742`. Helper `writeCurrentAttempt(cfg, finding, attemptN,
   phase string, started time.Time)`.

4. **`supervisor/status.go`** (new): `type StatusSnapshot struct` holding every pane's data plus
   computed rates (all fields JSON-tagged for `--json`); `CollectStatus(cfg) StatusSnapshot`
   assembling from: `run.json` + pid liveness, `BLOCKED`, `stats.json`, `e2e-stats.json`, queue
   dirs, `ScanFindings` + group aggregation + per-sig attempt records, `groups.json`,
   `parked.list`, `current-attempt.json` + transcript tail, `spend.json`, samples tail,
   `supervisor.log` tail, `FreeBytes`. Group aggregation returns a sorted `[]GroupRow{ClassShape,
   Sigs, EngineCounts, Attempts, OldestAge, Flags}`.

5. **`supervisor/render.go`** (new): `RenderStatus(snap StatusSnapshot, color bool, width int)
   string` per the layout; layout primitives shared by all panes — `paneRule(title, right,
   width)`, `labelGrid(rows [][2]string, cols int)`, `metricTable(header []string, rows
   [][]string)` with right-aligned numeric columns (the first column absorbs width overflow, min
   12, ellipsizing its cells, so numeric columns never truncate), and `sideBySide(left, right []string,
   colWidth, gutter int) []string` (zip two pane blocks, pad the shorter with blanks, ANSI-aware
   padding to `colWidth`); ANSI helper (`paint(code, s)`, `stripANSI` and `visibleWidth` for
   alignment and tests); humanizers (`fmtCount` 104.6k/340.0M, `fmtGrouped` 20 987 for edges,
   `fmtAgo`, `fmtDur`); the events reformatter. Panes render to `[]string` blocks at a given
   width; the frame assembler picks one- vs two-column and zips. Drive-by fix: `attemptSummary`
   (`report.go:208`) must skip the `attempts/<sig>.attempt<N>.stream.jsonl` transcripts its glob
   also matches — they inflate attempt totals with bogus records.

6. **`supervisor/watch.go`** (new): flag parsing, TTY/width detection, alt-screen lifecycle, tick
   loop calling `CollectStatus`+`RenderStatus`, height-budget truncation, non-TTY degrade.

7. **`supervisor/main.go`** — rewrite `statusCommand` (`main.go:107`): parse `--json`/`--no-color`;
   `--json` ⇒ `json.MarshalIndent(CollectStatus(cfg))`; else print `RenderStatus`. Drop the
   `report.md` read; `RenderReport`/`WriteReport` and the hourly cadence are untouched.

## Tests (extend `supervisor_test.go` style: tempdir state fixtures, no live system)

- `TestRatePositiveDeltaSum` — steady counter → exact rate; mid-window reset (1000→0→400) counts
  only +400; single sample ⇒ `ok=false`; short series ⇒ `covered` < window and rate uses covered.
- `TestReadSamplesTail` — file larger than `maxBytes` ⇒ first partial line discarded, rest parsed;
  empty/missing file ⇒ nil, no error.
- `TestSparkline` — known series ⇒ expected glyph string; empty buckets ⇒ `·`.
- `TestGroupRows` — fixture metas across 3 groups/2 engines with attempt jsonls ⇒ correct SIGS/ATT/
  OLDEST/MY-MA, sort order, `flap-parked` flag from a `groups.json` with `parked:true`.
- `TestCurrentAttemptStale` — `attempt_deadline` in the past ⇒ stale; live pid + future deadline ⇒
  running; missing file ⇒ idle.
- `TestMetricTableAlignment` — colored cells align: `visibleWidth` ignores SGR sequences; numeric
  columns right-aligned; overlong last cell truncated rune-safely.
- `TestSideBySide` — unequal block heights zip with blank padding; every output line has the left
  block padded to exactly `colWidth` visible columns (ANSI codes present); gutter width exact.
- `TestFrameBreakpoint` — width 130 ⇒ lanes and findings/agent regions paired; width 100 ⇒
  single-column stack, same pane order.
- `TestRenderStatusSmoke` — full fixture state dir ⇒ `RenderStatus(snap, false, 130)` contains the
  pane titles, table headers, group rows, and `n/a` markers for absent panes; no line wider than
  130 visible columns; no ANSI bytes when `color=false`.
- `TestEventsFilter` — heartbeat relay lines dropped, `fuzzer exited:` kept, timestamps
  reformatted.
- `TestStatusJSON` — `CollectStatus` on the fixture marshals and round-trips.

## Acceptance checks

1. From `tools/ddlfuzz/`: `go build ./... && go vet ./... && go test ./...` and
   `go build -o build/ddlsuper ./supervisor` green.
2. Against the **live** state dir (read-only, safe; from this worktree point the viewer at the
   main checkout): `DDLFUZZ_ROOT=/Users/ilia/Code/peerdb build/ddlsuper status` renders all panes
   with real data in <1s; `… status --json | jq .` parses; `… status --no-color | grep -c $'\x1b'`
   = 0.
3. `DDLFUZZ_ROOT=/Users/ilia/Code/peerdb build/ddlsuper watch` in a terminal: repaints at 3s
   without flicker; resize handled next tick; Ctrl-C restores the screen and cursor.
4. Kill/restart decoupling: `kill $(cat state/supervisor.pid)` while `watch` runs ⇒ header flips to
   red `down` within one tick, panes keep last-known data; restart `ddlsuper run` ⇒ header returns
   to green and samples resume, watch untouched throughout.
5. Rates: after the supervisor has run ≥15m with the sampler, `watch` shows non-`n/a` 1m/15m rates
   and a populated sparkline; a fuzzer hot-restart (or manual `kill` of the fuzz process) does not
   produce a negative or absurd rate on the next frames.
6. Fix-agent pane: during a live attempt (or `fix-once` on a synthetic finding per plan 40 check 4)
   shows sig, attempt N/3, elapsed, phase, and a last-message excerpt; after completion returns to
   `idle`.

## Scope guard

- Touch only `tools/ddlfuzz/supervisor/**`. No changes to the parser, oracles, driver, e2e lane,
  or any 20/30 contract; `report.md`, `stats.json`, `e2e-stats.json` schemas and cadences
  unchanged.
- Viewer commands never write state, never take the supervisor lock, never signal processes.
- Supervisor-side additions (`run.json`, `samples.jsonl`, `current-attempt.json`, sampler ticker)
  take effect on its next restart; mixed old-supervisor/new-viewer renders `n/a` panes, never
  errors — deployable against the currently running campaign.
- Development happens on the `ddlfuzz-status-watch` worktree branch: the live campaign's fix
  agents commit to `parser-wip` and its rollback deletes attempt-era untracked files under
  `tools/ddlfuzz/` in the main checkout (`DeleteNewUntracked`, `supervisor/git.go:214`), so
  nothing from this plan is written there until the campaign ends and the branch is rebased +
  merged back.

## Contract issues

1. **State-dir additions**: `run.json`, `samples.jsonl`, `current-attempt.json` — additive, same
   pattern as plan 40 Contract issue #1; `samples.jsonl` is append-only (line-atomic single
   writer), a deliberate exception to the atomic-rename rule that plan 40's own
   `attempts/*.jsonl` already shares.
2. **`status` semantics change**: plan 40 describes `status` as "a terse digest of report.md +
   stats.json + findings counts"; it now renders live from state files and gains `--json`.
   `report.md` remains the contractual hourly heartbeat, unchanged.
3. **New subcommand `watch`** — extends plan 40's provided-interface list
   (`run`/`hello`/`gate`/`fix-once`/`status`).

## Effort

~1–1.5 days: samples + rate math + tests 0.25d; collector 0.25d; renderer + layout primitives +
color 0.35d; watch loop + TTY handling 0.25d; fixloop/run wiring 0.1d; live smoke + polish 0.25d.
