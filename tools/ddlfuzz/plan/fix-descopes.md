# fix-descopes — approved descope + dead-code cleanup

Working dir for all Go work: `tools/ddlfuzz` (module root). Line numbers are current for this
tree; re-check before editing. Rules: whole-symbol removals only, no reflow of surrounding code
(especially e2e/fixture.go, e2e/worker.go, e2e/rewrite.go — a sibling branch touches them).
Never remove existing comments elsewhere; do not add new validation or new behavior beyond A7.
Do NOT touch plan/99-gaps.md. Do not run docker/e2e/oracle binaries.

## A. Code removals

A1. Remove both minimizers.
  - cmd/ddlfuzz/main.go: drop import of internal/minimize (:19), cfg field `minimizeBudget`
    (:39), its default (:67), the `-minimize-budget` flag (:97), `"minimize"` from the
    subcommand allowlist (:129), the `case "minimize"` dispatch (:205-211), the usage line
    mention (:218), and `runMinimize` (:247-~262).
  - e2e/checks.go: drop import of internal/minimize (:18), the tryMinimize call site
    (:343-346, i.e. the `if min, ok := tryMinimize(...)` block; keep `stmt := slices.Clone(...)`
    and the pre-existing `meta["minimized"] = false` at :328), and func tryMinimize (:396-406).
    If `bytes` becomes unused in checks.go, drop the import.
  - Delete the whole internal/minimize/ package (single file minimize.go, no tests). Verified:
    no other importers; minimize imports internal/compare, not vice versa — compare stays.

A2. supervisor/main.go: remove gzipOldAttempts (:593-604) and its call site — the
  `if free < 3*GiB { gzipOldAttempts(cfg, 6*time.Hour) }` block at :545-547. Intentional
  behavior change: low-disk transcript compression is dropped (its .gz output was unreadable by
  all consumers).

A3. internal/exec/exec.go: remove write-only `curSeq`/`curDead` fields (:28-29) and all their
  writes (:53-55, and the `w.curSeq.CompareAndSwap(seq, seq+1)` lines at :66 and :69, plus the
  now-unused `seq :=` at :53). Drop `sync/atomic` import if now unused.

A4. internal/wire/wire.go: at :65, :87, :103 change `if n < 0 || len(body) < n` to
  `if len(body) < n` (n comes from uint32 into 64-bit int; never negative). Delete the dead
  condition only; do not add validation.

A5. e2e/queue.go: remove the no-op readback loop at :115-117:
  `for engine := range engineChans { stats.SetPending(engine, stats.Snapshot()[engine].MatcherPending) }`.
  If `engineChans` or imports become unused, adjust minimally (engineChans is still used above).

A6. Dead symbols (each verified zero callers in this tree):
  - e2e/fixture.go: func resetFixture (:142-...) — whole func only, no reflow.
  - e2e/fixture.go: func isFixtureRename (:243-...) — same.
  - e2e/rewrite.go: func splitTopLevelFallback (:268-...) — same.
  - e2e/runner.go + e2e/worker.go: the buffered worker-completion channel: runner.go :89
    `done := make(chan struct{}, cfg.Workers)` and passing it at :94; worker.go: drop the
    `done chan<- struct{}` param from runWorker (:59) and the `done <- struct{}{}` send (:64).
    Do NOT touch runner.go's other `done` at :118-126 (wg.Wait close/select) — that one is live.
  - e2e/matcher.go: dead initializer at :487 — replace the three-line init+if/else (:487-492)
    with `queries := []string{"SHOW BINARY LOG STATUS", "SHOW MASTER STATUS"}` and
    `if ec.IsMariaDB { queries = []string{"SHOW BINLOG STATUS", "SHOW MASTER STATUS"} }`.
  - internal/replay/replay.go: func statusOf (:525-...).
  - internal/seed/seed.go evalString: the `default:` arm (:382-387) can never yield a string
    (fmt.Sprint of an ast.Expr is not a Go string literal), so reduce it to `return "", false`.
    Drop `go/constant` (and `fmt` if unused elsewhere in the file) imports as needed.
  - cmd/ddlfuzz/main.go: parsed-but-unused `-exec-workers`: cfg field (:30), default
    computation (:47-49), cfg literal entry (:59), flag registration (:88). Drop `runtime`
    import if unused.

A7. cmd/ddlfuzz/fuzzloop.go :193: `seeds, _ := seed.LoadDir(cfg.seedsDir)` — capture the error;
  on err log one line to stderr (fmt.Fprintf(os.Stderr, ...)) and continue with whatever seeds
  returned (non-fatal).

A8. Do NOT remove: mutate.Crossover, queueItem.SQLMode, anything else that looks dead.

## B. Doc ratifications (terse, match each doc's existing voice; goal: future audits stop
re-flagging these accepted divergences)

B1. plan/21-fuzzer.md:
  - Exit-3/fatal contract (:348 spawn-failure fatal, :627-630 os.Exit(3) path): add a short
    ratification note that in the implementation the fatal path parks on wg.Wait and the
    supervisor's stale-stats watchdog (stats.json ts >5min ⇒ restart) is the accepted recovery
    mechanism in place of exit(3).
  - R-list (Step 5, after R13 or wherever tolerances are enumerated): ratify
    `compare.ContainsSkippedVersionComment` and `e2echeck.EquivalentQueryText`
    (whitespace / trailing-`;` / version-comment tolerances) as sanctioned members of the
    reconciliation list.
  - Step 6 (:610): mark the watchdog/abandoned-worker hang containment as descoped — shipped
    containment is a per-case timer + leaked parser goroutine, and curSeq/curDead were removed
    as write-only. Revisit trigger: if timeout-class findings rise after retention/mutation
    changes land, reverse this descope first.
  - Step 11 (:786-789): mark the state/inflight.jsonl crash journal as descoped.
  - Step 15 (:870, minimize section ~:898+): record the minimize subcommand +
    internal/minimize + fast-lane predicate removal as an approved descope (A1); adjust the
    stray mentions at :18, :61, :74-79, :101, :106 with brief "(descoped)" annotations rather
    than rewrites.
  - :112 "Keep stdlib-only otherwise." — amend for sanctioned go-mysql and modernc.org/sqlite
    deps (B5).

B2. plan/00-overview.md:
  - :201 state-contract line `seed, inflight.jsonl` — mark inflight.jsonl descoped.
  - :236-237 "Preflight on every fuzzer restart (40): replay all findings/*/repro.sql" — mark
    descoped; accepted substitutes: per-attempt replay-all + merge-servicing replay.

B3. plan/30-e2e-lane.md:
  - :103-104 internal/minimize dependency bullet and :490-498 Minimization paragraph — update
    for the A1 removal (findings record minimized:false; fast-lane hook descoped).
  - :702-704 6-hourly `FLUSH BINARY LOGS` + `PURGE` — mark descoped; accepted:
    binlog_expire_logs_seconds=86400 (24h expiry).
  - :534 backlog throttle "resume < 64" — mark descoped; accepted: pause >256, resume ≤256
    (no low-water hysteresis).

B4. plan/40-supervisor.md:
  - Remove the now-false `-tags ddlfuzz` from build lines (:81, :237, :329, :445) — everything
    builds untagged.
  - Near the embedded fix-agent prompt template (:372): one-line note that the shipped
    supervisor/prompt.tmpl is authoritative over the embedded copy in this doc.
  - :220 disk-watchdog `<3GiB ⇒ gzip attempt transcripts` — remove/adjust for A2.

B5. plan/20-skeleton-first.md :47 "stdlib + `flow` only" (and plan/21 :112 per B1): amend to
  reflect the sanctioned go-mysql and modernc.org/sqlite dependencies.

B6. docs/mysql-ddl-parser-writeup.md :107-109: the bare `ALTER ... RENAME <word>` paragraph
  says the rename is "consumed and dropped" — correct it: the parser emits a `ddlRenameTable`
  statement for it (keep the TiDB-parity parenthetical if it still reads true).

No doc change for: G4 seed regeneration, G9 log rotation, G10 wire hardening, D6 periodic
requeue — accepted as-is.

## Validation

From tools/ddlfuzz: `go build ./... && go vet ./... && go test ./...`.
If the build needs the gitignored flow/generated protos, copy flow/generated/ from
/Users/ilia/Code/peerdb (read-only source) into this worktree first.
`golangci-lint run ./...` if available.
grep-verify each removed symbol has zero remaining references (including tests and docs where
code identifiers are load-bearing).
