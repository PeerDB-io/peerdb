# 12 — oracle lctn remediation + fix-agent guardrails

Case study: `feb2407a` (fix for `cd05d5a63830`). The MariaDB oracle boots with macOS-autosized
`lower_case_table_names=2` (`mysqld.cc:4505-4526`: case-insensitive datadir fs → lctn=2), so its
digests lowercase schema/table idents at parse (`lex_ident.h:234`) — unfaithful to Linux
production (lctn=0). The fix agent resolved the resulting divergence in the wrong direction:
lowercased idents in the production parser (`normalizeTableIdent`, violating plan 21 R10),
rewrote two existing test expectations (`1E23A`→`1e23a`, `ROLLUP`→`rollup`) and overwrote seed
`cd05d5a63830.sql` to make the gate pass. Validation rejected the attempt (`regression`), but
rollback ran on a cancelled ctx (99-gaps S3) and the next preflight ratified HEAD unconditionally
(`preflight.go:192`). Result: parser and oracle agree on the distortion; the rig is blind to the
case dimension on MariaDB; genuine bug `e9bd81d40423` (case-only rename dropped as noop) parked.
No agent has ever taken the oracle bucket — the incentive gradient (minutes of Go vs C++ +
~11min rebuild + golden inside the 45min kill) always favors bending the parser toward the oracle.

Two parts: **remediation** (revert + faithful oracle, one stopped-supervisor window) and
**guardrails** (make this class of fix impossible to land, not just discouraged).

## Part 1 — remediation (single window: stop ddlsuper, apply all, restart)

1. **Revert `feb2407a`.** Restores `normalizeTableIdent` removal, the `1E23A`/`ROLLUP`
   expectations, the original `seeds/cd05d5a63830.sql` bytes, and drops the wrong
   lowercase-asserting `INVOKER` test. Attempt-1's genuine fix (`c0e2ceed`, exponent idents)
   is a separate commit and stays.
2. **Force lctn=0 in the MariaDB oracle driver.** `--lower-case-table-names=0` is a dead end:
   explicit 0 on a case-insensitive fs hard-fails init (`mysqld.cc:4507-4518`). Instead, in
   `oracle/mariadb/main.cc` after `mysql_server_init()`, before `create_embedded_thd()`:
   ```cpp
   lower_case_table_names= 0;             // emulate Linux production default (parse-only:
   table_alias_charset= &my_charset_bin;  // no table files ever opened; init derived this
                                          // from lctn at mysqld.cc:4543-4545)
   ```
   Both are exported globals (`sql/mysqld.h:697`, `:98`). Same intervention level as the
   existing per-case `sql_mode` install (11-D5); black-box constraint holds.
   Fallback only if this shows other lctn-derived drift: case-sensitive APFS sparse image
   mounted at `build/scratch-cs` as datadir.
3. **Audit the MySQL oracle for the same exposure.** Circumstantially it preserves case (no
   MySQL-engine case findings all campaign); add the smoke probe and, if it folds, apply the
   same override in `oracle/mysql/driver.cc`.
4. **smoke.py case-preservation asserts (both engines):**
   `ALTER TABLE CamelCase ADD c INT` → `"table":"CamelCase"`;
   MariaDB `ALTER TABLE t RENAME TO T` → `"table":"t","new_table":"T"`.
5. **Anchor rows in `seeds/seeds.jsonl`** using the existing absolute check (`golden.go:150`):
   mixed-case idents with `expect_sig` for both engines, plus the case-only rename
   (`rename t>T`). From then on a case-folding "fix" on either side fails golden — which runs
   in every attempt's gate — instead of relying on plan-21 prose.
6. **Rebuild + validate:** `oracle/mariadb/build.sh` (and mysql if touched), `ddlfuzz golden`
   green, full gate.
7. **State hygiene:** unpark `e9bd81d40423` (drop from `parked.list`, meta → open, remove
   escalation file) — after the revert it replays clean (t≠T no longer noop) and auto-resolves;
   `7f5636632ad9`, `41bee5c7f43c` likewise. Recheck `0c216d4aff9d` (suspected oracle-side
   rename-under-ORACLE-mode digest, separate issue). Restart supervisor; preflight replay-all
   is the gate.

Ordering constraint: revert and oracle fix land in the SAME window. Parser reverted against the
old lowercasing oracle re-opens every mixed-case statement as a divergence and re-invites the
same wrong fix. Once the oracle preserves case, a parser-lowercasing fix can no longer pass
replay — the failure mode becomes structurally impossible.

## Part 2 — guardrails

- **A — absolute anchors in golden.** Golden is purely differential; a distortion applied
  consistently to both sides passes. Step 5 above starts this (parser-side `expect_sig`).
  Extend: committed oracle-digest expectations per anchor row (add `expect_digest` to the seed
  schema), covering environment-sensitive dimensions — ident case, quoting forms, latin1 bytes,
  `tinyint(1)`, sentinel-db mapping. Would have blocked the lctn artifact at preflight, before
  any fuzzing.
- **C — mechanical diff boundary** (validation-time, extend `forbiddenTouchedPaths`
  `git.go:254` / validation `fixloop.go:806-849`):
  - Existing tests append-only: `git diff --numstat last_good..HEAD` over
    `flow/connectors/mysql/*_test.go` must show 0 deletions. A fix that requires changing an
    existing expectation is a semantics change a human must approve → new outcome
    `test_weakened`, auto-park the sig.
  - `seeds/` add-only (`--diff-filter` allows only `A`). `feb2407a` violated both — caught twice.
  - Bucket-scoped allowlist: bucket 1 → `flow/connectors/mysql/**` + new seeds; bucket 2 →
    `tools/ddlfuzz/oracle/**`; bucket 3 → ledger append only. Forbid
    `tools/ddlfuzz/internal/compare/` in bucket-1 attempts (the other place a fix can neuter
    detection).
- **E — lifecycle: rejected commits must never ratify.** Validation DID reject `feb2407a`;
  the lifecycle kept it anyway.
  - Rollback on `context.Background()` + own timeout (S3); infra errors during validation
    (exit -1 / ctx canceled) → outcome `aborted`, not `regression`, still roll back.
  - In-run rollback stays as is: while the loop is running, commits past last_good belong to the
    current serialized attempt — reset is always safe.
  - Preflight drift handling (`preflight.go:192`): on HEAD ≠ last_good, classify each commit in
    `last_good..HEAD` against the attempt records (`attempts/*.jsonl` `commits` fields +
    `current-attempt.json` if a shutdown interrupted an attempt):
    - every drift commit attributable to a non-fixed/ledgered attempt → attempt residue
      (the `feb2407a` case) → reset to last_good;
    - any commit unknown to attempt records → human work made while the supervisor was off →
      keep it; gate + golden on HEAD (preflight already does) and adopt HEAD as the new
      last_good. Manual commits are NEVER reverted;
    - mixed residue + human commits, or torn attempt records → BLOCK listing the commits;
      human resolves. Never auto-reset anything not provably attempt residue.
  - Plus S2 (advance last_good only after rebuild succeeds) and S7 (`confirmFixed` must run
    `replayValidatesFinding`).

## Order of work

1. Part 1 in one stopped-supervisor window (revert, override, smoke, anchors, golden, unpark).
2. E (lifecycle) — small, prevents ratification of any future bad attempt.
3. C (diff boundary) — mechanical, would have caught this exact commit twice.
4. A extension (oracle-digest anchors) as budget allows.

## Implementation plan (this worktree, branch `oracle-lctn-remediation`)

All work happens in this worktree. The main checkout runs the live campaign — never touch it or
`tools/ddlfuzz/state/`. State hygiene (unparking) and the supervisor restart are the human's
landing window, NOT part of this implementation.

### 0. Build setup — reuse main-checkout artifacts (one-time)

The worktree `tools/ddlfuzz/build/` is empty (gitignored). Only the driver TU changes; the
server archive doesn't. Symlink the heavy artifacts from the main checkout's build dir:

```sh
WT=/Users/ilia/Code/peerdb/tools/ddlfuzz/worktrees/oracle-lctn
MAIN=/Users/ilia/Code/peerdb/tools/ddlfuzz/build
mkdir -p "$WT/tools/ddlfuzz/build"
ln -s "$MAIN/hostdeps" "$MAIN/mariadb-src" "$MAIN/mariadb-build" "$WT/tools/ddlfuzz/build/"
cp "$MAIN/oracle-mysql" "$WT/tools/ddlfuzz/build/oracle-mysql"
```

`oracle/mariadb/build.sh` phase guards check file existence → heavy phases skip; `ninja` no-ops
on the shared build tree; the driver recompiles in seconds to the worktree's
`build/oracle-mariadb`. If step 3 requires MySQL driver changes, symlink its src/build dirs the
same way (check `oracle/mysql/build.sh` for dir names) and run its build.sh.

### 1. Parser: remove MariaDB lowercasing (forward fix — `git revert feb2407a` will NOT apply;
three later commits built on it: `70091edc`, `67d41e3a`, `e3c92a3a`)

Current call graph in `flow/connectors/mysql/ddl_parser.go`: `normalizeTableIdent` (:170,
ToLower when isMariaDB) used by `parseTableIdent` (:204); identity variant
`parseAlterTableRenameIdent` (:207, used at :639 for ALTER..RENAME targets); `67d41e3a` routed
RENAME TABLE statement targets back through the lowercasing path (:1442). All of it mirrors the
broken oracle's lctn=2 inconsistency. Fix: identifiers always returned **as written** — delete
`normalizeTableIdent`, `parseAlterTableRenameIdent`, and the `parseTableIdentWith` indirection;
one plain `parseTableIdent`. Keep the `expectIdentToken` refactor. Keep `strings.ToLower` at
:942/:964/:976 — type/keyword classification, not identifiers.

Tests asserting the artifact — flip to case-preserving (keep the tests, they now assert
preservation): `ddl_lexer_test.go:278` `invoker`→`INVOKER`, `:285` `1e23a`→`1E23A`;
`ddl_parser_alter_test.go:521` `rollup`→`ROLLUP`; audit every case added by
feb2407a/70091edc/67d41e3a/e3c92a3a for lowercase-on-MariaDB expectations.

Restore `seeds/cd05d5a63830.sql` to the original bytes (no trailing newline):
`git show c0e2ceed:tools/ddlfuzz/seeds/cd05d5a63830.sql`.

Harness layers: KEEP `67d41e3a`'s structural `sigName` split in `internal/compare/compare.go`.
REMOVE the `CaseInsensitiveTableRename` tolerance (`internal/e2echeck/compare.go:12,:122,:334`,
`reproduce_ddlfuzz.go:129`, `e2e/checks.go:213`) — it papers over the lowercasing parser against
the case-preserving live Linux MariaDB container. Then sweep for further isMariaDB-conditional
case folds: `grep -rn "ToLower\|EqualFold" tools/ddlfuzz/internal/compare tools/ddlfuzz/e2e`.

### 2. MariaDB oracle: force lctn=0

`oracle/mariadb/main.cc`, after `mysql_server_init()` returns and before
`create_embedded_thd()`:

```cpp
lower_case_table_names= 0;             // emulate Linux production default; parse-only process,
table_alias_charset= &my_charset_bin;  // no table files opened (init derived this from lctn,
                                       // mysqld.cc:4543-4545)
```

Exported globals (`sql/mysqld.h:697`, `:98`). Do NOT pass `--lower-case-table-names=0` (hard
init failure on case-insensitive fs, `mysqld.cc:4507-4518`). Rebuild via
`oracle/mariadb/build.sh`.

### 3. MySQL oracle probe

Drive `build/oracle-mysql` with `ALTER TABLE CamelCase ADD c INT`; expected: digest preserves
`CamelCase` (no MySQL-engine case findings all campaign). If it folds, apply the same override
in `oracle/mysql/driver.cc` + rebuild. Either way add the smoke assert.

### 4. Smoke asserts (both `smoke.py`)

mariadb: `(0, "ALTER TABLE CamelCase ADD COLUMN c INT")` → `"table":"CamelCase"`;
`(0, "ALTER TABLE t RENAME TO T")` → `"table":"t"` with rename target `"T"` (exact JSON per the
digest schema incl. the G2 `new_table` extension — derive by running the binary once).
mysql: the CamelCase case.

### 5. Golden anchors

Append rows to `seeds/seeds.jsonl` with `"source":"anchor-case"` and `expect_sig`, both engines:
mixed-case ADD (`ALTER TABLE CamelCase ADD COLUMN c INT`), mixed-case qualified (`Db1.TBL`),
MariaDB case-only rename `ALTER TABLE t RENAME TO T` (expect `rename t>T`), MySQL
`RENAME TABLE Foo TO Bar`. Derive exact `expect_sig` strings by running the parser shim; every
anchor's expected sig MUST contain an uppercase letter — that is the anchor's purpose
(`golden.go:150` enforces absolutely).

### 6. Supervisor E — lifecycle

- `fixloop.go:788,:879`: `rollbackAttempt` on `context.Background()` + 2min timeout, never the
  attempt ctx.
- Outcome classification: validation failure caused by cancellation (`ctx.Err() != nil` /
  error wraps `context.Canceled`) → outcome `aborted`, not `regression`/`failed`; still rollback.
- S2: write `last_good_commit` + `applySuccessfulStatuses` only AFTER `rebuildAndHotRestart`
  (:861) succeeds; rebuild failure → `failed` → rollback.
- S7: `confirmFixed` (:1066) gains the `replayValidatesFinding` gate `autoResolveReconciled`
  (:1091) already has.
- Preflight drift classification (`preflight.go:192`): when `state/last_good_commit` exists and
  HEAD ≠ it, rev-list `last_good..HEAD` and classify against `attempts/*.jsonl` `commits` fields
  (outcome ∉ {fixed, ledgered}) plus `current-attempt.json` (read it BEFORE `main.go:129`
  removes it — reorder). All commits attributable to failed attempts → `git reset --hard
  last_good`, log it. Any unknown commit → human work: keep, gate ratifies, adopt HEAD as today.
  Mixed or unreadable records → BLOCKED listing the commits. Manual commits are NEVER reverted.
  Unit-test all three branches.

### 7. Supervisor C — diff boundary (validation block `fixloop.go:806-849`)

- `git diff --numstat <lastGood>..HEAD -- 'flow/connectors/mysql/*_test.go'`: any file with
  deletions > 0 → new outcome `test_weakened`.
- `git diff --name-status --diff-filter=MDR <lastGood>..HEAD -- tools/ddlfuzz/seeds/` non-empty
  → `test_weakened`.
- Extend `forbiddenTouchedPaths` (`git.go:254`): flow/ paths outside `flow/connectors/mysql/`;
  flow/ + `tools/ddlfuzz/internal/compare/` in the same attempt (same shape as the existing
  flow+oracle rule).
- `test_weakened`: after rollback, park the sig immediately (budget-exhaustion path, escalation
  reason "attempt modified existing test expectations/seeds") regardless of attempt count.
- Unit tests in `supervisor_test.go` per existing style.

### 8. Verification (from the worktree)

- flow gate: `cd flow && go build ./... && go vet ./connectors/mysql/ && go test
  ./connectors/mysql/ -run
  'TestDDL|TestProcessRenameTableQueryMetric|TestClassifyOnlineSchemaMigrationTool' -count=1 &&
  golangci-lint run ./connectors/mysql/... && go build -tags ddlfuzz ./...`
- `cd tools/ddlfuzz && go test ./...`
- `go build -o build/ddlfuzz ./cmd/ddlfuzz && build/ddlfuzz golden` → exit 0 (anchors green
  against the rebuilt oracle).
- `uv run oracle/mariadb/smoke.py` and the mysql smoke.

### 9. Remove the `ddlfuzz` build tag (added post-plan)

Separation was never the tag's doing — deps are isolated by the tools/ddlfuzz module split and
the campaign by the branch. The tag's only real effect was the `!ddlfuzz` stubs, which made it
possible to build a `ddlfuzz` binary whose parser errors on every input (99-gaps D8; bit the
step-8 golden verification here). Untag the shim + e2e lane + e2echeck/exec, delete the stubs,
drop the two tag-build gate steps and every `-tags ddlfuzz` in supervisor/prompt/scripts.
Bonus: lint and `go test ./...` now cover the shim and the e2e package by default (both were
previously exercised only under the tag, i.e. never by the gate).

### Commits

Small commits on this branch, no co-author trailer:
(1) parser forward-fix + tests + seed restore + e2echeck tolerance removal;
(2) oracle lctn override + smoke; (3) anchors; (4) supervisor E; (5) supervisor C.
