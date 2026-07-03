# 52 — Context: the finding "flapping" / coarse-signature problem

This is a **problem context** for an investigation, not a solution. Read it, study the code + live
evidence, and propose a plan (`plan/52-flapping-plan.md`) with options, tradeoffs, and a
recommendation. Do NOT implement code.

## Symptom

The 72h campaign accumulates "parked" findings that are not distinct unsolvable bugs but artifacts of
signature coarseness. Concretely, at one snapshot 12 of 14 parked findings were `sig_mismatch` with
shape `stmt_count`, most with **zero attempt records** — they were parked *on arrival* because their
group was already flap-parked. Two further observed artifacts:

- **Double-fix:** sig `02d99e5d62d4` was legitimately fixed twice for two *different* root causes
  (MariaDB reversed `/*!! */` comments, then numeric `SET STATEMENT` values) because both produced the
  same `stmt_count` divergence shape under the same engine+mode → same sig.
- **Internally-stale records:** a parked finding's `repro.sql` (e.g. `ALTER TABLE db.t RENAME TO
  after_col, MODIFY COLUMN a FLOAT INVISIBLE`) did not match its own `our_sig`/`oracle_digest` in
  meta — because `repro.sql` is overwritten on every re-discovery while the meta fields lag.
- **Seed clobber:** `seeds/<sig>.sql` is per-sig, so a second fix for the same sig overwrites the
  first fix's committed regression seed (the earlier repro survives only as a test-file case).

## The three mechanisms in tension

### 1. Signature / descriptor granularity  (`internal/compare` descriptor; `internal/findings.Record`)
`<sig>` = first 12 hex of `sha256(descriptor)` where the descriptor is
`{v, engine, sql_mode(masked to ANSI_QUOTES|ORACLE|MSSQL|NO_BACKSLASH_ESCAPES), class, lane, shape}`.
For `sig_mismatch`, `shape` is the **first-difference dimension** — e.g. `stmt_count`, `spec_count`,
`col_kind(int32≠int16)`, `table_qual`, `rename_pairs` — with identifiers, byte offsets, and incidental
statement text deliberately normalized OUT (`NormalizeShape`: quotes→`<id>`, hex→`<sig>`, digits→`<n>`).

- **Why coarse (the original intent):** a single parser bug hit by millions of fuzz inputs must dedup
  to ONE finding, or the fix loop drowns in a "divergence storm." Re-discovery via different random
  inputs should map to the same sig. This is load-bearing for a 72h unattended run.
- **Why coarse hurts here:** dimensions like `stmt_count` are *too* lossy — "our statement count ≠
  the server's" is a whole *family* of unrelated bugs (multi-statement `;`-chains, `ALTER … RENAME
  TO`, SET STATEMENT wrapping, comment handling). They all collapse onto near-identical sigs, so
  distinct bugs share identity, fixes appear to "reopen," and one flap parks the whole family.
- Consider: which shape dimensions are appropriately specific (`col_kind(a≠b)` carries the kinds) vs
  which are near-useless for identity (`stmt_count`, `spec_count`)? Would adding a bounded amount of
  detail (the actual counts; the first-differing statement KIND; the spec class) to the lossy shapes
  fix identity without reintroducing storm-sized cardinality? What is the cardinality risk of each
  option (a fuzzer can generate unbounded identifier variety, but bounded count/kind variety)?

### 2. Flap detector  (`supervisor/park.go`, `supervisor/fixloop.go`; `groups.json`)
Findings are grouped by `group_key = sha256(class|shape)` (coarser still — no engine/mode). The flap
detector parks a whole GROUP when it has `fix_count ≥ 2` and a new open sig appears (or a fixed sig
re-opens); thereafter new sigs mapping to that group are parked on arrival.

- **Why it exists:** to stop an infinite fix→reopen→fix loop from burning the whole run on one
  churning shape.
- **Why it hurts here:** with coarse shapes, a group is a *family of distinct bugs*; fixing two of
  them trips `fix_count ≥ 2` and parks all the rest — including genuinely-new, never-attempted bugs.
- Consider: should flap-parking key on the *same sig* re-opening repeatedly rather than *different
  sigs in a coarse group*? Should a re-open only count toward flapping if it actually reproduces
  (replay exit 10) — now that confirm-fixed exists (a fixed sig that re-opens but reconciles is
  already handled)? Does finer sig granularity (mechanism 1) largely dissolve this on its own?
  Interaction with the just-added confirm-fixed sweep (plan 51) must be considered.

### 3. Re-discovery overwrite policy  (`internal/findings.Record`)
On re-discovery of an existing sig: if status was `fixed` it re-opens and `repro.sql` is overwritten;
`f.Meta` is merged into meta; `times_seen++`. For e2e findings the full capture meta is refreshed.

- **Why overwrite is good:** keeps the repro current, refreshes e2e capture to the latest occurrence,
  dedups storms into one dir, `times_seen` tracks frequency.
- **Why overwrite is bad:** when distinct inputs share a coarse sig, each re-discovery clobbers the
  previous occurrence's repro — producing the internally-inconsistent records observed (repro.sql vs
  our_sig/digest), and clobbering committed seeds. The specific input that triggered a *particular*
  occurrence is lost.
- Consider: keep a canonical (first/minimized) repro separate from "latest occurrence"? Retain N
  distinct repros per sig? Don't overwrite an *open* finding's repro? Per-occurrence seed naming
  (`<sig>.<n>.sql` or content-hash) to stop seed clobber? How does this interact with `minimize`?

## Cross-cutting considerations for any proposal
- **Do not regress divergence-storm dedup** — the reason coarse sigs exist. Any fineness must keep
  cardinality bounded (a single bug must not spawn thousands of findings from identifier variety).
- **Stability of the running 72h run** — changes land via merge into a live campaign; must be
  contained (descriptor in `internal/compare`, `internal/findings.Record`, `supervisor/park.go`),
  behavior-preserving for the fast lane's comparison results, and not perturb existing on-disk sigs
  more than necessary (a sig-format change re-buckets all findings — assess migration/impact).
- **Interactions:** confirm-fixed (plan 51), the e2e offline reproduction (plan 50), the coarse
  `group_key` used by the fix-agent prompt's sibling grouping, and `minimize`.
- **Evidence to gather:** enumerate the current parked/open findings by (class, shape); measure how
  many distinct repros/our_sigs hide under each coarse sig; check `groups.json` fix_counts; quantify
  the storm risk of each granularity option from the generator's construct space.

## Deliverable
`plan/52-flapping-plan.md`: the problem restated crisply, 2–4 concrete solution options across the
three mechanisms (with cardinality/storm analysis, migration impact, and interaction notes), a
recommended option with rationale, and a rough implementation sketch + test plan for the recommendation.
Investigation is read-only + a committed plan doc; no code changes.
