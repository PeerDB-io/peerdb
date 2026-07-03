# 52 — Plan: coarse-signature flapping — identity, flap parking, and overwrite policy

Investigation deliverable for `plan/52-flapping-context.md`. Read that first for the symptom
narrative; this doc restates the problem, quantifies it from the live campaign state, lays out
options across the three mechanisms, and recommends one package with an implementation sketch and
test plan. **No code changes here — plan only.**

## Problem, crisply

A finding's identity is `<sig> = sha256_12({v, engine, masked sql_mode, class, lane, shape})`.
For `sig_mismatch`, `shape` is only the *first-difference dimension* (`stmt_count`, `spec_count`,
…), and for every e2e class it is the literal string `unspecified`. So identity for those classes
degenerates to `(engine × masked-mode)`: **every unrelated bug that first diverges on the same
dimension shares one sig per engine/mode slot**. Three mechanisms then compound the damage:

1. **Identity too coarse** — distinct root causes collapse onto one sig; fixing one makes the sig
   "re-open" when the next unrelated bug lands on it (double-fix of `02d99e5d62d4`).
2. **Flap detector keyed on the coarse group** — `group_key = sha256(class|shape)` is coarser
   still (no engine/mode). Two fixes anywhere in a family trip `fix_count ≥ 2`, and the *next new
   sig* parks the whole group, including never-attempted, genuinely-new bugs (park-on-arrival).
3. **Overwrite policy splits repro from meta** — `findings.Record` refreshes `our_sig` /
   `our_error` / `oracle_digest` (and e2e capture meta) on *every* rediscovery of an open sig but
   only rewrites `repro.sql` on the fixed→open flip, so a heavily-rediscovered finding's repro and
   meta describe *different occurrences*. Per-sig `seeds/<sig>.sql` is clobbered by each new fix
   for the same sig.

The coarseness itself is load-bearing: open sigs in the live state have absorbed **up to 813k
rediscoveries each** (`4ca6ae028918` times_seen=813357). Any fineness must keep a single bug's
finding cardinality bounded — never re-create a divergence storm.

## Evidence (live state, read-only, snapshot 2026-07-03)

145 findings: 108 fixed / 21 open / 15 parked / 1 ledgered. Engine×masked-mode slots in use: 15
(2 engines × masked modes {0,4,512,516,1024,1028,1048576,1048580,1049600,1049604} → ≤20 slots).

- **Parked ≠ unsolvable.** 12/15 parked are `sig_mismatch`/`stmt_count` — the whole group
  `d402e1fcd233` (`fix_count=2, parked=true`), one sig per engine×mode slot, **10 of 12 with zero
  attempt records** (parked on arrival). Their repro.sql files show *different* root-cause
  families (`ALTER … RENAME TO … , MODIFY …` trailing-spec handling, multi-statement continuation,
  etc.).
- **Double-fix.** `02d99e5d62d4` (mariadb, mode 0, stmt_count) has two `outcome=fixed` attempts
  with two commits for two unrelated causes: `08351af6` "MariaDB reversed comments", `a27f3f39`
  "split numeric SET values". The second fix's commit rewrote `seeds/02d99e5d62d4.sql`, clobbering
  the first fix's committed regression seed.
- **Internally stale records.** All 12 parked stmt_count findings have `repro.sql` that does not
  correspond to `meta.our_sig`/`oracle_digest` (e.g. `040fe490e361`: repro `ALTER TABLE db.t
  RENAME TO after_col, MODIFY COLUMN a FLOAT INVISIBLE` vs our_sig about `enum`/`json_col`
  columns). Cause is the meta-refresh-without-repro-rewrite path in `findings.Record`
  (`internal/findings/findings.go:104-127` — repro written only when `!existed` or
  `status=="fixed"`, while `our_sig`/`our_error`/`f.Meta` merge happens unconditionally).
- **e2e identity is engine×mode only.** e2e recorders never set `Meta["shape"]`, so
  `shapeFromFinding` defaults to `unspecified`; e.g. group `0bb2d1b8e84c`
  (`e2e-missed-column-effect`) holds 11 sigs, `fix_count=4` — it escaped flap-parking only because
  plan 51's confirm-fixed sweep reconciled the stale re-opens before the flap scan.
- **Flap "re-open" trigger is dead code.** `ApplyFlapDetector` (`supervisor/park.go:123`) checks
  `f.RediscoveredCount > 0`, but `rediscovered_count` is written *nowhere* — `findings.Record`
  writes `times_seen`. Only `!previouslyKnown` (a brand-new sig) trips group parking today.
- **Supervisor meta rewrites destroy fields.** `writeFindingMeta` marshals the typed `FindingMeta`
  struct (`supervisor/fixloop.go:22-38`), silently dropping every key not in it: 106 fixed
  findings lost `times_seen`/`descriptor`/`origin`/`seed`; e2e findings lose plan 50's capture
  fields (`submitted_text` absent from 27 e2e findings) whenever the supervisor flips their
  status — which breaks plan 50's offline `replay` for exactly the findings preflight later holds
  to the strict `fixed` bar.
- **Shape leakage bug.** Live shapes include `col_kind(uint32, B=uint32≠uint32; col B=uint32)` —
  identifier text (`B`) leaked into a shape via `FirstDifference`'s signature parser, violating
  the identifier-free descriptor requirement.

Useful negative result: shapes that already carry bounded semantics (`col_kind(a≠b)`,
`col_params(p,s≠p,s)`, `col_notnull`, `table_qual`) show no double-fix/flap pathology — their
groups closed cleanly (e.g. `table_qual`: 13 sigs, 2 fixes, all fixed via post-fix replay sweep).
The pathology is confined to the count-shaped dimensions and the shapeless e2e classes.

## Options

### Option A — bounded shape enrichment (mechanism 1: signature granularity)

Enrich only the lossy shapes with **kind/count detail, never identifier text**:

- `stmt_count` → `stmt_count(<oursKinds>≠<theirsKinds>)` where each side is the `+`-joined
  statement-kind sequence of the *canonical* signature, capped at 5 entries (`…` suffix beyond),
  e.g. the observed family becomes `stmt_count(alter+rename≠alter)`.
- `spec_count` → `spec_count(<a>≠<b>;<oursSpecKinds>≠<theirsSpecKinds>)` with per-statement spec
  kind sequences from the canonical (bucketed) form, alphabet {col,chg,ren,drop}, capped at 6.
- `parse_our_sig`/`parse_oracle_sig` (12 live findings, likewise degenerate) → append the
  `NormalizeError`-style masked parse-failure reason.
- e2e classes → populate `Finding.Meta["shape"]` from the check that fired, dimension-only, e.g.
  `col-attr(is_nullable)`, `missed-effect(added)`, `plumbing(mode)` — plan 50's
  `e2echeck.Result.Shape` is the natural producer.
- Fix the `col_kind` identifier-leak: sanitize `FirstDifference` payloads to the single
  kind/param token (reject or truncate anything containing spaces/`=`).

**Cardinality / storm analysis.** Shape variety is drawn from closed vocabularies: statement kinds
(2) × counts (generator emits ≤3 chained statements + ≤1 rename-split ⇒ per-side sequences
realistically ≤5 long), spec kinds (4) × spec counts (≤5 specs/statement + flatten). A single
root-cause bug typically produces ONE kind-sequence pair, worst case a handful (count varies with
random chaining): ≤ ~10 shapes × ≤20 engine·mode slots ⇒ **≤ ~200 sigs absolute worst case,
tens realistic** — versus millions of inputs deduped, and backstopped by the existing 200-open-
findings rate limit in `internal/findings`. Identifier variety still cannot enter the shape.
Post-fix `applySuccessfulStatuses` replays *all* open findings and auto-fixes the siblings, so a
one-fix family of N sigs still closes in one attempt.

**Migration.** Do NOT bump descriptor `v` (that would re-bucket every finding, including healthy
`col_kind` ones). Only the affected shape strings change ⇒ only stmt_count/spec_count/parse_*/e2e
sigs re-bucket. Old parked sigs stay suppressed via `parked.list` under their old identities; the
underlying *bugs* re-surface as new fine sigs with fresh attempt budget — this is precisely the
desired "unpark by re-bucketing" for the 10 never-attempted parked bugs. Old `groups.json` keys go
inert. No state rewrite required. `replay`/preflight are sig-identity-blind (they re-check
divergence of the recorded repro), so existing dirs keep working.

**Interactions.** minimize: `FastLanePredicate` preserves descriptor equality — a finer shape makes
shrinking *stricter* (cannot shrink into a different count-family), which is correct.
group_key: `NormalizeShape` masks digits but not kind words, so groups become finer too — see the
group-key note under the recommendation. confirm-fixed (51): unaffected, and needed less often.
e2e repro (50): the e2e shape needs 50's check-detail plumbing — ship A's e2e part with/after 50.

### Option B — reproduce-gated, sig-keyed flap detection (mechanism 2: park.go)

Keep coarse sigs as-is; rework parking so it can't swallow new bugs:

1. **Sig-level flap**: `findings.Record` increments `meta.reopened_count` on every fixed→open
   flip (also resurrecting the currently-dead `rediscovered_count` consumer). A sig with
   `reopened_count ≥ 2` *whose replay still exits 10 after the confirm-fixed sweep* is parked —
   that is genuine churn on one identity.
2. **Group park becomes a soft selection-freeze**: `fix_count ≥ 2` + a reproducing new/reopened
   sig sets `parked:true` on the group, which continues to exclude the group from `SelectFinding`
   — but member sigs are **not** appended to `parked.list`, metas stay `open`, and arrivals keep
   being recorded (`times_seen` keeps counting). One run-level escalation
   (`escalations/run-flap-<group>.md`) replaces the per-sig escalation spam. A human (or a later
   fix that closes siblings via the replay sweep) can lift the freeze by clearing `parked` in
   `groups.json`; hard parking (parked.list + per-sig escalation) remains only for sig-level flap
   and budget exhaustion.
3. **Reopen counting requires reproduction**: a re-open only counts toward flap if
   `replay <sig>` exits 10 at scan time (confirm-fixed already re-marks the reconcilable ones;
   this closes the remaining stale-binary window between fix commit and lane restart).

**Cardinality / storm.** Group families stay bounded by engine×mode (≤20 sigs), so the worst
fix-loop burn with the freeze lifted is 20 sigs × 3 attempts — the freeze caps it at whatever was
attempted before `fix_count` hit 2. A pure divergence storm (one bug, many inputs) still dedups to
the same coarse sigs; B changes no identity, so storm behavior is exactly today's.

**Migration.** Additive: new meta key `reopened_count`, `groups.json` unchanged in shape
(park-on-arrival simply stops appending to `parked.list`). Existing 15 parked sigs stay parked;
un-parking the 10 zero-attempt ones would be a manual one-time cleanup.

**Interactions.** Depends on plan 51's ordering (confirm-fixed before flap scan — already true in
`RunFixLoop`) and on plan 50 for e2e replay to be meaningful (else the "reproduces" gate can't be
evaluated for e2e findings — treat `exit 11` as "does not count toward flap"). Sibling grouping
and minimize untouched. Weakness: does nothing about double-fix, stale repro/meta, or seed
clobber — distinct bugs still share sigs; the fix agent still sees a repro that may not reproduce
the meta it reads.

### Option C — occurrence-preserving record & seed policy (mechanism 3: findings.Record)

Keep identity and flap logic; make each finding dir internally consistent and clobber-proof:

1. **Atomic pairing**: `our_sig`/`our_error`/`oracle_digest` (and e2e capture meta) may only be
   rewritten when `repro.sql` is rewritten in the same `Record` call. Open-finding rediscovery
   updates only `times_seen` + a new `last_seen_at`. (Also removes ~800k pointless meta rewrites
   per hot sig.)
2. **Archive on re-open**: on the fixed→open flip, move the current `repro.sql` to
   `repro.prev<N>.sql` (N ≤ 3, ring) before writing the new occurrence + its matching meta; bump
   `reopened_count`. The fix that was regressed (or the different bug that re-opened the sig)
   remains diagnosable.
3. **Seed naming**: fix-agent prompt writes `seeds/<sig>-<sha8-of-content>.sql` (+ matching
   `.meta.json`) instead of `seeds/<sig>.sql`; the seed loader already reads the directory, so a
   second fix for the same sig *adds* a seed instead of replacing one.
4. **Supervisor meta round-trip**: `writeFindingMeta` becomes read-raw-map / mutate
   status+fixed_by / write-back, so supervisor status flips stop pruning `times_seen`, descriptor
   fields, and plan 50's e2e capture keys (which currently breaks offline e2e replay for
   supervisor-touched findings).

**Cardinality / storm.** No identity change ⇒ no new sigs. Archives bounded (≤3 per sig); seeds
grow one file per successful fix (already the intent). Zero storm exposure.

**Migration.** Fully additive. Existing stale repro/meta pairs heal on their next rewrite event
(or stay stale-but-frozen, which preflight tolerates since replay only checks divergence).

**Interactions.** minimize writes a shrunken `repro.sql` deliberately — allowed (it re-verifies
descriptor equality first); pairing rule applies to `Record` only. Plan 50 *requires* item 4 or
its capture meta doesn't survive the first supervisor status flip. Plan 51 benefits: confirm-fixed
re-marks `fixed` without touching repro, consistent with pairing.

## RECOMMENDED: A + C, plus the two surgical slices of B

Option A is the root-cause fix — B and C alone leave distinct bugs sharing identity, which is what
manufactures both the flapping and the clobbering. But A without C still records internally
inconsistent findings (the pairing bug is independent of granularity), and A without B's gating
leaves the dead/park-on-arrival flap semantics waiting to re-bite on the *finer* groups.
Concretely:

1. **A in full** (fast-lane shapes now; the e2e shape as part of plan 50's `e2echeck` work).
2. **C in full** — items 1–4 are small, contained in `internal/findings` + `supervisor`, and item
   4 is a live defect regardless of this plan.
3. **From B**: (i) write `reopened_count` and make re-open counting reproduce-gated;
   (ii) demote group parking to the soft selection-freeze (no parked.list append, no per-sig
   escalation for arrivals). Skip B's sig-level-flap hard park initially: with fine sigs, a
   twice-reopened-and-reproducing sig means a real regressing fix and deserves the freeze +
   escalation, not silent suppression at the dedup layer.
4. **Group key coarsening (keeps sibling grouping useful)**: `GroupInfoForMeta` computes the
   group shape from the *dimension prefix* of the shape (text before the first `(`), post-
   `NormalizeShape`. Then `stmt_count(alter+rename≠alter)` and `stmt_count(alter+alter≠alter)`
   remain siblings in one `stmt_count` group — the fix-agent prompt still gets the family as
   context, the flap freeze still observes family-level churn, but *identity* (and budget, seeds,
   repro) is per-bug. This exactly restores today's group granularity while sigs get finer.

Why this is storm-safe: identity variety per bug stays drawn from closed vocabularies (≤ ~200
sigs absolute worst case per bug, tens realistic — see Option A analysis); the dedup layer,
200-open cap, and the coarse group freeze are all retained. Why it dissolves the observed
pathology: the 12-sig parked stmt_count family re-buckets into per-root-cause identities with
fresh budget; double-fix cannot recur (second root cause gets its own sig and its own seed file);
repro/meta/seed artifacts stop lying to the fix agent; park-on-arrival of never-attempted bugs is
gone; confirm-fixed keeps handling stale re-opens.

Rollout into the live run: merge-and-rebuild via the normal supervisor hot-restart. New shapes
apply to new discoveries only; nothing rewrites existing state. Optional one-time cleanup (safe,
manual): remove the 12 stmt_count sigs from `parked.list` or leave them — either way the bugs
resurface under fine sigs and get attempted.

### Implementation sketch

- `internal/compare` (`compare.go`):
  - `FirstDifference`: on `stmt_count`, render both sides' kind sequences (`stmtKinds(a)`,
    capped 5); on `spec_count`, counts + spec-kind sequences (capped 6); on `parse_*`, append
    masked reason; sanitize `col_kind`/`col_params` payloads to single tokens.
  - Keep descriptor `v:1`; add nothing to `Descriptor`.
- `internal/findings` (`findings.go`):
  - `Record`: open-rediscovery path updates only `times_seen`+`last_seen_at` (skip the
    `our_sig`/`our_error`/Meta merge unless repro is (re)written); fixed→open path archives
    `repro.sql`→`repro.prev<N>.sql` (ring of 3), increments `reopened_count`, then writes new
    repro + full meta refresh.
- `e2e/checks.go` + `internal/e2echeck` (with plan 50): populate `Finding.Meta["shape"]` from the
  failing check's dimension.
- `supervisor/fixloop.go`:
  - `writeFindingMeta` → raw-map read-modify-write (status, fixed_by only).
  - `GroupInfoForMeta`: group shape = dimension prefix of `NormalizeShape(shape)`.
- `supervisor/park.go`:
  - `ApplyFlapDetector`: reopen trigger reads `reopened_count` (now real) and requires a
    reproducing replay (exit 10; exit 11 ⇒ no count); group `parked:true` no longer feeds
    `parked.list`/per-sig escalations for arrivals — emit `escalations/run-flap-<group>.md` once.
  - `SelectFinding` already skips `g.Parked` groups — unchanged.
- `supervisor/prompt.tmpl`: seed path becomes `seeds/{SIG}-<sha8>.sql`; note the archive files.

Contained to `internal/compare`, `internal/findings`, `e2e`/`internal/e2echeck`, `supervisor` —
matching the context doc's containment requirement; fast-lane comparison outcomes (diverge or
reconcile) are untouched, only the shape *string* of a divergence changes.

### Test plan

Unit (all in-package, no live system):
- `compare`: table for enriched `FirstDifference` — kind sequences, caps, identifier-free
  guarantee (fuzz identifiers through both sides, assert shape unchanged); golden `DescriptorSig`
  values for *unchanged* shapes (`col_kind`, `table_qual`, `we_error`) proving no accidental
  re-bucketing; regression test for the `col_kind(… B=…)` leak.
- `findings`: rediscovery of open sig leaves repro+our_sig+digest untouched, bumps
  times_seen/last_seen_at; fixed→open archives to `repro.prev1.sql`, bumps `reopened_count`,
  rewrites meta consistently (assert `our_sig` matches the new statement's divergence); archive
  ring caps at 3.
- `supervisor`: `writeFindingMeta` round-trips unknown keys (fixture meta with `times_seen`,
  `submitted_text`); dimension-prefix group key (`stmt_count(alter+rename≠alter)` ≡ `stmt_count`
  group); `ApplyFlapDetector` scenarios — (a) 2 fixes + reproducing new sig ⇒ group frozen, no
  parked.list append, arrivals still recorded; (b) reopen with replay exit 0 (confirm-fixed path)
  ⇒ no flap count; (c) reopen with exit 10 twice ⇒ counts; (d) zero-attempt arrival in frozen
  group is skipped by `SelectFinding` but not parked.
- Rebuild the observed pathology as a fixture: two synthetic root causes that today share a
  `stmt_count` sig — assert they now get distinct sigs, distinct seeds, and that fixing one does
  not re-open the other.

Integration (scratch state dir, never the live campaign): `ddlfuzz replay` on a fixture finding
with an enriched shape (exit 10, correct class/shape in JSON); one `ddlsuper fix-once` smoke
verifying the freeze file and prompt seed naming; preflight replay across a state dir containing
old-format (v:1 coarse-shape) findings to prove migration inertness.

Acceptance: the standard gate (`go build/vet/test` untagged + `-tags ddlfuzz`, flow gate) green;
after merge, watch one supervisor cycle: no park-on-arrival events, `groups.json` shows
dimension-prefix keys, and newly recorded stmt_count findings carry enriched shapes.
