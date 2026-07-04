# 52 — Coarse-signature flapping: identity, flap parking, and overwrite policy

This restates the symptom, quantifies it from the live campaign state, lays out options across the
three mechanisms involved, and recommends one package with an implementation sketch and test plan.
**No code changes here — plan only** (the IMPLEMENTATION SPEC section below is the binding spec for
a follow-up change, not something already applied).

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

## IMPLEMENTATION SPEC — BINDING (v1)

This is the concrete, file-by-file spec the implementer builds. It layers on the current base
(HEAD `6d98bc46` on `53cb5d04`): `compare.OracleSig` already mirrors the parser's ALTER/rename
split and `supervisor.touchedParserOrOracle` re-runs golden on parser/oracle path changes — both
must be preserved untouched. Descriptor stays `V:1`; only shape *strings* change and only for the
lossy dimensions, so healthy `col_kind`/`col_params`/`table_qual`/`we_error` sigs do NOT re-bucket.

### 1. `internal/compare/compare.go` — bounded shape enrichment + identifier-leak fix (A + defect 3)

Change `FirstDifference` return values ONLY for the lossy dimensions; all other branches
(`stmt_kind`, `table_qual`, `position`, `spec_kind`, `col_name`, `col_count`, `col_notnull`,
`rename_pairs`, `col_params`) return exactly as today.

- **`stmt_count`** → `stmt_count(<oursKinds>≠<theirsKinds>)`, each side = `+`-joined statement-kind
  sequence over the parsed `[]sigStmt` (`.kind` ∈ {`alter`,`rename`}), capped at 5 entries with a
  trailing `…` when longer. Add helper `func stmtKinds(ss []sigStmt) string`. Example:
  `stmt_count(alter+rename≠alter)`.
- **`spec_count`** → `spec_count(<na>,<nb>;<oursSpecKinds>≠<theirsSpecKinds>)` where `na`/`nb` are
  the two specs lengths and each spec-kind side is the `+`-joined `.kind` sequence for THAT alter's
  specs (alphabet {`col`,`chg`,`ren`,`drop`}), capped at 6 with `…`. Add helper
  `func specKinds(specs []sigSpec) string`. Example: `spec_count(2,1;col+drop≠col)`.
- **`parse_our_sig` / `parse_oracle_sig`** (the two `Canonicalize` error branches in `Diff`, lines
  ~117-124): append the masked parse reason — `"parse_our_sig(" + NormalizeError(err.Error()) + ")"`
  and likewise for oracle. These live in `Diff`, not `FirstDifference`; edit both branches there.
- **`col_kind` identifier leak (defect 3)**: wrap the kind tokens through a new
  `func sanitizeKind(s string) string` that truncates at the first of ` `, `=`, `,`, `;`, `(` (i.e.
  keeps only the bare type token) and, if the result is empty, returns `"?"`. Apply it to BOTH the
  `col_kind(%s≠%s)` payloads. This closes the observed
  `col_kind(uint32, B=uint32≠uint32; col B=uint32)` leak. `col_params` already prints only ints — no
  identifiers — but assert that in a test rather than changing it.

Identifier-free guarantee: `stmtKinds`/`specKinds`/`sanitizeKind` emit only closed-vocabulary
tokens; column/table names never enter. No change to `Descriptor`, `DescriptorBytes`,
`DescriptorSig`, `MakeDescriptor`, `Canonicalize`, `OracleSig`, or the parser helpers.

**Cardinality (worst case per bug):** stmt-kind sequences over {alter,rename} capped 5 ⇒ ≤ (2^5+…)
bounded set; spec-kind over {col,chg,ren,drop} capped 6 ⇒ bounded; realistically ONE pair per root
cause. × ≤20 engine·mode slots ⇒ ≤ ~200 sigs absolute worst case per bug, tens realistic. Backstop:
existing 200-open-findings rate limit. Identifier variety cannot add sigs (proved by fuzz test).

### 2. `internal/findings/findings.go` — occurrence-preserving Record + meta round-trip (C)

In `Record`, replace the current unconditional meta merge (lines ~96-130) with occurrence-aware
logic. Define `repro` "rewritten this call" = `!existed || (existed && status=="fixed")`.

- **New / fixed→open (repro rewritten):**
  - `!existed`: write `repro.sql`, set `discovered_at`, `status="open"`, `minimized=false` (as today).
  - `existed && status=="fixed"` (**archive-on-reopen**): before overwriting, ring-archive the
    current `repro.sql` → `repro.prev1.sql` (shifting `prev1→prev2→prev3`, drop `prev3`; ring of 3)
    via a new `func archiveRepro(dir string) error`; then write the new `repro.sql`, set
    `status="open"`, increment `reopened_count` (int, via `intFromAny`), set
    `last_seen_at`. Also refresh `our_sig`/`our_error`/`oracle_digest` and merge `f.Meta` — the
    atomic pairing rule (repro + its meta move together) is satisfied because repro was rewritten.
- **Open rediscovery (repro NOT rewritten — `existed && status not in {fixed}`):** update ONLY
  `times_seen` and `last_seen_at`. Do NOT touch `our_sig`/`our_error`/`oracle_digest`/`shape` and
  do NOT merge `f.Meta`. This removes the internally-stale repro/meta pathology and the ~800k
  pointless rewrites per hot sig. (`status` stays whatever it was: open/ledgered/parked.)
- Always-updated invariants (both paths): `sig`, `engine`, `sql_mode`, `lane`, `class`,
  `descriptor_v`, `descriptor`, `times_seen`. `shape` is set on the repro-rewritten path only.
- **Raw-map meta round-trip (defect 2, findings side):** `Record` already reads `meta` as
  `map[string]any` and writes it back — it does not prune. Keep that. The `f.Meta` merge skip-list
  stays `{sig, times_seen, descriptor}`; ADD nothing that would drop `reopened_count`/`last_seen_at`.

Add `last_seen_at` (RFC3339 UTC) alongside `discovered_at` on every call.

### 3. `internal/e2echeck` + `e2e/checks.go` — e2e dimension shape (A, e2e slice)

Give every e2e class a bounded, identifier-free shape so they stop collapsing to `unspecified`.

- Add `func ShapeFor(class string, meta map[string]any) string` to `internal/e2echeck` (non-tagged
  file so `findings`/tests can use it without the ddlfuzz tag). Mapping, dimension-only:
  - `e2e-statusvar-walk`→`status-vars`; `e2e-sqlmode-mismatch`→`sql-mode`;
    `e2e-query-rewrite`→`query-rewrite`; `e2e-plumbing-sig`→`plumbing-sig`;
    `e2e-panic`→`parser-panic`; `e2e-parse-error-live-accept`→`parse-error-live-accept`;
    `e2e-position-missed`→`position-missed`.
  - `e2e-col-attr`→`col-attr(<attribute>)` reading the bounded `meta["attribute"]` ∈
    {rename_column_type, qkind, nullability, numeric_precision, numeric_scale}; `col-attr(?)` if absent.
  - `e2e-missed-column-effect`→`missed-effect(<effect>)` where effect ∈ {added, dropped, changed,
    benign} derived from which bounded key is present (`added_*`→added, `dropped_*`→dropped,
    `changed_unexpected`→changed, `benign_classification`→benign); `missed-effect(?)` otherwise.
  - default/unknown class → `""` (findings then falls back to its existing logic).
  Only closed vocabularies; never `column`/type strings/ordinals.
- In `e2e/checks.go` `recordE2EFinding`, before building the `findings.Finding`, if
  `meta["shape"]` is unset compute `s := e2echeck.ShapeFor(in.Class, meta)` and set
  `meta["shape"] = s` when `s != ""`. (findings.`shapeFromFinding` already honors a set
  `Meta["shape"]`.) Do not alter the required-meta list or capture fields.

### 4. `supervisor/fixloop.go` — meta round-trip + group-key coarsening (defect 2 + B/group)

- **`writeFindingMeta` raw-map round-trip (defect 2):** stop marshaling the typed `FindingMeta`.
  New behavior: read the existing `meta.json` into `map[string]any`, apply the caller's mutated
  fields, write back — so unknown keys (`times_seen`, `descriptor`, `origin`, `seed`,
  `submitted_text` and the other plan-50 e2e capture keys) survive supervisor status flips. Concrete
  approach: add `func writeFindingMetaFields(path string, fields map[string]any) error` that
  read-modify-writes the raw map, and route the supervisor's status/fixed_by updates through it.
  The call sites that today do `f.Meta.Status = X; writeFindingMeta(f.MetaPath, f.Meta)` become
  `writeFindingMetaFields(f.MetaPath, map[string]any{"status": X})` (and `{"status":X,"fixed_by":Y}`
  in `applySuccessfulStatuses`). Keep `loadFindingMeta`/`FindingMeta` for reads. Update
  `ParkSignature`/`confirmFixed`/`applySuccessfulStatuses` accordingly. If the raw file is missing,
  fall back to writing the typed struct (bootstrap case).
- **`GroupInfoForMeta` dimension-prefix key (B/group):** compute the group shape as the dimension
  prefix of `NormalizeShape(shape)` — text before the first `(`, trimmed. Add
  `func dimensionPrefix(s string) string`. So `stmt_count(alter+rename≠alter)` and
  `stmt_count(alter+alter≠alter)` both map to group shape `stmt_count`; `col-attr(nullability)`→
  `col-attr`. This keeps sibling grouping + flap observation at family granularity while sigs are
  per-bug. Apply the prefix in the branch where `meta.Shape` is set; the empty-shape fallback path
  is unchanged.

### 5. `supervisor/park.go` + `disk.go` — reopened_count wiring, reproduce-gated flap, soft freeze (defect 1 + B slices)

- **`reopened_count` field (defect 1):** rename `FindingMeta.RediscoveredCount`/`GroupedFinding.
  RediscoveredCount` → `ReopenedCount` with json tag `reopened_count` (the currently-dead
  `rediscovered_count` path is removed; the real key is now written by findings.Record §2). Update
  all references (`ScanFindings`, `ApplyFlapScanAndPark`, the test).
- **Reproduce-gated reopen counting (B slice i):** the flap trip must require the candidate to
  actually reproduce. `ApplyFlapDetector` stays a pure function but its `GroupedFinding` gains a
  bool `Reproduces`. Trip condition becomes: `open && g.FixCount >= 2 && (!previouslyKnown ||
  f.ReopenedCount > 0) && f.Reproduces` ⇒ set `g.Parked = true`. In `ApplyFlapScanAndPark`, compute
  `Reproduces` by running `RunReplay` ONLY for candidate sigs (open, in a group with `FixCount>=2`,
  and either new-to-group or `ReopenedCount>0`) and treating `ExitCode==10` as reproduces;
  `ExitCode==11`/`1`/error ⇒ does not count (no trip). This keeps replay cost bounded to genuine
  candidates. confirm-fixed already runs before the flap scan in `RunFixLoop`, so reconciled
  re-opens are marked `fixed` and never reach here.
- **Soft selection-freeze (B slice ii):** `ApplyFlapDetector` no longer returns a hard park list;
  it only sets `g.Parked=true` (the freeze) and returns the set of newly-frozen group keys.
  `ApplyFlapScanAndPark` then, for each newly-frozen group, writes ONE
  `escalations/run-flap-<groupKey>.md` (via `writeRunEscalation`) instead of per-sig escalations,
  and does NOT append member sigs to `parked.list`, does NOT set member meta `status="parked"`.
  Arrivals keep being recorded (`times_seen` counts) and metas stay `open`. `SelectFinding` already
  skips `g.Parked` groups (unchanged) so frozen groups are excluded from selection — that IS the
  freeze. Hard parking (`parked.list` + per-sig escalation via `ParkSignature`) remains ONLY for the
  budget-exhaustion path in `FixOnce` — leave `ParkSignature` and those call sites intact.
- Remove the now-unused hard-park plumbing that `ApplyFlapScanAndPark` used for flap (the
  `toPark`→`ParkSignature(..., flap=true, "flapping group")` loop). Budget-exhaustion parking is
  separate and stays.

### 6. `supervisor/prompt.tmpl` — content-hash seed naming (C item 3)

Change the seed instructions so a second fix for the same sig ADDS a seed instead of clobbering:
seed path becomes `tools/ddlfuzz/seeds/{SIG}-<sha8>.sql` (+ matching `.meta.json`), where `<sha8>`
is the first 8 hex of sha256 of the exact repro bytes. Note the `repro.prev<N>.sql` archives exist
and may be cited. No loader change is required: `seed.LoadDir` reads only `seeds/seeds.jsonl` (which
`seed.Extract` regenerates from the parser test files, so the appended regression test case is what
actually enforces the fix); the committed `seeds/<sig>*.sql`/`.meta.json` files are standalone
regression artifacts. Content-hash naming simply stops a second fix for the same sig from
overwriting the first fix's committed `.sql` — a prompt-only change; do NOT touch `seed.go`.

### Interactions / migration

- **No descriptor bump.** Only stmt_count/spec_count/parse_*/e2e shapes re-bucket; healthy sigs are
  byte-identical ⇒ their on-disk dirs and `groups.json`/`parked.list` entries stay valid. Old parked
  sigs remain suppressed under their old identities; the underlying bugs resurface as fresh fine
  sigs with new attempt budget (the intended "unpark by re-bucketing" of the 10 zero-attempt bugs).
  Old `groups.json` keys go inert. No state rewrite.
- **minimize:** `FastLanePredicate` preserves descriptor equality; finer shapes make shrinking
  stricter (can't cross count-families) — correct, no change needed.
- **plan 50 (e2e replay):** §3 gives e2e findings real shapes; §4 meta round-trip is what lets
  plan-50 capture fields survive supervisor status flips (previously pruned). For the flap
  reproduce-gate, an e2e sig whose replay can't be evaluated (`exit 11`) simply doesn't count.
- **plan 51 (confirm-fixed):** runs before the flap scan (unchanged ordering in `RunFixLoop`); §5's
  gate only sees genuinely-reproducing re-opens.
- **base OracleSig/touchedParserOrOracle:** untouched; golden must stay `irreconcilable:0`.

### Test plan

Unit (in-package, no live system):
- `compare`: table-driven enriched `FirstDifference` — stmt_count/spec_count kind sequences + caps;
  identifier-free guarantee (feed varying quoted identifiers into both sides, assert shape byte-
  identical); `parse_*` masked-reason branch; `col_kind` leak regression (a payload that previously
  leaked `B=` now yields a bare-token `col_kind`); assert `col_params` carries no identifiers.
  Preserve `TestDescriptorStability`; add golden `DescriptorSig` assertions that `col_kind`,
  `table_qual`, `we_error` sigs are UNCHANGED vs current values (compute-and-pin) to prove no
  accidental re-bucketing.
- `findings`: open-sig rediscovery leaves `repro.sql`+`our_sig`+`oracle_digest`+`shape` untouched
  and bumps `times_seen`/`last_seen_at`; fixed→open archives to `repro.prev1.sql`, bumps
  `reopened_count`, and rewrites repro+meta together (assert consistency); archive ring caps at 3
  (prev4 never appears); raw meta keys (e.g. `submitted_text`) survive a rediscovery.
- `e2echeck`: `ShapeFor` table over all classes incl. the bounded `col-attr(<attr>)` /
  `missed-effect(<effect>)` vocabularies and the `?`/`""` fallbacks; identifier-free.
- `supervisor`: `writeFindingMetaFields` round-trips unknown keys (fixture meta with `times_seen`,
  `submitted_text`, `descriptor`); `GroupInfoForMeta` dimension-prefix (`stmt_count(...)` ≡
  `stmt_count`, `col-attr(nullability)` ≡ `col-attr`); update/extend `TestFlapDetector` for the new
  semantics — (a) 2 fixes + reproducing new sig ⇒ group frozen, EMPTY hard-park list, arrivals not
  in parked.list; (b) reopen not reproducing ⇒ no freeze; (c) `ReopenedCount≥1` + reproduces in a
  fix_count≥2 group ⇒ freeze; (d) frozen-group open arrival is skipped by `SelectFinding` but its
  meta stays `open`.
- Pathology fixture: two synthetic canonical-sig pairs that today both yield `stmt_count` — assert
  they now produce DISTINCT `DescriptorSig`s (distinct identity ⇒ distinct seed names, independent
  budgets), so fixing one cannot re-open the other.

Golden / integration: `./build/ddlfuzz golden` MUST stay `irreconcilable:0` (behavior-preserving —
only shape strings of divergences change, and golden seeds are reconciling cases). `ddlfuzz replay`
on a scratch fixture finding returns the enriched shape in JSON with the right exit code.

Acceptance: standard gate green — untagged `go build ./... && go test ./...`; `-tags ddlfuzz`
build+vet+test across `./internal/... ./e2e/... ./supervisor/...`; `flow` `-tags ddlfuzz` build;
`./build/ddlfuzz golden` == `irreconcilable:0`. Post-merge, one supervisor cycle shows no
park-on-arrival, dimension-prefix group keys, and enriched stmt_count/e2e shapes on new findings.
