# 25 — per-statement behavior features

Successor to plan 24 Part I. I.1 bounded each *field* of the coarse tuple but hashed the
*product* of everything across a multi-statement chain into one feature. Rebuild feature
emission the AFL way, properly this time: features are independent small observations, each
drawn from a bounded per-statement space — the fams trick (24 I.1's family-set escape hatch),
extended to statements.

## Diagnosis (2026-07-04, hours after the errno-class fix landed)

The errno fix (1be00ddc) worked — oracle reject classes collapse to `errno:` templates —
and retention flooded straight through the next unbounded field: the structural class
(`feature.go` feature 0). `flatShape` folds one side's whole chain into a single tuple:
first-4 statement kinds (3⁴) × chain-summed spec-count vector (4⁵ = 1024) × chain-ORed
qual/pos/renameTo flags × statement-count and pair buckets — then feature 0 hashes *both
sides'* tuples together with mode/verdict/error. ~10M combinations per engine, and mutated
junk chains (`alter X{…} | rename a>b | alter Y{…} | …`) sample it near-uniformly: every
shuffle of drops and cols across a chain is a "new structural class."

Measured on the live campaign: ~230K distinct classes in 20 min, 99.3% injective over
retained rows, bitmap at 12% and climbing (26% maria / 20% mysql by the time of writing);
`retained_per_min` ≈ 17K; the retention storm holds `execs_per_sec_window` at ~10–19K/s.
The campaign binary (8b4a6aff) has the errno fix but not this one, so the corpus accretes
structural-class junk until this lands.

The lesson, stated as the design rule: a chain's shape is input-shaped, not behavior-shaped.
No feature may hash a cross-statement product.

## Design

### 1. Statement-shape features (replaces `flatShape`)

Per statement, one shape — kind byte, that statement's own spec-count vector (5 categories,
bucket4 each), its own qual/pos flags, pair-count bucket (rename kind). Shapes go
through a `famSet` twin (`shapeSet`, cap ~16, first-seen order, overflow feature), **shared
across both sides of the case**; each distinct member emits one feature:
`(engine, masked mode, 'S', shapeHash)`.

- `digestShape` and `sigShape` keep their scanners but build per-statement shapes with
  byte-identical hash input, so when the sides agree (the common case) they collapse to the
  same features; a diverging side contributes its own. No side tag, no second key build.
- Parity requires `digestShape` to mirror `OracleSig`'s rename normalization: a non-noop
  ALTER..RENAME TO splits into the alter shape plus a trailing standalone rename shape
  (pairs=1), and a spec-less alter is dropped — the sig renders exactly that split, so a
  digest-side `renameTo` flag would permanently double the shapes of agreeing sides.
- A 12-statement chain contributes ≤12 features from a per-(engine,mode) space of
  ~4K per kind-'a' statement (1024 counts × 4 flags) — finite and saturable, worst case
  ~6% of the 2^20 map at full mode palette, realistically far less. Dedup collapses across
  chains that share statements.
- Adjacency (what option B would have lost): kind-bigram features
  `(engine, mode, 'B', kind[i], kind[i+1])` over the uncapped kind sequence — alphabet
  {a,r,?,s} → ≤16 combos per mode. Keeps multi-statement ordering signal at zero risk.

### 2. Chain residue (feature 0)

Feature 0 keeps only the bounded non-shape fields, unchanged in kind: engine, masked mode,
oracle verdict byte + errno class (reject) / masked error (fallback), our-error class,
panic class — plus, replacing each side's `flat()` fold, that side's statement-count
bucket only. `flatShape`, `flat()`, and `maxCoarseStmts` combinatorics are deleted.
Reject-path behavior is identical to today (shape never contributed there).

### 3. Case key for stamping and distill

Feature 0 is now deliberately coarse, so it can't be the corpus stamp (budget eviction
groups behavior rows by `(engine, feature)` — `corpus.go` `DuplicateRows`; distill groups
noise rows the same way). New exported fold: `CaseKey(feats []uint64) uint64` — FNV over
the emitted features in order. `retainBehavior` stamps the fold; `BehaviorFeature`
(distill's key, `distill.go`) becomes the fold of the d=nil feature set. Strictly better
grouping than today at both ends: collapses cases with identical feature sets, still
separates genuinely distinct ones. Old stamped values remain valid grouping keys for old
rows (they're opaque text).

### 4. Bitmap invalidation

Keying changes ⇒ persisted bitmaps are meaningless under the new indexes, and the flood
already wrote ~500K junk bits that would suppress ~a-quarter of the new keyspace at random.
No code: path and format stay. Rollout deletes the stale files once —
`rm state/coverage/{mysql,mariadb}.behavior` at the restart boundary (old fuzzer stopped,
new binary not yet loading) — and the heartbeat rewrites them fresh.

### 5. Flood-window corpus cleanup (one-off, deferred)

Behavior-tier rows retained between the plan-24 landing (c840b486) and this landing are
majority structural-class junk with near-injective stamps budget eviction can't dedupe.
One-off `ddlfuzz corpus-distill -behavior-since <ts> -behavior-until <ts>`: re-key rows in
the window with the new fold (same offline re-parse as noise distill), keep smallest per
key, delete the rest. Full behavior-tier distill stays deferred pending the 24 I.4 A/B;
this touches only the window whose provenance we know is flooded. Execution deferred to
after the landing's watch window (same reasoning as 24 §Sequencing: don't move the corpus
under the canary).

### 6. Stats (additive)

`fresh_bits_by_kind` per engine — {chain, stmt, bigram, family} counters incremented when
a fresh bit is set, so the *next* runaway feature class is visible by name instead of via
another 500K-row post-mortem. Existing `behavior_bits` saturation stat unchanged.

## Implementation steps

All in `worktrees/stmt-features`, inside `tools/ddlfuzz/`. Gate after every step:
`gofmt -l . && go build ./... && go vet ./... && go test ./...`. Campaign live: nothing
committed in the main checkout; land via merge-staged.

- **A — feature emission** (§1, §2): `stmtShape` + shared `shapeSet` + bigrams in
  `feature.go`; per-statement scanners in `digestShape`/`sigShape`; chain residue; delete
  `flatShape`/`flat`. Parity test: for fixture (digest, sig) pairs describing the same
  statements, both sides emit identical shape-feature sets (extend `feature_test.go`).
  `BehaviorFeatures` gets a kind-tagged return or emits kinds via a callback only if the
  stats need it cheaply — decide in code; no allocation on the hot path
  (`behaviorkey_bench_test.go` stays ≤1µs, 0 allocs).
- **B — fold + call sites** (§3): `CaseKey`; `retainBehavior` stamps it;
  `BehaviorFeature` → fold; `distillFeatureKey` unchanged semantics.
- **C — stats** (§6): `fresh_bits_by_kind`.
- **D — probe validation**: `KEYSPACE_PROBE=1` re-run over the same 500K-row sample
  (read-only against the live corpus.db). Expect: distinct features ≤ ~20K combined,
  behavior-group classes in the low thousands, singleton share collapsing from 99.3%.
  A miss means a residual product — find it before landing, not after.
- **E — flood-window distill** (§5): window flags on `corpus-distill` + fixture-DB test.
  Command lands with the rest; **execution deferred** until the watch window closes.
- **F — verify + land**: full gate; `replay --all` green; roll onto `ddlfuzz-staged`;
  `ddlsuper merge-staged` (backgrounded); stale-bitmap `rm` (§4) at the restart
  boundary. Watch: `execs_per_sec_window`,
  `retained_per_min` (tapering, not zero), `behavior_bits` saturation growth,
  `fresh_bits_by_kind` composition, finding rate.

## Constraints (binding)

- `internal/gen`, `e2e/`, oracle drivers, wire protocol untouched. Go-only diff in
  `internal/compare`, `internal/fuzzcmd`, `internal/corpus` (distill window flags).
- stats.json additive (D12); corpus.db schema unchanged — `feature` column semantics
  (opaque grouping text) preserved; old rows readable and groupable.
- Feature hashes stay FNV-1a with the standard basis — process-stable, bitmap persists.
- If an option-B stopgap (first-statement-only tuple + length bucket) lands first, this
  plan supersedes it wholesale: both replace `flatShape` emission at the same seam, and
  the stale-bitmap delete at rollout makes the transition order-independent.

## Dropped

- Positional statement features (shape × position index) — position is input structure;
  bigrams carry the ordering signal at 1/500th the space.
- Spec-kind *order* within a statement — same argument one level down; the count vector
  is the observation.
- Full behavior-tier distill — still pending the 24 I.4 A/B; only the flood window moves.
- Any scheme that re-couples sides or statements into one hash (incl. "hash the sorted
  shape multiset as one feature") — that's the product again, one hop removed.

## Exit metrics

Within hours of landing: `execs_per_sec_window` recovers toward the plan-24 target
(≥800K/s steady state); `retained_per_min` drops orders of magnitude and tapers;
fresh bitmaps sit in low single-digit % saturation after a day with `stmt` bits the
dominant early kind, flattening. Finding rate at worst unchanged from the flood regime.

Step D probe, recorded 2026-07-04 (live corpus, 300K behavior + 16K noise rows sampled):
behavior 14,790 distinct features with 4,282 singletons (1.4% of rows; the old key was
99.3% injective) and head-heavy class sizes (top: 18,762); noise 2,495; combined 16,232
features → 16,103 bit indexes, **1.54%** of the 2^20 map (the flood had the old map at
12% within 20 minutes).
