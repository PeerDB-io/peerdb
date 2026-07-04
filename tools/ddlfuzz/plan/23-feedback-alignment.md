# 23 — feedback alignment: fix retention, reseed corpus, standard mutation

Successor to plan 22. That plan opened emission reachability; this one fixes the feedback loop.
No new abstractions: the changes are the standard fuzzing toolkit — retain what's actually
interesting with exact attribution, keep the corpus grammatical, mutate with havoc + splice +
dictionary.

## Diagnosis (2026-07-04, hours after plan 22 landed)

Plan 22 behaved as its rescope predicted: step jump (+715 mysql / +1,138 maria edges, 27
findings in 20 min), re-plateau within ~2h. But the coverage-feedback half never participated:

- Retention keeps the 256 **smallest** cases of a window of up to 32k when the window grew
  coverage (`fuzzloop.go:537-543` — "attribution is window-level guesswork"). Smallness
  preference is fine (AFL-cmin logic) — smallness *instead of attribution* is not: the smallest
  of 32k are always mutation fragments, never the case that opened the edge.
- Result: corpus.db = 160k entries averaging 11–12 bytes (` NERGE `, `/M! */`, `)C \n`);
  gen-origin retentions lifetime 834 of 160k, **zero** post-22. Step E then loads this pool into
  `es.bases`, so 60% of ~1M execs/s (`mutRatio`) mutates exhausted noise.
- The objective-aligned per-case signals — oracle digest, our sig/error — are computed in
  `processBatch` (`fuzzloop.go:444-451`) and discarded.
- The `go` edge counter is hardwired 0 (`fuzzloop.go:618`): the parser under test was never
  instrumented.
- `mutate.Crossover` (splice) is dead code; no sql_mode op (99-gaps G1); the dictionary has no
  comment delimiters or version gates, though comment semantics are the campaign's best-yield
  bug class (7da624d0, 87bb570b, 08351af6, skip rule G3).

## Design

### 1. Retention with exact attribution

Two signals, both per-case:

- **Behavior novelty** (primary). Key from what `processBatch` already has:
  `(engine, mode, server verdict, digest shape, our sig shape, our error class, panicked?)` —
  shapes identifier-stripped via the existing `sigIdent` vocabulary (`compare.go:679`); new
  `compare.BehaviorKey(...)`. Per-engine in-memory seen-set (cap 1M keys; when full, stop
  inserting, bump a stat). New key → retain. Shaping costs a signature parse, so a first-level
  seen-set on the raw inputs (64-bit hash of the strings already in hand) short-circuits repeat
  behaviors — steady-state cost is a map probe; shaped keys are computed only on raw misses.
  Both-reject keys (server reject + our error) count toward the noise budget (§2), so junk
  can't recolonize.
- **Oracle edges** (secondary). Keep window polling and `grew`; change selection only: retain
  the smallest **server-accepted** cases in the window (cap 64) instead of smallest-overall.
  Verdicts are already in hand per case; one filter. A rejected case that opens new code isn't
  attributed here — its behavior key (new error class) retains it instead. Attribution stays
  approximate — acceptable once behavior keys carry the primary load. Exact bisect needs fresh
  oracle processes (cumulative counters); deferred unless the tier stays noisy.

Retained rows get a `signal` tag (`behavior` / `oracle-edge` / `noise`) — corpus.db additive
column, `init()` adds when missing.

### 2. Corpus hygiene

- One-off `ddlfuzz corpus-migrate`: stamp all pre-existing rows `signal='noise'`.
- `loadCorpusBases`: behavior/oracle-edge tiers in full (existing 256 MB cap); noise tier
  reservoir-sampled under a fixed 12.8 MB budget (5% of the pool byte cap).
- `seed.LoadDir` also loads committed per-fix `<sig>.sql` regression seeds (closes 99-gaps D5).
- Recency ring unchanged — it now rings over grammatical retentions by construction.

### 3. Mutation: havoc + splice + dictionary

Keep the existing token/byte havoc ops. On a grammatical corpus they produce near-misses of
valid statements — the P2/P3/P4/P7 boundary class — instead of deeper junk. Add:

- **Splice**: revive `mutate.Crossover` as a mutation arm — cut two corpus entries at random
  token boundaries, join. Standard AFL splice; on valid DDL it manufactures the spec-adjacency
  and clause-ordering collisions (P3, P7) no single generated statement emits.
- **Tuple flip**: mutate the non-SQL part of the case — flip one sql_mode bit (G1), or flip
  engine (same bytes, other oracle: targets the `/*!` vs `/*M!` asymmetric-execution contract).
  Cheapest divergence A/B there is.
- **Dictionary extension**: this is where the comment/lexer wisdom goes — not new operators,
  dictionary entries. Add to `internal/dict`: `/*`, `*/`, `/*!`, `/*M!`, `/*+`, version gates
  derived from the *actual oracle versions* (±1, exact, 5- and 6-digit forms, and a
  never-executed `/*!99999` — today's decorator constants all sit below both servers, so
  skipped-content forms are at probability zero), `-- `, `#`, `'`, `` ` ``, `"`, `;`, and
  multi-token clause phrases (`REMOVE PARTITIONING`, `WAIT 3`, `NOWAIT`, `IF NOT EXISTS`).
  Give `mutate` a dict-insert op (insert at token boundary) beside the existing substitution.
  Unbalanced delimiters land both-reject almost surely; behavior keys collapse that into a few
  keys and the noise budget bounds it — no special casing.

Budget inside the mutation lane (`-mut-ratio` semantics unchanged): grammatical-tier bases 95%,
noise-tier 5%.

### 4. Instrument the target

Build the fuzzer binary with `-cover` (supervisor build path + `build/` script); snapshot
`runtime/coverage` counters per window, diff newly-nonzero positions as an opaque vector — no
symbolization, no new deps. Retention as §1-secondary. If the binary lacks instrumentation,
disable the signal and log once. A few hundred edges that saturate fast — but they're edges of
the code we ship, and the inputs opening the last of them are the mutation bases we want.

### 5. Stats (additive, D12)

`behavior_keys` per engine, `retained_by_signal`, `seen_set_full`, plus median SQL length of
rows retained in the last window — the one-glance junk detector.

## Implementation steps

All in this worktree (`worktrees/feedback-alignment`), inside `tools/ddlfuzz/` plus the one
supervisor build-flag change. Gate after every step: `gofmt -l . && go build ./... && go vet
./... && go test ./...`. Land via `ddlsuper merge-staged` (campaign live).

- **A — behavior keys.** `compare.BehaviorKey` + tests over golden seed digests; seen-set in
  `engineState`; retention hook after `Diff`; `signal` column migration; stats.
- **B — corpus hygiene.** `corpus-migrate`, tiered `loadCorpusBases`, per-fix seed loading.
  Test: pool composition respects tier budgets on a fixture DB.
- **C — mutation.** Splice arm, tuple-flip op, dict entries + dict-insert op. Byte-safety fuzz
  test per op (existing invariant: re-validate or drop).
- **D — oracle-edge selection + go cover.** Accepted-only window retention; `-cover` build flag
  (`preflight.go` build lines + `build/`), window snapshot, flag-guarded.
- **E — verify + land.** Full gate; e2e lane untouched (`GenerateConstrained` unchanged — no gen
  changes at all in this plan); `replay --all` green; roll onto `ddlfuzz-staged`, `ddlsuper
  merge-staged` (backgrounded); watch the first post-land hour of `retained_by_signal` and
  median retained length.

## Constraints (binding)

- `internal/gen` untouched; `gen.GenerateConstrained` unchanged (30 §Interfaces).
- stats.json additive only (D12); corpus.db schema additive, old rows readable.
- Wire protocol untouched. Byte-safety invariant everywhere; sentinel never emitted.
- `-mut-ratio` flag semantics unchanged.

## Dropped

- Recipe-carrying generation and recipe-level mutation ops (earlier draft): structure
  preservation is what dictionaries + splice on a grammatical corpus already buy, without a
  parallel representation to keep honest. A flat-SQL corpus also can't *lose* an interesting
  decoration — the retained case is its bytes.
- Per-construct accept telemetry and the `gen-audit` adjacency tool: nice-to-have observability,
  not on the critical path. Revisit only if the reseeded loop goes quiet in a way the stats
  can't explain.
- Bandit/adaptive grammar weights; deeper expression grammar; exact edge bisect; external
  corpora (mysql-test harvesting needs a 00 constraint 2/3 call) — unchanged from before.

## Exit metric

Within hours of landing: behavior-tier retentions flowing with a gen+mut mix; median retained
length ≥ ~40 bytes; noise share ≤ 5%. Then, when new-key discovery decays to zero under an
aligned signal and a grammatical corpus, quiet means converged — that is the deliverable.
