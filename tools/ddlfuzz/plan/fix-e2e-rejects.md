# fix-e2e-rejects — eliminate self-inflicted e2e exec-rejects

Measured from `state/e2e-exec-rejects.jsonl` (1% systematic sample, 14,736 records): 54% ERROR
1060 duplicate column (name histogram == the 16-entry `FreshNames` roll call), 26% 1064 syntax
(of which ~12% is ` VISIBLE` on MariaDB — MariaDB has no VISIBLE column keyword — and a slice is
the unquoted ORACLE/MSSQL fallback), 7.6% 1067 + 2.4% 1101 (`DEFAULT 'x,y'` on non-string
types), 3% 1054 (partly the hardcoded ``AFTER `first``` fallback), 1.5% 1050 (rewrite maps
rename-pair NEW sides to `fixture` → `RENAME TABLE fixture TO fixture`). Goal: kill only the
structurally-guaranteed failures; hostile fuzz (mode-gated grammar, hostile spellings) must keep
flowing.

All paths relative to `tools/ddlfuzz/`. Do NOT touch `e2e/worker.go` (sibling branches own it;
the design deliberately needs no worker change). Do NOT touch `plan/99-gaps.md`.

## 1. `internal/gen/gen.go` — fresh-name allocator (kills the 54% class)

`genAlterSpec` is called only from `GenerateConstrained` (e2e lane); `Generate`/fast lane uses
the tables and is out of scope — leave `randomVocab`, `normalizeVocab`, `genRename` untouched.
`Vocab.Columns` is already the live snapshot column list (worker refreshes it per case), so the
filter lives entirely in gen and stays engine-agnostic.

- Add to `Ctx` (unexported fields): `freshUsed map[string]bool`, `colSet map[string]bool`.
- Add `func (c *Ctx) freshIdent() string`:
  1. Lazy-init: `colSet` from `c.V.Columns`, `freshUsed = map[string]bool{}`.
  2. Candidates = entries of `c.V.FreshNames` not in `colSet` and not in `freshUsed`.
  3. If candidates non-empty: pick uniformly with `c.R`. This keeps the hostile palette names
     (`` new`tick ``, `spelled name`, keyword-shaped) appearing whenever they're free — palette
     always preferred over synthesis; after a fixture reset all 16 are free again.
  4. Else synthesize: `fmt.Sprintf("nf%d_%d", len(c.freshUsed)+1, c.R.Uint64N(1_000_000))`,
     retry while the name is in `colSet` or `freshUsed` (same spirit as
     `e2e/fixture.go fallbackFreshName`'s `n%d_%d` alternate). Bare-safe ASCII by construction.
  5. Mark `freshUsed[name] = true`, return the raw name (caller quotes).
- In `genAlterSpec` replace every fresh draw with the allocator (each wrapped in `quoteMaybe`):
  - the shared `fresh := quoteMaybe(c, pickString(c.R, c.V.FreshNames))` at the top — draw it
    lazily per branch instead (only branches 0-2, 3, 6-7, 9 consume a fresh name; the top-level
    eager draw would burn allocator entries on DROP/MODIFY/INDEX branches),
  - the second name in the `ADD (a t, b t)` branch (case 3) — this is the intra-statement dup
    source, both names must come from the allocator.
  `freshUsed` persists across the up-to-`MaxSpecs` specs of one statement (Ctx is per-call), so
  one statement never adds the same name twice; across cases the fresh snapshot in
  `Vocab.Columns` does the filtering.
- `genAttrsSimple(c *Ctx)` → `genAttrsSimple(c *Ctx, typ string)` (constrained-lane-only helper;
  all 3 call sites are in `genAlterSpec` and pass the spec's type):
  - base attrs (always valid): `""`, `" NOT NULL"`, `" NULL"`, `" DEFAULT NULL"`,
    `" COMMENT 'ddlfuzz'"`, `" INVISIBLE"`.
  - `" VISIBLE"` only when `!c.IsMariaDB` (MariaDB rejects it in every mode — measured ~3% of
    all rejects).
  - `" DEFAULT 'x,y'"` only for the comma-tolerant string family: type has prefix `varchar`,
    `char`, `varbinary`, or `binary` (keeps the comma-in-default parse pressure alive where it
    can actually execute).
  - `" DEFAULT 1"` for the numeric family: prefix `int`, `bigint`, `tinyint`, `decimal`,
    `double`, `bit`.
  - everything else (text/blob/json/geometry/enum/set/temporal/uuid/inet/vector): base attrs
    only (1101 forbids defaults on blob-likes; 1067 on the rest).
  Match on `strings.HasPrefix(typ, ...)` after lowercasing; unknown types → base attrs.

## 2. `e2e/rewrite.go` — snapshot-aware rewrite targets (rest of 1060, plus 1050)

- Add a small allocator (can live in `rewrite.go`):
  `type freshAlloc struct { free []string; used map[string]bool; seq int }`,
  `newFreshAlloc(live snapshot)` → `free` = `FreshNames` entries not in `live` (order
  preserved: palette preferred), `next()` pops the first free entry, else synthesizes
  `fmt.Sprintf("nf%d_r%d", seq, seq)`-style names checked against `live` and `used`.
  Deterministic (no rng) — keeps `rewriteCorpusStatement` signature/test behavior simple.
- Replace the `FreshNames[freshIdx%len(FreshNames)]` loop (rewrite.go:57-65) with
  `alloc.next()` per distinct parsed new column. Also skip names already mapped in `oldMap`
  (existing behavior) and names already live-and-identical (a parsed new name that equals its
  own old name maps through `oldMap` untouched — keep current logic).
- Rename-table mapping (kills 1050): today `parsedTables` returns old AND new pair sides and
  all get replaced with `fixture`. Split:
  - `parsedOldTables`: `alter_table` `stmt.Table` + `pair.OldTable` → all replaced with
    `fixtureTable` (unchanged semantics).
  - `parsedNewTables`: `pair.NewTable` values not already in the old set (rename chains
    `a→b, b→c`: `b` counts as new — single consistent textual mapping handles the chain) →
    each distinct name maps to a distinct fresh table name `fmt.Sprintf("fixture_r_c%d", i)`
    (worker's reset policy already handles renamed-away fixtures: `shouldReset` →
    `table-renamed`).
  - If any pair has `OldTable == NewTable` (self-rename — guaranteed 1050), return
    `("", false)`: drop the candidate.
- Shape verification (existing `shapeSignature` check) is name-insensitive — unchanged.

## 3. `e2e/fixture.go` — fallback fixes

- `simpleFallbackDDLForMode` (ORACLE/MSSQL branch): emit the fresh name ANSI-double-quoted —
  both MariaDB composites expand to include ANSI_QUOTES (verified from live readbacks in the
  reject log), so `"spelled name"` / `` "new`tick" `` are valid identifiers there; backticks
  were the thing to avoid, not quoting. Add
  `func ansiQuoteIdent(s string) string { return `"` + strings.ReplaceAll(s, `"`, `""`) + `"` }`
  (next to `quoteIdent` in `types.go` or locally): the branch becomes
  `"ALTER TABLE fixture ADD COLUMN " + ansiQuoteIdent(name) + " int"`.
- `simpleFallbackDDL`: the `AFTER` target is hardcoded to `first`, which drift can drop
  (measured 1054s). Use `first` only if present in the snapshot (`s["first"]`), else the first
  entry of `canonicalColumns(s)`; if that's somehow empty, omit the suffix.
- `fallbackFreshName` unchanged.

## 4. Tests

`internal/gen` (same package as `tables_test.go`):
- `freshIdent`: with all 16 palette names in `Columns` → 10 draws yield 10 distinct
  synthesized names, none in `Columns`; with one palette name free → it is returned before any
  synthesis; after removing names from `Columns` (fresh Ctx — simulates reset) palette names
  return.
- Intra-statement: `GenerateConstrained` with `Vocab{Columns: <fixture cols>, FreshNames:
  <one free name>}`, seeded rng, many iterations: for statements containing `ADD (`, the two
  column names inside differ (regex on the known emission shape).
- Statement-level no-dup-against-snapshot: many `GenerateConstrained` runs with all palette
  names in `Columns`; assert no emitted `ADD COLUMN <name>` / `ADD (<name>` / `CHANGE ... <name>`
  / `RENAME COLUMN ... TO <name>` uses a `Columns` entry (regex-extract the fresh positions, or
  assert none of the palette names appear backtick-quoted immediately after those keywords).
- `genAttrsSimple`: over many draws — no `VISIBLE` when IsMariaDB; `DEFAULT 'x,y'` only for the
  string family; `DEFAULT 1` only numeric; blob/text/json/enum/temporal get base attrs only.

`e2e`:
- `rewriteCorpusStatement`: corpus stmt adding a name that exists live → rewritten to a free
  palette name; live snapshot containing all 16 palette names → synthesized names (distinct, not
  live); rename `t1 TO t2` → `fixture TO fixture_r_c0` (new side ≠ `fixture`); self-rename
  `t TO t` → rejected; existing tests in `rewrite_queue_test.go` stay green.
- `simpleFallbackDDLForMode`: for `seq` hitting indices 13/14 with an ORACLE and an MSSQL mode
  entry → statement contains `"new` + "`" + `tick"` / `"spelled name"` and no bare hostile name;
  non-ORACLE/MSSQL path unchanged.
- `simpleFallbackDDL`: snapshot without `first` → `AFTER` target is a snapshot column (or no
  suffix); snapshot with `first` → `` AFTER `first` `` preserved.

## 5. Validation

From `tools/ddlfuzz/`: `go build ./... && go vet ./... && go test ./...`
(plus `golangci-lint run ./e2e/... ./internal/gen/...` if installed).
If the flow module fails on missing generated protos, copy `flow/generated/` from the main
checkout `/Users/ilia/Code/peerdb/flow/generated/` (read-only source) into this worktree.

## 6. Expected impact (of the ~42% reject rate)

- 1060 (54% of rejects): ~eliminated (residual: corpus statements that intrinsically dup a
  parsed name — same name maps to same target).
- 1067+1101 (~10%): ~eliminated.
- MariaDB `VISIBLE` (~3%): eliminated.
- 1050 (1.5%): ~eliminated; ORACLE/MSSQL fallback slice: eliminated.
- 1054 (3%): partially (fallback AFTER fixed; within-statement DROP-then-reference conflicts
  remain — see follow-ups).
- Net: rejects down ~70%; lane reject rate ~42% → ~12-15%, dominated by intended hostile 1064.

## Follow-ups (out of scope, do not implement)

- Within-statement spec conflicts: `DROP COLUMN x` followed by a spec referencing `x`
  (1054/1091), double-DROP of the same column — needs per-statement column-state tracking in
  `genAlterSpec`.
- Rewrite `oldMap` cycling can map two distinct old columns onto the same live column when
  distinct-old count exceeds live column count (wrap) — rare.
