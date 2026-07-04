# 22 — generator completeness upgrade

Successor plan for 99-gaps G1 + D6. Goal: open the input-space regions the current
generator/mutators cannot reach at all.

Rescope principle: at ~400k execs/s (2.8B execs to date), randomness is free and reachability
is everything. Any random combination of pieces the generator *can* emit has been exhaustively
sampled; new oracle edges come only from syntax at probability zero today. Items are ranked by
that test; probability-reweighting machinery from the earlier draft is dropped (see end).

## Diagnosis (unchanged)

Coverage plateaued (mysql 21,656 / maria 10,610 edges, flat) with `internal/gen/gen.go` at a
small fraction of plan 21 step 12: single-statement ALTER/RENAME, 12-way spec switch, ~20
types, ≤1 attribute, fixed vocab, backtick-only quoting. sql_mode is sampled *after* the SQL
(`fuzzloop.go:252-255`), so mode-gated syntax can never be emitted. `dictionarySub` injects
single keywords, but multi-token grammatical sequences are unreachable via 1–3 random token
ops — and the found parser bugs (P3/P4/P7) are exactly multi-token interaction bugs.
Corroboration: maria trails mysql by 2× on a *larger* grammar — the maria-only constructs are
precisely the unimplemented ones.

## Work items

### 1. Grammar table completion (biggest edge mass)
Encode the 21-fuzzer checklist (~150 constructs) as data: one weighted table entry per item,
each with a stable `ConstructID` and engine/mode gates. A unit test walks the checklist and
asserts every ID has an entry — completeness becomes testable. Richest missing sub-grammars,
in rough edge-mass order: partitioning ops, FK/REFERENCES clauses, table_option runs, index
options, GENERATED/CHECK columns, PERIOD/SYSTEM VERSIONING, full type-synonym table
(INT4/MIDDLEINT/DEC/FIXED/NCHAR-family/LONG VARCHAR/CHAR BYTE/SERIAL), attribute zoo (0–4 per
column). Statement-level structure is part of this item: ~5% benign heads, `;`-chains of 0–3
statements (exercises truncate-at-first-other), SET STATEMENT wrapping (multi-var, expression
values, quoted `FOR`), multi-pair RENAME [TABLES] with per-pair WAIT/NOWAIT, IF [NOT] EXISTS /
ONLINE IGNORE head combos, and the LL(2) ambiguity forms (`ADD period…`, `ADD VECTOR…`,
`ADD SYSTEM…`). Identifier pools draw from `internal/dict` tokens so keyword-shaped names
(`first`, `after`, `period`, `key`, …) ride along at zero marginal structure; sentinel db
stays excluded (test-asserted).

### 2. Expression generator (bounded)
Small recursive generator, depth ≤3, for `DEFAULT (expr)`, `CHECK (expr)`,
`GENERATED ALWAYS AS (expr)`, partition `VALUES` — literals (incl. introducers
`_utf8mb4'…'`/`X'…'`/`0x…`/`1e5`), operators, function calls, nested parens, strings with
quotes/commas/`*/` inside. The server's expression grammar is a large edge region; the value
for the parser under test is quotes/comments/parens *inside* skipped expressions stressing the
skip/lexer logic. Deliberately capped: deep expression edges don't map to code under test —
don't over-invest.

### 3. Mode-first generation (emission capability, not correlation)
Sample `(engine, sqlMode)` before generating; thread `genCtx{engine, mode, rng, vocab}`
through all tables; entries declare `engineMask`/`modeRequired`/`modeForbidden`. `chooseMode`
becomes the plan's weighted matrix (~50% mode 0, singles, byte-safe pairwise combos;
ORACLE/MSSQL maria-only per R11). Note the point at 400k/s: independent sampling already
brute-forces mode×construct correlation; what's new is what `quoteMaybe` cannot *spell* —
ANSI double-quoted identifiers, MSSQL brackets, backslash-vs-doubled escape variants. Both the
mode-matched path (identifier) and mode-mismatched path (string literal) are new edges. This
is the structural prerequisite for item 1's gated entries; do it first.

### 4. Lexical decorator pass (best bug-relevance density)
Post-assembly token-level pass (local tokenizer, byte-safety asserted after), low per-case
probability, *mid-statement* placement — today `quoteFlip` only wraps whole statements:
- comment insertion between tokens: `/* */`, `/*+ */`, `/*! */`, `/*!NNNNN */` (5/6-digit,
  <50000 gate), `/*M! */`, `-- \n`, `#\n`; version-comment wrap of a single spec;
- quote-style respelling of one identifier (backtick↔ANSI↔bracket, mode-gated via item 3;
  doubled-quote escapes);
- whitespace substitution (tab, `\n`, `\r`, `\v`, `\f`, bare-`\r` non-terminating a line
  comment);
- NULs: trailing and embedded-in-string;
- string respelling in DEFAULT/COMMENT values: `N'…'`, introducers, `$tag$…$tag$` (mysql 9),
  backslash vs doubled escapes (mode-gated).
Lexer edge count is modest, but this is where the parser-under-test's confirmed bug class
lives (P5/P6 are lexer bugs) — highest edges-to-relevant-bugs ratio.

### 5. Corpus reload with recency-weighted mutation (fixes D6, D5)
Current state: `es.bases` (the mutation pool) is seeded only from the 725 `seeds.jsonl`
entries (`fuzzloop.go:198`) and grows in-process on retention (`fuzzloop.go:442`, unbounded);
`corpus.db` (144k entries / 26 MB) is write-only from the fast lane. Because the SanCov
accumulator persists across restarts, the seeds→corpus growth chain cannot replay: after every
restart, 60% of throughput mutates a pool that is ~0.5% of what the campaign has learned.
Matters most in combination with items 1–4: findings → fix commits → hot-restarts, each one
resetting the mutation lane exactly while the corpus grows fastest.

- **Load the full corpus** into `es.bases` at startup (26 MB is trivial; base-pick is O(1)).
  No smallest-N tier: retention already prefers smallest-in-window (`fuzzloop.go:430`), and a
  global smallest sample double-dips that bias into degenerate near-duplicate fragments.
- **Recency tier**: ring buffer of the most recent K=8192 retentions per engine; `nextCase`
  picks the mutation base from the ring with p=0.5, else uniform over the full pool. Pool
  composition stops being an accidental throughput-allocation knob — fresh frontier inputs get
  half the mutation budget regardless of how much stale corpus is loaded.
- **Byte cap** per engine (~256 MB), reservoir-evicting from the uniform tier only — bounds
  both the reload and the pre-existing unbounded in-process growth.
- **Per-tier retention stats**: attribute each retention to its base tier
  (fresh-gen / recent-base / old-base) in stats.json. Within an hour of the post-landing
  restart this shows whether old-base mutation pulls its weight; tune p on evidence.
- Adjacent one-liner (D5): `seed.LoadDir` (`seed.go:32`) reads only `seeds.jsonl` — also load
  the committed per-fix `<sig>.sql` regression seeds.

## Implementation plan

All work stays inside `tools/ddlfuzz/` in this worktree (`worktrees/gen-coverage`); `flow/` is
untouched. Gate after every step: `cd tools/ddlfuzz && gofmt -l . && go build ./... &&
go vet ./... && go test ./...`. Two binding compatibility constraints:
`gen.GenerateConstrained(r, v, p)` — signature, `Vocab`/`Profile` semantics — is normative for
the e2e lane (30 §Interfaces) and must keep compiling and behaving; stats.json changes must be
additive fields only (D12). Byte-safety invariant everywhere: output bytes ≥0x80 only as whole
valid UTF-8 sequences from a curated set; sentinel `peerdb_ddlfuzz_nodb` never emitted.

### Step A — mode-first plumbing (item 3)
- `internal/gen`: `type Ctx struct{ R *rand.Rand; IsMariaDB bool; Mode uint64; V Vocab }`;
  named mode consts (`ModeANSIQuotes=1<<2`, `ModeOracle=1<<9`, `ModeMSSQL=1<<10`,
  `ModeNoBackslashEscapes=1<<20`).
- `func ChooseMode(r *rand.Rand, isMariaDB bool) uint64`: ~50% mode 0, ~30% singles, ~20%
  byte-safe pairs; ORACLE/MSSQL and their pairs only when maria (R11).
- `Generate(r *rand.Rand, isMariaDB bool, mode uint64) string`; update the caller
  (`cmd/ddlfuzz/fuzzloop.go` `nextCase`: `mode := gen.ChooseMode(…)` then generate under it);
  delete `chooseMode` from `cmd/ddlfuzz/main.go`. `GenerateConstrained` wraps a Ctx with
  `Mode=0` (e2e semantics unchanged).

### Step B — grammar tables (item 1)
- `internal/gen/tables.go`: `type entry struct{ ID string; W int; MariaOnly, MySQLOnly bool;
  ModeReq, ModeForbid uint64; Emit func(*Ctx) string }` plus a gate/weight-honoring
  `pick(ctx, table)`. Tables: alter heads, column specs, benign specs (incl. index/constraint/
  FK/CHECK/PERIOD/versioning), table-option runs, partition ops, types, column attributes,
  rename forms, benign heads.
- Contents: transcribe `plan/21-fuzzer.md` §"Generator construct checklist" — that section is
  the source of truth, one entry per enumerated construct. ID scheme: `head.*`, `spec.*`,
  `type.*`, `attr.*`, `rename.*`, `benign.*`, `lex.*`.
- `internal/gen/checklist.go`: `var Checklist []string`; `tables_test.go` asserts every
  checklist ID has ≥1 entry and every entry ID ∈ checklist.
- Assembly in `Generate`: ~5% benign head; `;`-chains of 0–3 independently generated
  statements; SET STATEMENT wrap (maria; multi-var, expression values, quoted `FOR` in a
  value); ~1% trailing/embedded-in-string NUL; ~1% comment/whitespace-storm prefix.
- Vocab: per-case pools — plain, keyword-shaped (sampled at init from `dict.Tokens(engine)`
  filtered ASCII-alpha), digit-leading (`1ea10`), `db.1234`, curated UTF-8 names, long names;
  identifier quoting by mode (backtick always; `"…"` iff ANSI_QUOTES; `[…]` iff MSSQL+maria;
  doubled-quote escapes).
- Fuzz-style test: 50k cases per engine across modes → `utf8.Valid`, no sentinel substring.

### Step C — expression generator (item 2)
`internal/gen/expr.go`: `genExpr(c *Ctx, depth int)`, depth ≤3 — literals (ints; floats `.5`,
`1e5`, `-1.5e-10`; strings containing `,` `*/` `FOR` and quotes; `X'…'`, `b'…'`, `0x…`, `0b…`;
introducers `_utf8mb4'…'`, `N'…'`; NULL/TRUE), identifiers, unary/binary ops, `CAST(… AS …)`,
function calls (CONCAT/COALESCE/NOW/CURRENT_TIMESTAMP), nested parens. Wired into
`DEFAULT (expr)`, `CHECK (expr)`, `GENERATED ALWAYS AS (expr)`, and partition `VALUES` in the
step-B tables.

### Step D — lexical decorator (item 4)
`internal/gen/decorate.go`: applied inside `Generate` with ~15% probability, 1–3 ops, token
boundaries via `mutate.Tokens`: comment insertion between tokens (`/* */`, `/*+ */`, `/*! */`,
`/*!NNNNN */` 5/6-digit both below and above server version, `/*M! */`/`/*M!NNNNN */` maria,
`-- \n`, `#\n`); version-comment wrap of one whole spec; whitespace substitution (tab, `\n`,
`\r`, `\v`, `\f`, bare-`\r` non-terminating a line comment); identifier quote respelling
(mode-gated per step B); string respelling in values (`N'…'`, introducers, `$tag$…$tag$`
mysql-only, backslash vs doubled escapes gated on NO_BACKSLASH_ESCAPES). Assert `utf8.Valid`
after; drop the decoration on violation.

### Step E — corpus reload + recency tier (item 5)
- `internal/corpus`: `AllCases(engine string) ([]run.Case, error)` (sql, sql_mode, origin).
- `run.Case` gains `BaseTier uint8` (0 fresh-gen, 1 recent-base, 2 old-base).
- `cmd/ddlfuzz/fuzzloop.go`: after `loadSeedBases`, load the full per-engine corpus into
  `es.bases`. Per-engine byte cap 256 MB: over-cap at load or on retention append → evict
  uniform-random `es.bases` entries (swap-delete), never ring entries.
- `engineState` gains a fixed 8192-slot recency ring; the retention path (currently
  `fuzzloop.go:442`) pushes new retentions to ring + bases. `nextCase`: with p=0.5 pick the
  mutation base from the ring (when non-empty), else uniform over `es.bases`; stamp `BaseTier`.
- stats.json additive fields: `retained_by_tier` counters + `bases_bytes`.
- `internal/seed.LoadDir`: also load per-fix `<sig>.sql`+`<sig>.meta.json` pairs (D5).

### Step F — verification
Full gate green; `git status` shows changes only under `tools/ddlfuzz/`; e2e lane
(`cmd/ddlfuzz-e2e`, `e2e/`) compiles unchanged against the new `gen` API.

### Step G — reserved-word-aware identifier quoting

Problem: `quoteMaybe` leaves any structurally bare-able identifier unquoted with p=4/5,
uniformly — it has no notion of reserved-ness. Keyword-shaped identifiers enter from the
`randomVocab` pools (`key`, `select`, `column`, `table` are reserved; `first`, `after`,
`period`, `system`, `vector` are not) and from `keywordName` (random dict token, 25% of cases;
roughly a third of the symbol bag is reserved). A bare reserved word in identifier position is
a whole-statement server reject — unreachable-by-invariant, so ~80% of every reserved draw
buys only a no-panic check. Estimated 15–25% of generated statements are poisoned this way.
Bare *non-reserved* keywords must stay bare-heavy: keyword-vs-identifier ambiguity is the
P2/P4 bug class.

Design:
- Embed curated per-engine reserved-word lists as `internal/gen/reserved_mysql.txt` (~260
  entries, from dev.mysql.com "Keywords and Reserved Words", reserved subset only) and
  `internal/gen/reserved_mariadb.txt` (~290, from the MariaDB KB "Reserved Words" page).
  Source is public documentation — allowed by 00 constraint 3; the server-source dict
  exception (constraint 2) is not implicated.
- `go:embed` + per-engine set built once (`sync.OnceValue`), lookup case-insensitive ASCII.
- `quoteMaybe`: reserved for the current engine → bare p=1/10 (keep a trickle for near-miss
  lexing), quoted otherwise via the existing mode-gated style switch; non-reserved →
  unchanged (bare p=4/5).
- Tests: lists non-empty (>200 mysql, >250 maria), ASCII-only; the four reserved pool names
  present in both lists; over N=10k draws a reserved name emits quoted ≥85%.

Effect: converts the wasted reject slice into accepted statements exercising quoted-reserved
paths (`` `select` ``, `"table"` under ANSI_QUOTES, `[column]` under MSSQL — all currently
rare) without reducing bare non-reserved ambiguity pressure.
