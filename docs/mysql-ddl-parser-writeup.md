# Hand-rolled MySQL/MariaDB DDL parser — project writeup

State as of 2026-07-02, branch `parser-wip` (branched off `DBI-861/filter-out-the-rest-of-parse-failures`
at its head, 5f6eac57). This doc is the handoff:
a new session starting from it (plus `docs/mysql-ddl-parser-handroll-estimate.md`) has the full context.

## Status

All work is **uncommitted in the working tree** (nothing committed or pushed, by instruction):

- New: `flow/connectors/mysql/ddl_lexer.go` (320 LOC), `ddl_parser.go` (1,044), plus test files
  `ddl_parser_test.go`, `ddl_lexer_test.go`, `ddl_parser_types_test.go`, `ddl_parser_alter_test.go`,
  `ddl_parser_classify_test.go`, `ddl_parser_tidb_diff_test.go` (~3,800 LOC, ~1,050 cases, 37 TestDDL*
  functions, ~1,150 subtest executions).
- Modified: `flow/connectors/mysql/cdc.go`, `cdc_test.go`.
- Deleted: `flow/connectors/mysql/query_parser.go`, `query_parser_test.go` (every case migrated).
- Unrelated untracked files that must not be touched (separate in-flight work):
  `docs/mysql-clickhouse-charset-e2e.md`, `docs/temporal-history.md`, `flow/e2e/clickhouse_mysql_charset_test.go`.

Verification is green:

```
cd flow && go build ./... && go vet ./connectors/mysql/ \
  && go test ./connectors/mysql/ -run 'TestDDL|TestProcessRenameTableQueryMetric|TestClassifyOnlineSchemaMigrationTool' -count=1
golangci-lint run ./connectors/mysql/...   # 0 issues; gofumpt clean
```

Do NOT run the whole `./connectors/mysql/` test package without a live database — non-DDL tests need one.
`flow/switchboard/upstream_mysql.go` still uses the TiDB parser independently (out of scope); the TiDB
dependency stays in go.mod for it and for the differential test.

## Why

The DBI-861 branch originally added `query_parser.go`: a leading-keywords fallback classifier that suppressed the
`ParseSQLErrorsCounter` noise from statements the TiDB parser rejects but MySQL/MariaDB legally emit
(`SET STATEMENT ... FOR` RDS heartbeats, stored-routine bodies, MariaDB-only types — see PR #4505, where
TiDB's inability to parse MariaDB UUID/INET4/INET6 blocked `ALTER TABLE ADD COLUMN` handling). The project:
(1) scope what an ideal parser strictly needs, (2) estimate hand-rolling it (research workflow →
`docs/mysql-ddl-parser-handroll-estimate.md`: ~2,600 LOC parser, ~2,450 tests, ~900 fuzz, ~33 person-days),
(3) implement it, replacing both the TiDB calls and the fallback classifier in the CDC path. Steps 2 and 3
ran as multi-agent workflows; research raw findings lived in `/tmp/ddl-research/*.md` and the mined
statement corpus in `/tmp/ddl-impl/corpus.json` (both ephemeral — everything load-bearing is distilled into
the estimate doc, this doc, and the tests; the corpus is embedded in `ddl_parser_tidb_diff_test.go`).

## What PeerDB needs (requirements, from cdc.go + PR #4505)

Per binlog QueryEvent: classify each statement as ALTER TABLE (column-affecting) / RENAME TABLE(S) /
ignored. **Safety property: never silently classify an actionable statement as ignored; the reverse
(benign misreported) is tolerable noise.** For ALTER TABLE extract per spec: added/modified/changed columns
(name, type string, precision/scale, NOT NULL, FIRST/AFTER flag), dropped and renamed column names. For
RENAME TABLE: (old, new) pairs with optional schemas. Consumers: `processAlterTableQuery`,
`processRenameTableQuery`, and `QkindFromMysqlColumnType` (type_conversion.go), whose load-bearing inputs
are only: lowercased base name before `(`, a trailing `" unsigned"`, and `tinyint(1)` for bool detection;
Precision/Scale feed `MakeNumericTypmod` and are read downstream only for numeric kinds.

## Implementation design

### API (package connmysql, all unexported)

```go
parseQueryEvent(query []byte, sqlMode uint64, isMariaDB bool) ([]ddlStatement, error)
// ddlStatement = *ddlAlterTable{Schema, Table, Specs []ddlAlterSpec} | *ddlRenameTable{Pairs}
// ddlAlterSpec{NewColumns []ddlColumnDef, HasPosition, OldColumnName, NewColumnName}
// ddlColumnDef{Name, TypeStr, Precision, Scale, NotNull}   // -1 = param not written
```

Errors are only produced from inside actionable-headed statements (`ALTER ... TABLE`, `RENAME TABLE|S`) —
benign statements structurally cannot error, actionable ones cannot be half-parsed silently
(`skipSpecRemainder` treats lexer errors / unbalanced `)` as parse failures).

### Lexer (`ddl_lexer.go`)

Server-exact token boundaries for byte-safe client charsets (utf8/utf8mb4/latin1/ascii). Token kinds:
word, quoted-ident, string, punct (single byte), EOF/err. Key rules, each verified in server source:

- Line comments (`#`, `-- ` with the space-or-control-char requirement) end at `\n` or NUL **only** — bare
  `\r` does not terminate (both engines; the old query_parser.go behavior was deliberately corrected).
- Executable comments: bodies are **lexed as code** with a depth counter making `*/` a closer. Rationale:
  source servers patch version comments they *skipped* into plain comments before binlogging, so any
  surviving `/*!NNNNN` was executed — no version comparison needed. `<5` digits (incl. zero) ⇒ body is code
  immediately; `/*M!` MariaDB-only (plain comment on MySQL); MariaDB `/*!!` with 5-6 digits ⇒ plain comment
  (source skipped it), digitless ⇒ code. `/*+` ⇒ plain comment.
- Strings: doubling always escapes; backslash escapes unless `sqlModeNoBackslashEscapes`. `"` is a string
  unless `sqlModeANSIQuotes` (then delimited ident). Backtick doubling; `[bracket]` idents under
  MariaDB+`sqlModeMSSQL`; MySQL-9 `$tag$...$tag$` dollar quoting; NUL = end of input at token start and
  in quoted identifiers, but an ordinary byte inside string literals (server string scans are
  pointer-bounded, not NUL-terminated).
- sql_mode bits (per-event via status vars; layouts agree across engines below bit 32; the doc table in
  libbinlogevents statement_events.h is stale — trust system_variables.h / sql_class.h):
  ANSI_QUOTES=1<<2, ORACLE=1<<9 (Maria), MSSQL=1<<10 (Maria), NO_BACKSLASH_ESCAPES=1<<20.

### Parser (`ddl_parser.go`)

Recursive descent, unbounded peek (LL(3) in practice). Head classification: `ALTER [ONLINE|IGNORE]* TABLE
[IF EXISTS]`; `RENAME TABLE|TABLES` on **both** engines (RENAME TABLES is valid undocumented MySQL —
`table_or_tables`, mysql sql_yacc.yy:16847; the old classifier had it MariaDB-only, a bug); MariaDB
`SET STATEMENT vars FOR <stmt>` unwrapped (FOR recognized at paren depth 0 only). Any other head ignores
the **entire remaining input** — no splitting on `;` (routine bodies contain semicolons; servers binlog one
statement per QueryEvent, so this is unreachable anyway). After a parsed actionable statement, trailing
`;` + further statements do continue.

Only ~7 of ~97 ALTER spec alternatives are parsed (ADD/MODIFY/CHANGE/DROP/RENAME COLUMN, ADD multi-column);
everything else is consumed token-wise (balanced parens, quote-aware) and dropped. Defaults are direction-
aware: unknown word after ADD/DROP ⇒ column (safe); unknown spec head ⇒ benign skip;
unexpected token inside actionable ⇒ error ⇒ reported. LL(2) ambiguity points (live-server verified):
`ADD period date` is a MySQL column vs `ADD PERIOD FOR`/`ADD PERIOD IF` benign; `ADD VECTOR(v)` is a
MariaDB vector-index add (INDEX/KEY optional there) vs a MySQL column; `SYSTEM`+`VERSIONING`; explicit
`COLUMN` or `IF [NOT] EXISTS` commits to the column branch *before* those checks. Bare `RENAME <word>` in a
spec list is a table rename and emits a `ddlRenameTable` statement, same as `ALTER ... RENAME TO`.
MariaDB WAIT n|NOWAIT after table names and per RENAME pair.

Type normalization: canonical lowercase base + `"(params as written)"` + `" unsigned"` (zerofill implies
unsigned; SERIAL ⇒ bigint unsigned + NotNull; BOOL/BOOLEAN ⇒ synthetic `tinyint(1)`). Synonym map
(INT1-8, MIDDLEINT, DEC/FIXED, FLOAT4/8, REAL, NATIONAL/NCHAR/NVARCHAR/VARCHARACTER/CHAR VARYING, LONG
[VARCHAR|VARBINARY], CLOB). `CHARACTER SET binary` (and MariaDB `CHAR ... BYTE`) remap char→binary types;
the bare `BINARY` attribute is a collation marker, no remap. Oracle-mode (Maria + sqlModeOracle):
NUMBER(p[,s])→decimal / bare→double, VARCHAR2→varchar, RAW→varbinary, CLOB→longtext, lengthless
BLOB→longblob; `mariadb_schema.`/`oracle_schema.`/`maxdb_schema.` qualified types handled. **MariaDB's type
slot is open** (any identifier = candidate UDT: uuid, inet4/6, vector, geometry family, plugin types —
passed through lowercased); **MySQL's is closed** (unknown word in type position ⇒ parse error ⇒ reported,
the safe direction). Precision/Scale populated only for decimal/numeric and integer display widths, as
written (-1 otherwise; do not substitute display defaults — matches what downstream reads).

Attribute region: extracts NOT NULL (+ `NOT NULL ENABLE`, `SERIAL DEFAULT VALUE`), FIRST/AFTER (arg may be
any word, `AFTER first`), UNSIGNED/ZEROFILL, charset remaps; `ident=value` pairs consumed atomically and
checked first (MariaDB engine options — prevents keyword-shaped values being misread); everything else
skipped (DEFAULT (expr), CHECK, GENERATED AS, REFERENCES, COMMENT strings with commas are single tokens —
token-level skipping is what kills the desync class).

### cdc.go integration

`sqlModeFromStatusVars` (codes 0/1 walker) kept; `setParserSQLModeFromStatusVars`/`parseSQL` deleted; the
QueryEvent branch passes the **full** sqlMode (fixes the latent NO_BACKSLASH_ESCAPES bug — before, only
ANSI_QUOTES was forwarded to TiDB, which could yield successful-but-wrong parses). Handlers take the new
AST types with 1:1 logic/log/metric parity, except one guard: when `QkindFromMysqlColumnType` errors (e.g.
MariaDB uuid — no mapping on this branch), the column is **error-logged + `ParseSQLErrorsCounter` + skipped**
instead of hard-failing the pull loop (preserves today's soft-skip outcome for such statements, which
previously failed TiDB parse).

## Intentional behavior changes vs the old path

1. Benign statements never log parse warnings/errors or touch metrics at all (the point of the project).
2. NO_BACKSLASH_ESCAPES honored (bug fix).
3. `RENAME TABLES` actionable on MySQL (bug fix; old classifier and a test had it MariaDB-only).
4. Bare-`\r` line-comment termination removed (server-exact; one migrated test expectation flipped).
5. MariaDB-only types now parse; unknown-to-mapper columns warn+metric+skip instead of erroring the loop.
6. Log-only: TiDB parse-warning log and the `col.Tp == nil` "no column type" warn are gone (the new parser
   never emits typeless columns; `ALTER COLUMN SET DEFAULT` specs are consumed as benign).
7. Multi-statement events whose *first* statement is benign ignore the rest (unreachable: servers binlog
   one statement per event; guards routine bodies against `;`-splitting).

## Review findings & dispositions

Workflow adversarial review (safety lens found nothing; parity and quality lenses, 5 findings): applied —
test-helper dedup, overlapping-test folding, zero-copy fast path in `scanDelimitedIdent`; skipped — items
6/7 above (spec-mandated / log-only). Manual design review additions, all accepted as-is: lexer errors
during *head* classification classify benign (only reachable with truncated events — executed statements
can't contain unterminated tokens); unterminated `/*!` body accepted to EOF where the server errors (same
unreachability, benign→actionable direction only); `numeric` not canonicalized to `decimal` (cosmetic,
both accepted downstream); the per-event `string(bytes.TrimRight(...))` copy is deliberate — extracted
names must not alias go-mysql's reusable event buffer.

The tidb-diff suite (590 cases) compares at the semantic level (classification, names, qkinds via
`QkindFromMysqlColumnType` on both type strings, NotNull, HasPosition, Precision/Scale for decimal only) —
per the research, raw InfoSchemaStr strings are full of unconsumed cosmetics (display-width defaults,
`year(-1)`, enum re-escaping) and must not be compared.

## Future work

1. **Non-byte-safe client charsets (gbk, big5, sjis, cp932, gb18030) — transcode before parsing.**
   Statements executed under these charsets are valid but mis-lexable: multibyte trail bytes can be
   0x5C/0x60, so the byte lexer can misread a quote/backslash boundary — the one path to a
   *successful wrong parse* that bypasses the report-on-error backstop. Most of the fix already exists in
   this package, built for row decoding: `charset.go` has `mysqlCharsetEncodings` (all five charsets →
   x/text decoders, incl. the accepted cp932≈ShiftJIS approximation), `charsetForCollation` (cached
   collation-id→charset resolution), and `decodeMySQLBytes`. Remaining work:
   - A length-aware status-var walker reaching Q_CHARSET (code 4): full code table incl. MariaDB codes
     128-131 and 255, the conditional length of 130, and the **known ordering violation** — MariaDB 11.5+
     writes Q_CHARACTER_SET_COLLATIONS (131, payload 1 + 4×count) *before* code 4, so the "increasing
     order" comment on `sqlModeFromStatusVars` must not be relied on. ~100-160 LOC + synthetic-blob tests
     for both engines (~200 LOC).
   - Glue in the QueryEvent branch: resolve `character_set_client`; if not byte-safe, decode `ev.Query`
     via the existing infra before `parseQueryEvent`; on decode error fall through to the reported path
     (error log + `ParseSQLErrorsCounter`). ~30-50 LOC.
   - Parser tests with crafted trail-byte sequences (e.g. Shift-JIS ソ = `0x83 0x5C` inside an identifier
     or string) asserting correct extraction post-transcode.
   ~1 day total, ~200 LOC code + ~250 LOC tests. Side benefit: fixes non-ASCII table/column name matching
   against `TableNameMapping` for these charsets (silently wrong today on both old and new paths). A
   reject-only gate was considered and dropped — transcoding is cheap because the decoder infra exists.
2. Optional cleanups: canonicalize `numeric`→`decimal`

## Ground truth for future verification

Server sources: `~/Code/mysql-server` (tag mysql-9.7.0) and `~/Code/mariadb-server` (main, 13.1.0-alpha) —
`sql/sql_yacc.yy` (alter_list_item at mysql:9126 / maria:8103), `sql/sql_lex.cc` (executable comments at
mysql:1938 / maria:2407), `sql/system_variables.h:123-165` and `sql/sql_class.h:168-206` (sql_mode bits),
`libs/mysql/binlog/event/statement_events.h` (QueryEvent layout; its sql_mode table is stale). Binlog
invariants that the design leans on: one statement per QueryEvent (server splits multi-statement packets);
ALTER/RENAME binlogged verbatim (rewrite_query only touches credential-bearing statements); skipped version
comments patched to plain before binlogging; statements in the binlog always executed successfully on the
source (so unterminated tokens / invalid syntax are unreachable inputs).
