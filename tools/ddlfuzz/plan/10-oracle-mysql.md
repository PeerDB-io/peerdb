# 10 — MySQL 9.7 in-process parse oracle

Executable plan. A smaller model can build this with no other context. Read `00-overview.md` for the
binding contracts (wire protocol v1, digest JSON schema, directory layout). This doc conforms to them;
open questions I resolved during research are recorded, and one place where the digest schema cannot be
faithfully produced from the server's data structures is filed under **Contract issues**.

All paths absolute. RO ground truth: `~/Code/mysql-server` (tag `mysql-9.7.0`, verified
`git -C ~/Code/mysql-server describe --tags` → `mysql-9.7.0`). Never write outside
`/Users/ilia/Code/peerdb`. All artifacts under `/Users/ilia/Code/peerdb/tools/ddlfuzz/build/`.

---

## Goal

A single-threaded C++ executable `build/oracle-mysql` that:

1. Boots the *real* MySQL server parser in-process (no datadir, no network) using the server's own
   unit-test bootstrap (`my_testing::setup_server_for_unit_tests` from `unittest/gunit/test_utils.cc`).
2. Speaks **protocol v1** (00-overview §"oracle wire protocol") on stdin/stdout, logs on stderr.
3. For each `(sql_mode, statement_bytes)` case: installs `sql_mode` on a reused `THD`, parses with the
   MySQL grammar under `character_set_client=utf8mb4`, and emits the **digest JSON** (00-overview
   §"digest JSON schema") — `reject` with errno+message, or `accept` with an ordered `stmts` list.
4. Is compiled with `-fsanitize-coverage=inline-8bit-counters` over the whole server library, and serves
   the cumulative counter bitmap via `GET_COVERAGE`.
5. Survives 72h of adversarial input: bounded per-case memory (MEM_ROOT reset), stack guard set, and
   "let it crash" on the rare abort (the Go client respawns + bisects).

Non-goals: execution, table opening, DD lookups, semantic validation. Parse + contextualize only.

---

## Interfaces (as implemented; see 00-overview for the normative spec)

Wire, little-endian, frame = `u32 body_len | body`, body = `u8 msg_type | payload`:

- **1 PARSE_BATCH** req `u32 count; count×{u64 sql_mode; u32 stmt_len; u8 stmt[stmt_len]}` →
  resp `u32 count; count×{u32 digest_len; u8 digest_json[digest_len]}`.
- **2 GET_COVERAGE** req empty → resp `u32 n; u8 counters[n]` (cumulative, never reset).
- **3 HELLO** req empty → resp `u32 len; u8 json[len]` =
  `{"engine":"mysql","server_version":"9.7.0","protocol":1}`.

Digest JSON (one object per case, verbatim from 00-overview):
```jsonc
{"verdict":"reject","error":"1064: You have an error ..."}
{"verdict":"accept","stmts":[ <stmt>... ]}
// stmt: {"kind":"other"}
//       {"kind":"alter_table","schema":"","table":"t","specs":[ <spec>... ]}
//       {"kind":"rename_table","pairs":[{"old_schema":"","old_table":"a","new_schema":"","new_table":"b"}]}
// spec: {"op":"add","cols":[<col>...],"has_position":false}
//       {"op":"modify","cols":[<col>],"has_position":false}
//       {"op":"change","old_name":"a","cols":[<col>],"has_position":false}
//       {"op":"drop","old_name":"a"}
//       {"op":"rename_col","old_name":"a","new_name":"b"}
// col:  {"name":"c","type_str":"decimal(10,2) unsigned","not_null":true,"params_written":[10,2]}
```

---

## Design decisions (with evidence)

### D1 — Bootstrap via the server's gunit harness, one reused THD
`unittest/gunit/test_utils.cc:86` `setup_server_for_unit_tests()` initializes just enough of mysqld to
parse: it fabricates argv `--secure-file-priv=NULL --log_syslog=0 --explicit_defaults_for_timestamp
--datadir=<DATA_DIR> --lc-messages-dir=<ERRMSG_DIR>` (`:88`), runs `init_common_variables()`,
`xa::Transaction_cache::initialize()`, `delegates_init()`, `gtid_server_init()`, `query_logger.init()`,
`init_optimizer_cost_module(false)`, `DD_initializer::SetUp()` (`:99-115`). `DATA_DIR`/`ERRMSG_DIR` are
compile-time `-D` macros baked into the prebuilt `test_utils.cc.o` inside `libgunit_large.a` (confirmed:
the reference compile line defines `-DDATA_DIR=".../unittest/gunit"` and `-DERRMSG_DIR=".../share"`);
both dirs exist after the build (GenError writes `share/`).

`Server_initializer::SetUp()` (`test_utils.cc:150`) creates `new THD(false)`, `set_new_thread_id()`, sets
`m_thd->thread_stack = (char*)&stack_thd` (this is our stack-overflow guard baseline — D8),
`store_globals()`, `lex_start()`. We reuse **one** `Server_initializer`/`THD` for the whole process; per
case we reset (D6). No datadir/table access is needed for parsing (D4).

`parsertest.h:43` `parse()` is the canonical per-statement recipe: `Parser_state state; state.init(thd,
query, len); lex_start(thd); mysql_reset_thd_for_next_command(thd); parse_sql(thd, &state, nullptr);`.
We drop its `ER_MUST_CHANGE_PASSWORD` / `Mock_error_handler` trickery (that only prevents *execution*,
which we never call — `parse_sql` stops after `make_sql_cmd`, D3) and its `db` fixup (we set db once, D5).

### D2 — Link the whole static server library; borrow a gunit test's compile+link lines
`server_unittest_library` (defined `CMakeLists.txt:2495`, non-shared branch, our config forces
`-DWITH_SHARED_UNITTEST_LIBRARY=OFF`) is a `STATIC` lib publicly linking `perfschema sql_main minchassis
ext::icu binlog`. Server gunit tests link `gunit_large server_unittest_library` (`unittest/gunit/
CMakeLists.txt:376,423`). We cannot add a CMake target to the RO tree, so `build.sh` **extracts** the
exact compile line (for a reference `*-t.cc.o`) and the exact link line (for a reference `*-t`
executable) from the generated Ninja graph via `ninja -t commands`, then substitutes our driver object /
output name. Verified working:

- Reference compile of `create_field-t.cc.o`: include dirs are `-I<build> -I<build>/include
  -I~/Code/mysql-server -I~/Code/mysql-server/include -I~/Code/mysql-server/libs
  -I~/Code/mysql-server/router/src/harness/include -I~/Code/mysql-server/extra/abseil/...` plus ~20
  `-isystem` bundled deps; defines include `-DMYSQL_SERVER -DEXTRA_CODE_FOR_UNIT_TESTING -DHAVE_TLSv13
  -DNDEBUG -DDATA_DIR=... -DERRMSG_DIR=...`. (`NDEBUG` ⇒ server `assert()`s compiled out — good for 72h
  robustness; matches a production-like build.)
- Reference link of `create_field-t` (8,575-char line): pulls `libgunit_large.a
  libserver_unittest_library.a libgmock.a libgtest.a libsql_main.a libbinlog.a libinnobase.a libsql_dd.a
  libsql_gis.a librpl*.a libperfschema.a` + all storage engines + `libicu*.a libprotobuf-lite.dylib
  libssl.dylib libcrypto.dylib libmysys.a libstrings.a libabsl_*.a` + `-Wl,-framework,CoreFoundation`.
  We link `libgunit_large.a` for `test_utils.cc.o` (Server_initializer, setup/teardown); its
  `gunit_test_main_server.cc.o` defines a gtest `main()` but is an archive member, so it is **not**
  extracted (our own `main()` resolves the symbol first). Only members our driver references are pulled.

This "borrow the link line" strategy is version-agnostic and needs zero transcription of the 8 KB line.

### D3 — `parse_sql` already runs contextualization; read `THD::lex` after it returns
`THD::sql_parser()` (`sql/sql_class.cc:3179`) calls `my_sql_parser_parse(this,&root)` then, on success,
`lex->make_sql_cmd(root)` (`:3203`). `parse_sql` (`sql/sql_parse.cc:7208`) wraps it with the parser
MEM_ROOT capacity guard (`:7276` `set_max_capacity(parser_max_mem_size)`) and a temporary DA. So **after
`parse_sql` returns false**, the parse tree has been contextualized and `thd->lex->alter_info` /
`thd->lex->query_block` hold the facts we need — no separate `contextualize()` call, no table open.

`PT_alter_table_stmt::make_cmd` (`sql/parse_tree_nodes.cc:3739`) sets `sql_command=SQLCOM_ALTER_TABLE`,
builds a `Table_ddl_parse_context pc(thd, current_query_block, &m_alter_info)`, runs
`init_alter_table_stmt` (`:3712`, only `add_table_to_list` — builds a `Table_ref`, **no open**), then
`action->contextualize(&pc)` for each action, and stores `lex->alter_info=&m_alter_info` (`:3773`).
`Table_ddl_parse_context` is just `Parse_context(thd, select) + create_info/alter_info/key_create_info`
(`parse_tree_nodes.h:203`, `.cc:163`) — a THD + Query_block, exactly what `parsertest` provides for
SELECTs. **Contextualization is table-independent.** (This is the key feasibility answer.)

### D4 — Digest extraction locations
After `parse_sql` success, read `LEX *lex = thd->lex; lex->sql_command`:

- **`SQLCOM_ALTER_TABLE`**: `Alter_info *ai = lex->alter_info`.
  - Table/schema: `Table_ref *tr = lex->query_block->get_table_list()`. `table = tr->table_name`
    (decoded, quoting removed). `schema = tr->is_fqtn ? tr->db : ""` — `is_fqtn` is set **only** when the
    identifier was written schema-qualified (`sql/sql_parse.cc:6134`; the else-branch copies the session
    default db, which we must not report). Our config leaves `lower_case_table_names=0`, so names are
    byte-preserved (`:6118` casedn is skipped).
  - Added / modified / changed columns: `List<Create_field> ai->create_list`
    (`sql/sql_alter.h:436`). Per `Create_field` (`sql/create_field.h:51`): `field_name`, `change`
    (`:99` — NULL for ADD; = old name for CHANGE/MODIFY), `after` (`:100` — NULL = no position;
    `first_keyword` sentinel = FIRST; else column name), `flags` (`:114`), `sql_type` (`:112`),
    `decimals` (`:113`), `m_max_display_width_in_codepoints` (private; via
    `max_display_width_in_codepoints()` `:56`), `charset` (`:125`), `interval_list`.
    - **op classification** (see Contract issue C1): `change==nullptr` → `add`; `change!=nullptr &&
      strcmp(change,field_name)==0` → `modify`; `change!=nullptr && !=field_name` → `change` with
      `old_name=change`. Multi-column `ADD (a,b)` appears as separate `create_list` entries → we emit one
      `add` spec per column (`cols` has exactly one element).
    - `has_position = (cf.after != nullptr)`.
    - `not_null = (cf.flags & NOT_NULL_FLAG) != 0` (`NOT_NULL_FLAG=1`, `include/mysql_com.h:154`).
  - Dropped columns: `Mem_root_array<const Alter_drop*> ai->drop_list` (`sql/sql_alter.h:416`). Emit
    `{"op":"drop","old_name":name}` **only** for `type==Alter_drop::COLUMN` (`sql/sql_alter.h:67`); skip
    KEY/FOREIGN_KEY/CHECK_CONSTRAINT/ANY_CONSTRAINT (contract: index/constraint specs omitted).
  - Renamed columns: `Mem_root_array<const Alter_column*> ai->alter_list` (`sql/sql_alter.h:418`). Emit
    `{"op":"rename_col","old_name":name,"new_name":m_new_name}` **only** for
    `change_type()==Alter_column::Type::RENAME_COLUMN` (`sql/sql_alter.h:106,138`); skip
    SET_DEFAULT/DROP_DEFAULT/visibility/masking (contract: not column-relevant; our hand-parser consumes
    `ALTER COLUMN SET DEFAULT` as benign).
  - `specs` is emitted in this order: for-each create_list (add/modify/change), then drop_list (drop),
    then alter_list (rename_col). (Order within the event does not affect the signature comparison for
    column ops; see C1.)
- **`SQLCOM_RENAME_TABLE`**: pairs are the `query_block` table list, appended `from1,to1,from2,to2,…`
  (`sql/sql_yacc.yy:9873` `table_to_table_list` → `:9878` `table_to_table` adds `$1` then `$3`). Walk
  `lex->query_block->get_table_list()` via `->next_local`, consume two at a time:
  `{old_schema: tr->is_fqtn?db:"", old_table: tr->table_name, new_schema:…, new_table:…}`.
- **anything else** → `{"kind":"other"}`.

### D5 — Fixed session state: sql_mode per case, utf8mb4 client charset, db set once
Per case set `thd->variables.sql_mode = sql_mode` (the wire u64) **before** `lex_start`/parse. The lexer
reads `MODE_IGNORE_SPACE`/`MODE_ANSI_QUOTES`/`MODE_NO_BACKSLASH_ESCAPES` etc. from it
(`sql/sql_lex.cc:254` `ignore_space = sql_mode & MODE_IGNORE_SPACE`). MySQL's sql_mode bit layout is the
same the hand-parser assumes below bit 32 (writeup §lexer). Note: bits meaningful only to MariaDB
(ORACLE=1<<9, MSSQL=1<<10) are harmless no-ops here.

Charset fixed to utf8mb4 (contract §"Charset scope"): once at startup after `SetUp()`, set
`thd->variables.character_set_client = &my_charset_utf8mb4_0900_ai_ci;`
`thd->variables.character_set_results = &my_charset_utf8mb4_0900_ai_ci;`
`thd->variables.collation_connection = &my_charset_utf8mb4_0900_ai_ci;` then `thd->update_charset();`
(these are extern `CHARSET_INFO`s; declared in `mysql/strings/m_ctype.h`). Set the session db once so
`copy_db_to` has a value: `LEX_CSTRING db{"db",2}; thd->reset_db(db);` (mirrors `parsertest.h:64`, but
once).

### D6 — Per-case reset that mirrors the server, bounded memory
Before each **statement**: `lex_start(thd); mysql_reset_thd_for_next_command(thd);` —
`reset_for_next_command` (`sql/sql_parse.cc:5198`) clears the item list, DA
(`:5241` `clear_error()`, `:5242` `reset_diagnostics_area()`, `:5243` `reset_statement_cond_count()`),
error flags — exactly the state that would otherwise wedge the next parse.
After each **event** (all statements done, digest emitted): `thd->cleanup_after_query()`
(`sql/sql_class.cc:1862`, frees items) then reclaim the MEM_ROOT the way `dispatch_command` does
(`sql/sql_parse.cc:2527`): `if (thd->mem_root->allocated_size() < 40960)
thd->mem_root->ClearForReuse(); else thd->mem_root->Clear();`. This is what keeps 72h memory flat;
gunit tests never hit it because they use a fresh THD per test.

### D7 — Multi-statement enumeration (contract: one entry per executed statement, in order)
`Lex_input_stream::found_semicolon` (`sql/sql_lex.h:3827`) is set by the lexer when a `;` follows a
complete statement; `dispatch_command` loops on it (`sql/sql_parse.cc:2167`). We mirror that loop over
the event bytes:
```
const char *p = stmt; size_t remaining = stmt_len; std::vector<Stmt> out; bool rejected=false; string err;
while (remaining > 0) {
  reset_for_next_command(thd); lex_start(thd);
  Parser_state st; if (st.init(thd, p, remaining)) { rejected=true; err="oom"; break; }
  thd->set_query({p, remaining}, ...);          // set query so error offsets are valid (see impl)
  if (parse_sql(thd, &st, nullptr)) {            // true = error
     rejected = true; err = da_error(thd); break;
  }
  out.push_back(extract_digest(thd));
  const char *sc = st.m_lip.found_semicolon;
  if (!sc) break;
  // advance past ';', skip leading spaces (dispatch_command:2191)
  size_t consumed = (size_t)(sc - p);
  p += consumed; remaining -= consumed;
  // p now points at ';'? found_semicolon points *after* the stmt at the ';'
  while (remaining && (*p==';' || my_isspace(thd->charset(), *p))) { p++; remaining--; }
}
```
(Exact pointer arithmetic in the impl file; `found_semicolon` points into the original buffer.)
Any parse failure at any position → `verdict:"reject"` for the whole event (a real binlog QueryEvent is
a single successfully-executed statement, so a partially-failing multi-statement text is
unreachable-by-invariant; classifying it `reject` makes the fuzzer skip the signature comparison, which
is the safe outcome). Otherwise `verdict:"accept"` with `out`.

### D8 — Reject capture, stack guard, crash policy
- **Reject text**: after `parse_sql` returns true, read the statement DA:
  `Diagnostics_area *da = thd->get_stmt_da(); if (da->is_error()) err =
  std::to_string(da->mysql_errno()) + ": " + da->message_text();` (prefix errno per contract). Then
  `reset_for_next_command` on the next iteration clears it. `parse_sql` already copies parser-DA errors
  into the stmt DA (`sql/sql_parse.cc:7303-7333`).
- **Stack guard**: `THD::thread_stack` is set by `Server_initializer::SetUp` to a `SetUp`-frame address.
  Because we run everything on the main thread and `SetUp` is called from `main`, that baseline is valid
  for the process lifetime; the parser's `check_stack_overrun` uses it. Pathological nesting that still
  overflows → SIGSEGV → crash (policy below). Optionally lower `thd->variables.parser_max_mem_size` from
  its default so runaway grammars OOM-error (a clean reject) before they exhaust RAM — leave at default
  unless golden validation shows a hang.
- **Crash policy**: no C++ exception catching around `parse_sql` (server code is `-fno-exceptions`
  style; errors are return codes + DA, not throws). On the rare abort/segfault we **let it crash**; the
  Go client (component 20) respawns the oracle and bisects the in-flight batch to the offending input,
  which becomes a finding. Do **not** install a SIGSEGV handler that swallows it. Known crashy area to
  watch during golden validation: none identified in parse+contextualize for ALTER/RENAME; deeply
  nested `DEFAULT (expr)` / generated-column expressions itemize during contextualize and are the most
  expression-heavy path.

### D9 — `type_str` + `params_written`: hand-render from `Create_field` (no Field instantiation)
The parser fully normalizes the type into `Create_field` at parse time, so we render from it directly
rather than instantiating a `Field` (which would need a `TABLE_SHARE` + `interval` TYPELIB and adds
crash surface on adversarial input). The only load-bearing outputs are what
`QkindFromMysqlColumnType` reads (`flow/connectors/mysql/type_conversion.go:11`): the lowercase base
name before `(`, a trailing `" unsigned"`, and `tinyint(1)`; plus decimal `(precision,scale)`. The
render function reproduces the server's own `Field::sql_type` naming logic:

**Base name by `cf.sql_type`** (`enum_field_types`, `include/field_types.h:55`). Values that actually
reach `create_list` after parse (grammar in `sql/sql_yacc.yy` `type:` `:7156`, node ctors in
`sql/parse_tree_column_attrs.h`):

| enum_field_types | render | notes / evidence |
|---|---|---|
| `MYSQL_TYPE_TINY` | `tinyint` | BOOL/BOOLEAN → this with width 1 ⇒ `tinyint(1)` (`PT_boolean_type` `:758`) |
| `MYSQL_TYPE_SHORT` | `smallint` | |
| `MYSQL_TYPE_INT24` | `mediumint` | |
| `MYSQL_TYPE_LONG` | `int` | |
| `MYSQL_TYPE_LONGLONG` | `bigint` | SERIAL → this + flags UNSIGNED|NOT_NULL|AUTO_INC (`PT_serial_type` `:995`) ⇒ `bigint unsigned`, not_null=true |
| `MYSQL_TYPE_NEWDECIMAL` | `decimal` | precision/scale below |
| `MYSQL_TYPE_FLOAT` | `float` | |
| `MYSQL_TYPE_DOUBLE` | `double` | REAL→double unless `MODE_REAL_AS_FLOAT` (`:7399`) — already resolved into the enum |
| `MYSQL_TYPE_BIT` | `bit` | |
| `MYSQL_TYPE_STRING` | `char` if `cf.charset!=&my_charset_bin` else `binary` | `Field_string::sql_type` (`sql/field.cc:6334`, `has_charset()` = charset≠bin) |
| `MYSQL_TYPE_VARCHAR` | `varchar` / `varbinary` (same charset rule) | `Field_string::sql_type` VAR_STRING branch / `Field_varstring` |
| `MYSQL_TYPE_TINY_BLOB` | `tinytext` if charset≠bin else `tinyblob` | `Field_blob::sql_type` (`sql/field.cc:7336`, packlength+charset) |
| `MYSQL_TYPE_BLOB` | `text` / `blob` | |
| `MYSQL_TYPE_MEDIUM_BLOB` | `mediumtext` / `mediumblob` | |
| `MYSQL_TYPE_LONG_BLOB` | `longtext` / `longblob` | LONG / LONG VARCHAR map here (`:7304,7337`) |
| `MYSQL_TYPE_ENUM` | `enum` | value list irrelevant to qkind — emit bare `enum` |
| `MYSQL_TYPE_SET` | `set` | emit bare `set` |
| `MYSQL_TYPE_JSON` | `json` | |
| `MYSQL_TYPE_YEAR` | `year` | |
| `MYSQL_TYPE_NEWDATE` | `date` | `DATE` is rewritten NEWDATE in `Create_field::init` (`sql/create_field.cc:515`) |
| `MYSQL_TYPE_DATE` | `date` | (defensive; normally rewritten) |
| `MYSQL_TYPE_TIME` / `MYSQL_TYPE_TIME2` | `time` | parser emits TIME2 (`Time_type::TIME` `:875`) |
| `MYSQL_TYPE_DATETIME` / `MYSQL_TYPE_DATETIME2` | `datetime` | parser emits DATETIME2 |
| `MYSQL_TYPE_TIMESTAMP` / `MYSQL_TYPE_TIMESTAMP2` | `timestamp` | parser emits TIMESTAMP2 (`:901`) |
| `MYSQL_TYPE_VECTOR` | `vector` | |
| `MYSQL_TYPE_GEOMETRY` | `geometry` | all spatial subtypes map to QValueKindGeometry; bare `geometry` is sufficient |
| `MYSQL_TYPE_BOOL` | `tinyint` (with `(1)`) | placeholder; not expected via ALTER but handle defensively |

Base names not in this table (should be unreachable post-parse: `MYSQL_TYPE_DECIMAL`,
`MYSQL_TYPE_VAR_STRING`, `MYSQL_TYPE_NULL`, `MYSQL_TYPE_TYPED_ARRAY`, `MYSQL_TYPE_INVALID`): render the
enum's numeric value as `"type_<n>"` — this deliberately fails `QkindFromMysqlColumnType` (→ "ERR" on
both sides only if the hand-parser also produced an unknown), and surfaces as a finding if it ever
occurs.

**Unsigned suffix**: `unsigned = (cf.flags & (UNSIGNED_FLAG|ZEROFILL_FLAG)) != 0`
(`UNSIGNED_FLAG=32`, `ZEROFILL_FLAG=64`, `include/mysql_com.h:159-160`; zerofill implies unsigned — the
grammar already ORs UNSIGNED when ZEROFILL, `PT_numeric_type::get_type_flags` `:729`). Append
`" unsigned"` when true. (We never emit `" zerofill"`; the contract's round-trip only needs
`" unsigned"`, and `QkindFromMysqlColumnType` strips a leading `" zerofill"` before `" unsigned"`
anyway, `type_conversion.go:14`.)

**Params in `type_str` and `params_written`**:
- `MYSQL_TYPE_NEWDECIMAL`: scale = `cf.decimals`; precision =
  `my_decimal_length_to_precision(cf.max_display_width_in_codepoints(), cf.decimals,
  (cf.flags&UNSIGNED_FLAG)!=0)` (`sql-common/my_decimal.h:184`) — because `Create_field::init` stores the
  *length*, not precision, into the display width for decimals (`sql/create_field.cc:381`). Emit
  `type_str="decimal(P,S) [unsigned]"`, `params_written=[P,S]`.
- `MYSQL_TYPE_TINY` with explicit width 1 (BOOL, or `tinyint(1)`): emit `type_str="tinyint(1)"`,
  `params_written=[1]`. Use `cf.explicit_display_width()` (`create_field.h:83`) to decide whether a
  width was written; render `(W)` where `W=max_display_width_in_codepoints()` only when explicit.
- Other integer types: if `explicit_display_width()`, emit `(W)` in `type_str` and `params_written=[W]`;
  else no parens and `params_written=null`.
- `bit`: `(W)` where W = display width (BIT always has a width; default 1 from `PT_bit_type` `:745`).
- char/varchar/binary/varbinary: emit `(len)` where len = `max_display_width_in_codepoints()`;
  `params_written=[len]`. (Cosmetic for qkind but keeps `type_str` info-schema-shaped.)
- everything else: no parens, `params_written=null`.

`params_written` documents the **server-recorded** (normalized) values, which the contract explicitly
permits ("params as written when recoverable, else as the server recorded them — document which"): after
contextualization the as-written token strings are gone (they lived on the `PT_type` node, private and
consumed by `make_cmd`). Component 20 already reconciles written-vs-defaulted decimal params (its
`ddlDiffColSig` compares precision/scale only for the numeric kind, and the tidb-diff suite handles
defaulting) — this is its job per 00-overview §"signature grammar".

### D10 — SanitizerCoverage
Add `-fsanitize-coverage=inline-8bit-counters` to `CMAKE_C_FLAGS`/`CMAKE_CXX_FLAGS` at configure time
(scope = whole build; fine per contract). Verified Apple clang 21 accepts it and emits, per object: a
`__DATA,__sancov_cntrs` section, a `__mod_init_func` ctor `_sancov.module_ctor_8bit_counters` that calls
`__sanitizer_cov_8bit_counters_init(start,stop)` using `section$start/end$__DATA$__sancov_cntrs`
boundary symbols, and an **undefined** `__sanitizer_cov_8bit_counters_init` (because we link no
sanitizer runtime). We **define** it in the driver and record the region(s):
```cpp
extern "C" void __sanitizer_cov_8bit_counters_init(uint8_t *start, uint8_t *stop) {
  g_cov_regions.push_back({start, stop});   // called by module ctors before main()
}
```
`GET_COVERAGE` returns the concatenation of all regions' bytes in registration order (cumulative, never
zeroed — the counters accumulate across cases by design). **Dedup identical (start,stop) pairs**: on
Mach-O every instrumented TU's ctor passes the same whole-section boundary pair, so without dedup the
region list is thousands of copies of one region (observed: 39×1.66 MB before the 64 MiB cap; deduped
payload is ~1.7 MB — the main section plus a distinct region from the instrumented
libprotobuf-lite.dylib). Do **not** define
`__sanitizer_cov_trace_pc_guard*` or `__sanitizer_cov_pcs_init` — the inline-8bit-counters mode does not
reference them (confirmed: the only undefined sancov symbol in a test object was
`__sanitizer_cov_8bit_counters_init`).

---

## Implementation steps

### Directory layout to create
```
tools/ddlfuzz/oracle/proto/proto.hpp     # shared frame + JSON (MariaDB oracle reuses this)
tools/ddlfuzz/oracle/mysql/driver.cc     # main + parse loop + digest extraction + type render + sancov
tools/ddlfuzz/oracle/mysql/build.sh      # configure server, build libs, compile+link driver
tools/ddlfuzz/oracle/mysql/smoke.py      # smoke test client (PEP 723 uv script)
```
Build outputs (gitignored):
```
tools/ddlfuzz/build/mysql-build/          # out-of-source cmake/ninja tree for ~/Code/mysql-server
tools/ddlfuzz/build/oracle-mysql          # the driver binary
```

### Step 1 — `oracle/proto/proto.hpp` (shared, single header)
Frame IO + a minimal JSON string writer. No third-party deps. Sketch (complete enough to type in):
```cpp
#pragma once
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>
#include <unistd.h>

namespace ddlproto {
enum MsgType : uint8_t { PARSE_BATCH = 1, GET_COVERAGE = 2, HELLO = 3 };

// ---- low-level fd IO (loop over partial reads/writes) ----
inline bool read_full(int fd, void *buf, size_t n) {
  auto *p = static_cast<uint8_t *>(buf);
  while (n) { ssize_t r = ::read(fd, p, n); if (r <= 0) return false; p += r; n -= (size_t)r; }
  return true;
}
inline bool write_full(int fd, const void *buf, size_t n) {
  auto *p = static_cast<const uint8_t *>(buf);
  while (n) { ssize_t w = ::write(fd, p, n); if (w <= 0) return false; p += w; n -= (size_t)w; }
  return true;
}
// ---- LE scalar helpers on a byte cursor ----
struct Reader {
  const uint8_t *p, *end;
  Reader(const std::vector<uint8_t> &v) : p(v.data()), end(v.data() + v.size()) {}
  bool u32(uint32_t &o){ if(end-p<4) return false; std::memcpy(&o,p,4); p+=4; return true; }
  bool u64(uint64_t &o){ if(end-p<8) return false; std::memcpy(&o,p,8); p+=8; return true; }
  bool bytes(const uint8_t *&o, uint32_t n){ if((uint32_t)(end-p)<n) return false; o=p; p+=n; return true; }
};
inline void put_u32(std::string &s, uint32_t v){ s.append((const char*)&v, 4); }

// Read one frame body (msg_type byte + payload). Returns false on EOF/error (clean shutdown).
inline bool read_frame(int fd, std::vector<uint8_t> &body) {
  uint32_t len; if (!read_full(fd, &len, 4)) return false;
  body.resize(len); return len == 0 ? true : read_full(fd, body.data(), len);
}
// Write a frame: 4-byte len prefix + body (body already begins with msg_type byte).
inline bool write_frame(int fd, const std::string &body) {
  uint32_t len = (uint32_t)body.size();
  return write_full(fd, &len, 4) && write_full(fd, body.data(), len);
}

// ---- JSON string escaping (RFC 8259; \uXXXX for controls, handles embedded NUL) ----
inline void json_escape(std::string &out, const char *s, size_t n) {
  static const char *hex = "0123456789abcdef";
  out.push_back('"');
  for (size_t i = 0; i < n; ++i) {
    unsigned char c = (unsigned char)s[i];
    switch (c) {
      case '"': out += "\\\""; break;
      case '\\': out += "\\\\"; break;
      case '\n': out += "\\n"; break;
      case '\r': out += "\\r"; break;
      case '\t': out += "\\t"; break;
      default:
        if (c < 0x20) { out += "\\u00"; out.push_back(hex[c>>4]); out.push_back(hex[c&0xf]); }
        else out.push_back((char)c);   // pass through UTF-8 bytes verbatim (idents are utf8mb4)
    }
  }
  out.push_back('"');
}
inline void json_escape(std::string &out, const std::string &s){ json_escape(out, s.data(), s.size()); }
} // namespace ddlproto
```
Notes: identifiers are decoded utf8mb4 (multibyte pass-through is fine; only control bytes are
`\u`-escaped). Byte-safe by construction because the harness charset scope forbids non-UTF-8 (00-overview
§"Charset scope"). The MariaDB oracle plan (11) `#include`s this same header.

### Step 2 — `oracle/mysql/driver.cc`
Structure (headers via the borrowed include line, so ordinary `#include "sql/..."` works):
```cpp
#include "sql/sql_class.h"
#include "sql/sql_lex.h"
#include "sql/sql_parse.h"
#include "sql/sql_alter.h"
#include "sql/create_field.h"
#include "sql/field.h"
#include "sql/table.h"
#include "sql-common/my_decimal.h"
#include "mysql/strings/m_ctype.h"
#include "mysql_com.h"
#include "unittest/gunit/test_utils.h"
#include "../proto/proto.hpp"
#include <vector>
using namespace ddlproto;

// --- sancov ---
struct CovRegion { uint8_t *start, *stop; };
static std::vector<CovRegion> g_cov_regions;
extern "C" void __sanitizer_cov_8bit_counters_init(uint8_t *start, uint8_t *stop) {
  g_cov_regions.push_back({start, stop});
}
extern const char *first_keyword;   // sql/mysqld.h — FIRST sentinel for Create_field::after

// --- helpers ---
static std::string da_error(THD *thd) {
  Diagnostics_area *da = thd->get_stmt_da();
  if (da->is_error()) return std::to_string(da->mysql_errno()) + ": " + da->message_text();
  return "0: parse error";
}

// render type_str + params_written per D9. Appends to a JSON object under construction.
static void render_type(std::string &out, const Create_field &cf); // full mapping table from D9

// emit one alter_table stmt object from thd->lex into `out`
static void emit_alter(std::string &out, THD *thd);
static void emit_rename(std::string &out, THD *thd);

// parse one event (may be multi-statement) -> digest JSON. Implements D7 loop.
static std::string parse_event(THD *thd, Server_initializer &, uint64_t sql_mode,
                               const char *stmt, size_t len);

int main() {
  my_testing::setup_server_for_unit_tests();
  Server_initializer init; init.SetUp();
  THD *thd = init.thd();
  // D5 fixed session state
  thd->variables.character_set_client = &my_charset_utf8mb4_0900_ai_ci;
  thd->variables.character_set_results = &my_charset_utf8mb4_0900_ai_ci;
  thd->variables.collation_connection = &my_charset_utf8mb4_0900_ai_ci;
  thd->update_charset();
  LEX_CSTRING db{ "db", 2 }; thd->reset_db(db);

  std::vector<uint8_t> body;
  while (read_frame(0, body)) {
    if (body.empty()) continue;
    uint8_t msg = body[0];
    Reader r(body); const uint8_t *skip; r.bytes(skip, 1);   // consume msg byte
    std::string resp; resp.push_back((char)msg);             // echo msg_type
    if (msg == HELLO) {
      std::string j = "{\"engine\":\"mysql\",\"server_version\":\"9.7.0\",\"protocol\":1}";
      put_u32(resp, (uint32_t)j.size()); resp += j;
    } else if (msg == GET_COVERAGE) {
      uint32_t n = 0; for (auto &rg : g_cov_regions) n += (uint32_t)(rg.stop - rg.start);
      put_u32(resp, n);
      for (auto &rg : g_cov_regions) resp.append((const char*)rg.start, rg.stop - rg.start);
    } else if (msg == PARSE_BATCH) {
      uint32_t count; r.u32(count); put_u32(resp, count);
      for (uint32_t i = 0; i < count; ++i) {
        uint64_t mode; uint32_t slen; const uint8_t *sp;
        r.u64(mode); r.u32(slen); r.bytes(sp, slen);
        std::string dj = parse_event(thd, init, mode, (const char*)sp, slen);
        put_u32(resp, (uint32_t)dj.size()); resp += dj;
      }
    } else { /* unknown: echo empty */ }
    if (!write_frame(1, resp)) break;
  }
  init.TearDown();
  my_testing::teardown_server_for_unit_tests();
  return 0;
}
```
`parse_event` implements D6/D7/D8: set `thd->variables.sql_mode=sql_mode`; loop statements; on the first
(or any) `parse_sql` error → `{"verdict":"reject","error":"<da>"}`; else collect stmt objects from
`emit_alter`/`emit_rename`/`{"kind":"other"}`; after the loop reclaim MEM_ROOT (D6). Set the query on the
THD each iteration so error positions resolve: `thd->set_query({p, remaining});
thd->set_query_id(next_query_id());` (query_id bump avoids DA/PSI confusion). Use `Parser_state st;
st.init(thd, const_cast<char*>(p), remaining)` (buffer is mutated in place by version-comment patching —
so pass a **writable copy** of the event bytes; allocate one `std::string mut(stmt, len)` per event and
parse from `mut.data()`).

`emit_alter` walks `create_list`/`drop_list`/`alter_list` per D4 and calls `render_type` per D9.
`render_type` is the full table from D9 (the smaller model types it out verbatim; base name switch +
unsigned suffix + params).

### Step 3 — `oracle/mysql/build.sh`
```bash
#!/usr/bin/env bash
set -euo pipefail
ROOT=/Users/ilia/Code/peerdb/tools/ddlfuzz
SRC=/Users/ilia/Code/mysql-server
BUILD=$ROOT/build/mysql-build
OUT=$ROOT/build/oracle-mysql
BISON=$(brew --prefix bison)/bin/bison
SSL=$(brew --prefix openssl@3)

mkdir -p "$BUILD"
# 1) configure (idempotent; sancov on whole build)
cmake -G Ninja -S "$SRC" -B "$BUILD" \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DWITH_UNIT_TESTS=ON \
  -DWITH_SHARED_UNITTEST_LIBRARY=OFF \
  -DWITH_SSL="$SSL" \
  -DBISON_EXECUTABLE="$BISON" \
  -DCMAKE_C_FLAGS="-fsanitize-coverage=inline-8bit-counters" \
  -DCMAKE_CXX_FLAGS="-fsanitize-coverage=inline-8bit-counters"

# 2) build a reference gunit server test — this builds every .a the driver links
#    AND gives us the exact compile+link commands. -j to taste (<=6 to be gentle).
ninja -C "$BUILD" -j6 create_field-t

# 3) extract reference compile line (for a *-t.cc.o) and link line (for the *-t exe)
CC_LINE=$(ninja -C "$BUILD" -t commands unittest/gunit/CMakeFiles/create_field-t.dir/create_field-t.cc.o | tail -1)
LD_LINE=$(ninja -C "$BUILD" -t commands runtime_output_directory/create_field-t | tail -1)

# 4) compile our driver object using the reference flags (swap source + output)
DRV_O="$BUILD/ddlfuzz_driver.o"
# strip the reference source and -o target from CC_LINE, keep flags/-I/-D/-isystem
CC_FLAGS=$(echo "$CC_LINE" | sed -E 's#-o [^ ]+##; s#-c [^ ]+##; s#-MD##; s#-MT [^ ]+##; s#-MF [^ ]+##')
# CC_FLAGS still begins with the compiler (clang++). Recompile:
eval "$CC_FLAGS -c $ROOT/oracle/mysql/driver.cc -o $DRV_O"

# 5) link: substitute the reference object with our driver object, and -o with our binary
LD_CMD=$(echo "$LD_LINE" \
  | sed -E "s#unittest/gunit/CMakeFiles/create_field-t.dir/create_field-t.cc.o#$DRV_O#" \
  | sed -E "s#-o runtime_output_directory/create_field-t#-o $OUT#")
( cd "$BUILD" && eval "$LD_CMD" )
echo "built $OUT"
```
The `sed` for `CC_FLAGS` must remove the reference `-c <src>`, `-o <obj>`, and the `-M*` depfile flags
(the exact tokens are visible in the captured line — the executing model should verify by printing
`$CC_LINE` first and adjust the regex if the token order differs). Keep every `-I/-isystem/-D` and
warning/opt flag. `LD_CMD` runs from `$BUILD` because the link line uses build-relative `.a` paths.

Prerequisites (host, one-time; NOT under peerdb — the user/executor installs these, do not script an
install that writes outside the repo): `brew install bison` (≥3.0.4; MySQL rejects the macOS system
bison 2.3 — `cmake/bison.cmake:129`), `openssl@3`, `cmake`, `ninja`. Bundled boost/icu/protobuf/abseil
under `~/Code/mysql-server/extra/` are used automatically (no `-DWITH_BOOST` needed;
`cmake/boost.cmake:24` finds `extra/boost/boost_1_87_0`).

### Step 4 — smoke test `oracle/mysql/smoke.py`
PEP 723 uv script (`# /// script` header, run `uv run oracle/mysql/smoke.py`). Spawns
`build/oracle-mysql`, then:
1. HELLO → assert JSON == `{"engine":"mysql","server_version":"9.7.0","protocol":1}`.
2. PARSE_BATCH of the cases below; assert each digest matches expected.
3. GET_COVERAGE → assert `n > 0`.
Frame helpers mirror `proto.hpp` (struct little-endian). Expected digests:

| sql (sql_mode=0) | expected digest (JSON, key order as emitted) |
|---|---|
| `ALTER TABLE t ADD c INT NOT NULL` | `{"verdict":"accept","stmts":[{"kind":"alter_table","schema":"","table":"t","specs":[{"op":"add","cols":[{"name":"c","type_str":"int","not_null":true,"params_written":null}],"has_position":false}]}]}` |
| `ALTER TABLE s.t ADD c DECIMAL(10,2) UNSIGNED` | schema `"s"`, one add col `type_str":"decimal(10,2) unsigned"`, `params_written":[10,2]`, `not_null":false` |
| `ALTER TABLE t MODIFY c BOOL` | one `{"op":"modify","cols":[{"name":"c","type_str":"tinyint(1)",...,"params_written":[1]}],...}` |
| `ALTER TABLE t CHANGE a b INT` | `{"op":"change","old_name":"a","cols":[{"name":"b","type_str":"int",...}],...}` |
| `ALTER TABLE t DROP c` | `{"op":"drop","old_name":"c"}` |
| `ALTER TABLE t RENAME COLUMN a TO b` | `{"op":"rename_col","old_name":"a","new_name":"b"}` |
| `ALTER TABLE t ADD c SERIAL` | add col `type_str":"bigint unsigned"`, `not_null":true` |
| `ALTER TABLE t ADD c INT FIRST` | add col with `"has_position":true` |
| `RENAME TABLE a TO b, c.d TO e.f` | `{"kind":"rename_table","pairs":[{"old_schema":"","old_table":"a","new_schema":"","new_table":"b"},{"old_schema":"c","old_table":"d","new_schema":"e","new_table":"f"}]}` |
| `SELECT 1` | `{"verdict":"accept","stmts":[{"kind":"other"}]}` |
| `ALTER TABLE t ADD c INT; ALTER TABLE t DROP c` | two stmts, in order |
| `ALTER TABLE t ADD` (garbage) | `{"verdict":"reject","error":"1064: ..."}` |
| `SET STATEMENT max_statement_time=1 FOR SELECT 1` | `{"kind":"other"}` (MySQL rejects `SET STATEMENT`? — actually MySQL has no SET STATEMENT; expect reject. Record actual and pin it.) |

(The smoke script pins the exact bytes on first green run; if a digest key order differs from the table,
update the table to the emitted order — the JSON is emitted by hand so order is deterministic.)

### Step 5 — golden validation hook (consumed by component 20)
Component 20 drives golden validation by sending every existing test corpus case
(`flow/connectors/mysql/*_test.go`, ~480 unit + ~230 tidb-diff) as `PARSE_BATCH` and reconciling each
digest against `FuzzDDLSignature`. This oracle exposes nothing beyond protocol v1, so component 20 needs
no oracle-side changes; the only requirement here is that `parse_event` is **deterministic** (same bytes
+ sql_mode ⇒ same digest) and side-effect-free across cases (guaranteed by D6 reset). During golden
validation, mismatches localize to one of: (a) a `render_type` mapping bug (fix the table here);
(b) the op-classification / multi-add reconciliation (C1 — fix in component 20's reduction, not here);
(c) a genuine hand-parser divergence (ledger/fix per 00-overview). The oracle is ground truth: prefer
fixing (a) here and (b) in component 20 before ever touching the hand-parser.

---

## Build & commands
```bash
# one-time host prereqs (installed by the operator; write outside repo, so not scripted here):
brew install bison            # >=3.0.4 required; macOS system bison 2.3 is rejected
brew install openssl@3 cmake ninja

# build the oracle (idempotent; first run does the full server compile, ~tens of minutes):
bash /Users/ilia/Code/peerdb/tools/ddlfuzz/oracle/mysql/build.sh

# smoke test:
uv run /Users/ilia/Code/peerdb/tools/ddlfuzz/oracle/mysql/smoke.py
```
Configure was validated live: with `-DBISON_EXECUTABLE=$(brew --prefix bison)/bin/bison` and the two
sancov `-D*_FLAGS`, `cmake … -S ~/Code/mysql-server -B build/mysql-build` reports
`Configuring done / Generating done` and folds the sancov flag into `CMAKE_C(XX)_FLAGS`. The link/compile
extraction was validated: `ninja -t commands runtime_output_directory/create_field-t | tail -1` yields
the full 8 KB link line (all `.a` + ssl/crypto dylibs + `-Wl,-framework,CoreFoundation`), and the
per-object compile line yields the include/define set in D2.

---

## Acceptance checks
1. `build.sh` produces `build/oracle-mysql` with no unresolved symbols (link succeeds — the
   gtest-`main` archive member is correctly *not* pulled).
2. `smoke.py`: HELLO exact match; all PARSE_BATCH digests match the pinned table; `GET_COVERAGE` `n>0`
   and grows after parsing more distinct constructs (parse 1 case, snapshot; parse 20 varied cases,
   snapshot; assert popcount increased).
3. Determinism: send the same 100-case batch twice; digests byte-identical.
4. Memory: send 1,000,000 trivial `ALTER TABLE t ADD c INT` cases; RSS plateaus (D6 MEM_ROOT reclaim).
   `ps -o rss= -p <pid>` before/after differ by < ~50 MB.
5. Reject hygiene: a batch of `[valid, garbage, valid]` yields `[accept, reject, accept]` — the garbage
   case does not corrupt the neighbors (D6 DA reset).
6. Crash containment: a case that segfaults (if any found) kills only the process; the parent observes
   EOF on the response pipe (that behavior is component 20's; here just confirm no SIGSEGV handler
   swallows crashes).

---

## Risks & fallbacks
- **R1 (medium): `render_type` mapping drift vs `Field::sql_type`.** Golden validation catches this.
  *Fallback:* build the `Field` and call `Field::sql_type` — `get_sql_type_by_create_field`
  (`sql/dd/dd_table.cc:243`) shows the recipe (`make_field(cf, share); fld->init(table); fld->sql_type`),
  but it needs a `TABLE_SHARE` + `Fake_TABLE` (`unittest/gunit/fake_table.cc`) and, for enum/set, a built
  `interval` TYPELIB, and `is_unsigned/is_zerofill` pre-populated (they are set in
  `prepare_pack_create_field` `sql/sql_table.cc:4251`, *not* during contextualize). This adds crash
  surface, so it is the fallback, used only for a case class where hand-render proves wrong.
- **R2 (low): first line-extraction regexes in `build.sh` are brittle to token order.** Mitigation: the
  script prints `$CC_LINE`/`$LD_LINE` before editing; the model verifies the two substitutions
  (`create_field-t.cc.o` → driver obj; `-o …/create_field-t` → `$OUT`) hit exactly once
  (`grep -c`), else fail loudly.
- **R3 (low): sancov region count > 1 or ordering nondeterminism.** We accumulate all regions in
  init order and never reset; `GET_COVERAGE` concatenates deterministically. If two runs disagree on
  layout, component 20 keys coverage on the OR of the whole bitmap, which is order-insensitive.
- **R4 (low): a construct aborts inside contextualize (e.g. exotic generated-column expr).** Policy is
  crash + client bisect (D8). If a *class* of inputs proves reliably crashy and non-actionable, gate it
  at the generator (component 20) — do not add server-side try/catch.
- **R5 (build time): first build compiles most of the server (~tens of minutes, `-j6`).** One-time;
  the tree is cached under `build/mysql-build`. Incremental driver rebuilds only re-run steps 4–5.

---

## Effort
- proto.hpp: 0.5 day. driver.cc (parse loop + digest + full type table + sancov): 2 days.
- build.sh + first green build (incl. debugging the line extraction): 1 day.
- smoke.py + pinning digests: 0.5 day.
- Golden-validation shakeout with component 20 (mapping fixes): 1–2 days (shared with 20).
- **Total: ~4–5 person-days** for a green, smoke-tested oracle; +buffer for golden shakeout.

---

## Contract issues

**C1 — The digest cannot faithfully distinguish MODIFY-vs-CHANGE-same-name, nor recover multi-column ADD
grouping, from the server's data structures; component 20's reduction must canonicalize both sides.**
- `MODIFY c …` and `CHANGE c c …` produce byte-identical `Alter_info` (both set `Create_field.change =
  "c" == field_name`; both use `PT_alter_table_change_column`, `parse_tree_nodes.h:4386/4400`). The
  server itself cannot tell them apart, so the oracle classifies `change==field_name` as `modify` and
  `change!=field_name` as `change`. A hand-parser that emits `chg c c` for `CHANGE c c` would diverge on
  this pathological input only.
- `ALTER TABLE t ADD (a INT, b INT)` and `ALTER TABLE t ADD a INT, ADD b INT` both flatten to the same
  ordered `create_list` (`Alter_info::add_field` appends per column, `sql/sql_parse.cc:5720`); the
  grouping lives only on the private, `make_cmd`-consumed `PT_alter_table_add_columns::m_columns`
  (`parse_tree_nodes.h:4349`), unreadable without patching the RO tree. The oracle therefore emits **one
  `add` spec per column**. The hand-parser groups `ADD (a,b)` into one spec (writeup §parser).

  **Requirement on component 20:** before comparing, canonicalize *both* signatures by (i) flattening
  every multi-column `add`/`col …, …` spec into per-column `add` specs, and (ii) treating `modify c` and
  `change c c` as equal. Without this, these two construct families produce guaranteed false
  divergences. This does not change the digest schema; it constrains the reduction rules component 20
  owns (00-overview §"signature grammar" already delegates written-vs-defaulted reconciliation to 20 —
  this is the same class of rule). Recommend documenting these two normalizations explicitly in 20.

Everything else conforms to 00-overview as written.
