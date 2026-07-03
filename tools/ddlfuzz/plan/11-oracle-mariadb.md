# 11 — MariaDB 13.1 in-process parse oracle

Single-threaded C++ binary speaking protocol v1 (00-overview.md) on stdin/stdout. Parses each
statement with the real MariaDB parser (embedded server build of the pinned checkout) under a
per-case `sql_mode`, emits the contract digest JSON. Everything below was **validated on this
machine** during planning: the library is already built under `tools/ddlfuzz/build/` and three
probe programs (`build/probe/probe{,2,3}.cc`, kept as reference, safe to delete) exercised init,
parsing in all relevant sql_modes, digest field extraction, reject recovery, multi-statement
`found_semicolon`, and SanCov counter collection end-to-end. Where a claim says "probe-validated"
it is an observed result, not a hypothesis.

## Goal

Produce `tools/ddlfuzz/build/oracle-mariadb` such that, for every fuzz case
`(statement bytes, sqlMode u64)`, its verdict + digest is byte-for-byte what a MariaDB 13.1.0
server (commit `c3ec2dc368a8c7165cdbea58208eb828e76ebc57`, `~/Code/mariadb-server` main) would
decide at parse time, plus cumulative inline-8bit SanitizerCoverage over the server library.

## Interfaces

Provides:
- Binary `tools/ddlfuzz/build/oracle-mariadb` — protocol v1 msgs HELLO/PARSE_BATCH/GET_COVERAGE
  exactly per 00-overview.md. `argv[1]` (optional): scratch datadir; default
  `tools/ddlfuzz/build/oracle-scratch/mariadb.<pid>` (created on start). Each process needs its
  own datadir (Aria control-file is lock-exclusive), hence the pid default. stdout carries frames
  only; all logging is stderr (the embedded server already logs to stderr; driver must never
  write stdout except frames).
- Build script `tools/ddlfuzz/oracle/mariadb/build.sh` (idempotent, crash-restartable).
- HELLO json: `{"engine":"mariadb","server_version":"13.1.0","protocol":1}` (version =
  `MYSQL_SERVER_VERSION` from the build's `include/mysql_version.h`, truncated at first `-`).

Consumes:
- `tools/ddlfuzz/oracle/proto/proto.hpp` — specced by 10-oracle-mysql.md; this driver uses only:
  frame read/write (u32 len + body), the msg-type constants, and the JSON string-escape helper.
  Digest JSON is assembled with a local append-writer (sketch below); if proto.hpp ships a digest
  builder, prefer it, but do not block on it.
- `~/Code/mariadb-server` — READ-ONLY. Never run cmake/git-submodule against it (see D2).
- Host deps (all verified present): cmake 4.1.2, ninja, Apple clang 21, brew `openssl@3`,
  `pcre2`. NOT present and handled by build.sh: bison ≥ 2.4 (system is 2.3; MariaDB requires
  2.4+, `sql/CMakeLists.txt:72`) — built from source into `build/hostdeps`. Network needed once
  (bison tarball + `libmariadb` submodule fetch).

## Design decisions (with evidence)

### D1. Bootstrap: embedded-server init + direct parser calls (option a, hybrid)

Chosen: call `mysql_server_init()` (= `init_embedded_server`, `libmysqld/lib_sql.cc:532`) for
global init, then `create_embedded_thd()` (`libmysqld/lib_sql.cc:705`, declared `extern "C"` via
`libmysqld/embedded_priv.h:23`) for the THD, then bypass the client API entirely and call
`lex_start`/`parse_sql` directly per case. Rationale:

- `init_embedded_server` runs the full supported boot: `my_thread_init`, `sys_var_init`,
  `init_common_variables`, `init_server_components`, `my_tz_init`, `init_update_queries`
  (lib_sql.cc:532-660). This transitively initializes everything parsing touches that a
  hand-rolled init would have to replicate one segfault at a time: charsets, errmsgs, MDL,
  the sys-var hash (needed at *parse time* by `SET STATEMENT` — see D7), and `plugin_init`,
  which registers the `uuid`/`inet4`/`inet6` Type_handlers (both plugins are
  `MANDATORY RECOMPILE_FOR_EMBEDDED`, `plugin/type_uuid/CMakeLists.txt:16-18`,
  `plugin/type_inet/CMakeLists.txt:16-18` — compiled into the embedded lib, so `ADD c uuid`
  parses; probe-validated).
- `create_embedded_thd` is the exact THD recipe (`new THD(next_thread_id())`, `thread_stack`,
  `store_globals()`, `lex_start`, `init_for_queries`, `db= null_clex_str`); we pass
  `CLIENT_MULTI_STATEMENTS` (`1UL<<16`, `include/mysql_com.h:244`) so the grammar sets
  `found_semicolon` (gate at `sql/sql_yacc.yy:2124-2153` checks `thd->client_capabilities &
  CLIENT_MULTI_QUERIES && lip->multi_statements`; the lip flag defaults TRUE,
  `sql/sql_lex.cc:861`).
- Rejected (b) mimic `unittest/sql/`: those tests link `sql` + a stub `dummy_builtins.cc` but do
  `MY_INIT` only — none constructs a THD or parses (`unittest/sql/explain_filename-t.cc`);
  proves linking is possible, not that parse init is small.
- Rejected (c) minimal hand-rolled init as primary: prior art exists (mariabackup links
  `sql sql_builtins aria` and calls `setup_error_messages(); sys_var_init();
  plugin_mutex_init(); ...`, `extra/mariabackup/xtrabackup.cc:7410-7420`), but the THD ctor,
  plugin sysvars, find_sys_var, and data-type plugin registration form a long tail of hidden
  dependencies. Embedded init costs ~1s process start (measured) — negligible vs. respawn rate.
  (c) remains the fallback if embedded init ever becomes a problem.
- Embedded init boots fine against an **empty datadir** with no InnoDB (we build with
  `PLUGIN_INNOBASE=NO`): probe-validated; it creates only Aria logs/control file (~KBs) in the
  scratch dir. Stderr noise like `Can't open the mysql.func table` is expected and harmless.

Exact init sequence (probe-validated):

```cpp
static char *srv_argv[]= { (char*)"oracle-mariadb",
  datadir_arg,                       // "--datadir=<scratch>"
  lcdir_arg,                         // "--lc-messages-dir=<BUILD>/mariadb-build/sql/share"
  (char*)"--character-set-server=utf8mb4",
  (char*)"--skip-networking", nullptr };
static char *groups[]= { (char*)"server", (char*)"embedded", nullptr };
if (mysql_server_init(5, srv_argv, groups)) die("server init failed");
THD *thd= (THD*)create_embedded_thd(1UL<<16 /*CLIENT_MULTI_STATEMENTS*/);
thd->store_globals();                          // re-attach: create_embedded_thd calls reset_globals()
LEX_CSTRING db= {STRING_WITH_LEN(SENTINEL_DB)};
thd->set_db(&db);                              // see D8: parser requires a current db
thd->variables.character_set_client= &my_charset_utf8mb4_general_ci;   // deterministic client cs
thd->variables.collation_connection= &my_charset_utf8mb4_general_ci;
thd->variables.character_set_results= &my_charset_utf8mb4_general_ci;
thd->update_charset();                         // sql_class.h:5216
```

`store_globals()` also sets `thd->thread_stack` via `my_get_stack_bounds`
(`sql/sql_class.cc:2455-2457`), which `check_stack_overrun` requires (`sql/sql_parse.cc:7354`);
run everything on the main thread. Never call `mysql_server_end()` — the process is killed by
the client.

### D2. Sources: `git clone --shared` of the pinned commit; never configure the RO tree

`cmake/submodules.cmake:1-50` runs `git submodule update --init --recursive --depth 1` **in the
source dir** at configure time, and all submodules in `~/Code/mariadb-server` are uninitialized —
configuring in-tree would write into the RO checkout. Therefore build.sh clones:
`git clone --shared --no-checkout ~/Code/mariadb-server build/mariadb-src` (alternates → instant,
no object copy, no writes to origin), detaches at `c3ec2dc368a8c7165cdbea58208eb828e76ebc57`,
sets `git config cmake.update-submodules no` (honored at submodules.cmake:13-18), and manually
inits **only** `libmariadb` (required or configure FATAL_ERRORs, submodules.cmake:47-50). WSREP
is off so `wsrep-lib` is not needed; `WITH_SSL=system` avoids the wolfssl submodule.

### D3. Build config (validated: configure 72s, `ninja mysqlserver` ~11 min at -j6)

`WITH_EMBEDDED_SERVER=ON` produces the merged static archive
`build/mariadb-build/libmysqld/libmariadbd.a` (583 MB, target name `mysqlserver`,
`libmysqld/CMakeLists.txt:217`) — the whole server (sql compiled with `-DEMBEDDED_LIBRARY`,
mysys, strings, engines, plugins) in one archive; the driver links just it + system libs.
Heavy engines and InnoDB are disabled (parse-only oracle; smaller/faster build; InnoDB absence
probe-validated harmless). `RelWithDebInfo` ⇒ `-DNDEBUG -DDBUG_OFF` — asserts off, matching
production semantics and tolerating the diagnostics-area write-once discipline.

Full flag set is in build.sh below. Compile/link lines for the driver were derived from
`ninja -t commands mariadb-embedded` (the in-tree embedded client is the link-line donor) and
then minimized; the validated lines are hardcoded in build.sh with the donor command noted as
fallback for future flag drift.

### D4. SanitizerCoverage

- `-fsanitize-coverage=inline-8bit-counters` added globally via `CMAKE_C_FLAGS`/`CMAKE_CXX_FLAGS`.
  On darwin/arm64 Apple clang 21 links binaries with the hook left as a weak-resolved undefined
  symbol — build-time generator tools (gen_lex_hash etc.) link and run uninstrumented-hook fine
  (validated: `nm` shows `U ___sanitizer_cov_8bit_counters_init`, binary runs), so no stub
  archive is needed anywhere.
- The driver TU is compiled **without** the flag (its own edges must not pollute counters) and
  provides the strong hook:

```cpp
static std::vector<std::pair<uint8_t*,uint8_t*>> g_regions;
extern "C" void __sanitizer_cov_8bit_counters_init(uint8_t *start, uint8_t *stop) {
  if (start == stop) return;
  for (auto &r : g_regions) if (r.first == start && r.second == stop) return;  // dedup, see below
  g_regions.emplace_back(start, stop);
}
```

- **Dedup is load-bearing**: on Mach-O every instrumented TU's module ctor passes the *same*
  whole-section `section$start/end$__DATA$__sancov_cntrs` pair (645 identical calls observed);
  without dedup GET_COVERAGE ships 645 copies of the ~379 KB bitmap = 244 MB per poll.
- GET_COVERAGE response = `u32 n` + concatenation of all distinct regions in registration order
  (deterministic under static linking; ~379 KB total). Counters are cumulative, never reset by
  the oracle. Note: 8-bit counters wrap mod 256; the client OR-accumulates over time so transient
  zeros are absorbed.

### D5. sql_mode: install the raw u64 directly

`thd->variables.sql_mode = (sql_mode_t)case.sql_mode;` before each parse. Evidence this is
correct: the replication applier installs the binlog Q_SQL_MODE_CODE value the same way
(`sql/log_event_server.cc:1933`, modulo MODE_NO_DIR_IN_CREATE which is parse-irrelevant). Bit
layout `sql/sql_class.h:168-209` matches the writeup: ANSI_QUOTES=1<<2, ORACLE=1<<9,
MSSQL=1<<10, NO_BACKSLASH_ESCAPES=1<<20; MariaDB-specific 1<<32..34; `WAS_ORACLE`=1<<35 is
internal-only (harmless if a fuzzed mode sets it). The mode picks the parser inside `parse_sql`:
`thd->variables.sql_mode & MODE_ORACLE ? ORAparse(thd) : MYSQLparse(thd)`
(`sql/sql_parse.cc:10371-10372`; MariaDB compiles the grammar twice: `yy_mariadb.cc`,
`yy_oracle.cc`). MSSQL `[bracket]` idents and `/*M!` comments are pure lexer behavior, active in
plain `parse_sql` with no client flags involved (`sql/sql_lex.cc:2407-2551`) — probe-validated
(`ALTER TABLE [t] ADD [c] INT` under 1<<10; `/*M!100000 ... */`).

### D6. Per-statement parse + cleanup on one reused THD

Mirror of `mysql_parse` (`sql/sql_parse.cc:7890-7891`) + the `dispatch_command` epilogue
(`sql/sql_parse.cc:2493-2499`). Probe-validated over mixed accept/reject sequences with no state
bleed and flat memory:

```cpp
// case buffer: writable copy + 1 trailing NUL. Writable is REQUIRED: the lexer patches
// non-matching version comments in place (sql_lex.cc:2524-2540). Embedded NULs in stmt bytes
// are passed through; the lexer treats NUL as end-of-input exactly like the server.
std::vector<char> buf(stmt, stmt + len); buf.push_back('\0');

thd->variables.sql_mode= sql_mode;
lex_start(thd);
thd->reset_for_next_command();          // clears diagnostics area (sql_parse.cc:7437,7451,7516)
thd->set_query(cur, curlen);            // pointer install, no copy (sql_class.h:5821)
Parser_state ps;
if (ps.init(thd, cur, curlen)) goto oom;   // sql_lex.h:5351
bool err= parse_sql(thd, &ps, nullptr);    // sql_parse.cc:10328
// ... extract digest or error HERE (all names live on thd->mem_root) ...
const char *fsemi= ps.m_lip.found_semicolon;   // save before cleanup
// per-statement cleanup (mysql_parse epilogue sql_parse.cc:7967-7969 + dispatch tail 2493-2499):
thd->end_statement();                                  // lex_end etc., sql_class.cc:4462
thd->Item_change_list::rollback_item_tree_changes();
thd->cleanup_after_query();                            // frees free_list
thd->lex->restore_set_statement_var();
thd->lex->m_sql_cmd= nullptr;
free_root(thd->mem_root, MYF(MY_KEEP_PREALLOC));
```

On reject: `thd->is_error()` is true; read `thd->get_stmt_da()->sql_errno()` and
`thd->get_stmt_da()->message()` (NOT `message_text()` — that's the MySQL name;
`sql/sql_error.h:1077,1085`) **before** the cleanup block, which clears it. `parse_sql` already
ran `LEX::cleanup_lex_after_parse_error` internally (sql_parse.cc:10374-10380). Subsequent
parses are unaffected (probe-validated: garbage → 1064, then a clean accept).

Stack safety: `check_stack_overrun` works once `thread_stack` is set (D1); bison recursion is
heap-grown and capped (`YYMAXDEPTH 3200`, `sql_yacc.yy:32`; grows to `MY_YACC_MAX 32000`,
`sql_parse.cc:7386-7398`, then clean parse error). Deep inputs reject gracefully, not crash.

### D7. Multi-statement + SET STATEMENT

**Loop** (mirror of `dispatch_command`, `sql/sql_parse.cc:1905-2004`): after an accepted parse,
if `ps.m_lip.found_semicolon != NULL` it points at the char **after** the `;`
(`sql_yacc.yy:2124-2153`; NULL when the statement ends at EOF). Advance `cur` there, trim leading
`my_isspace(thd->charset(), *cur)` like sql_parse.cc:1930, and parse again — **even if the
remainder is empty**: the real dispatcher does, and an empty parse yields ER_EMPTY_QUERY 1065.
Probe-validated semantics:

| input | result |
|---|---|
| `A` / `A;` | one stmt, `found_semicolon` NULL |
| `A; B` | stmt A accepted, `found_semicolon` → `B` |
| `A; ` / `A;;` | A accepted, then next parse rejects 1065 "Query was empty" |
| `` / ` ` | reject 1065 |
| `# comment` | accept (digest `[{"kind":"other"}]`) |

Digest verdict rule: reject at statement k (any k, including 1065 on an empty tail) ⇒ the whole
case is `{"verdict":"reject","error":"<errno>: <message>"}`. Justification: the server executes
statements up to k then errors, so this exact event text can never be a successfully-executed
binlog event — reject = unreachable is precisely the contract's meaning. Progress is guaranteed
(found_semicolon strictly advances past the `;`), so no statement-count cap is needed.

**SET STATEMENT var=val FOR <stmt>** (probe-validated + source): there is no sub-statement LEX.
The grammar (`sql_yacc.yy:17548-17559`) stashes the var list into `lex->stmt_var_list` and the
trailing `directly_executable_statement` is parsed **into the same LEX**, overwriting
`sql_command`. After `SET STATEMENT max_statement_time=0 FOR ALTER TABLE t ADD c INT`:
`lex->sql_command == SQLCOM_ALTER_TABLE`, `alter_info` populated normally, `stmt_var_list`
non-empty. ⇒ **The driver extracts the inner statement's facts with zero special-casing**; the
digest simply shows the ALTER, as the comparison side expects. Nested SET STATEMENT: innermost
wins (plain list overwrite; `mysql-test/main/set_statement.test:836`), still nothing to do.
Caveat that IS behavior: variable names are resolved at **parse time** — the yacc action calls
`find_sys_var` (`sql_yacc.yy:17647-17688` → `sql/sql_lex.cc:9725` → `sql/sql_plugin.cc:2976`),
so `SET STATEMENT nonexistent=1 FOR ALTER ...` is a parse-time reject 1193 "Unknown system
variable" (probe-validated). This is faithful server behavior; the parser under test accepts it,
and the comparison side treats oracle-reject as unreachable — correct, since such a statement
could never have executed.

### D8. Digest extraction from LEX

All structures are parse-time-complete in MariaDB (yacc actions fill `LEX::alter_info` directly).
Everything below is probe-validated. Names (`Lex_ident_*` = LEX_CSTRING) are already dequoted,
escape-collapsed, charset-converted to system charset, and mem_root-allocated
(`THD::to_ident_sys_alloc`, `sql/sql_class.cc:2807-2821`) — extract before `free_root`.

- `lex->sql_command` (`sql/sql_lex.h:1724`): `SQLCOM_ALTER_TABLE`, `SQLCOM_RENAME_TABLE`
  (`sql/sql_command.h:27,51`); anything else ⇒ `{"kind":"other"}`.
- **alter_table**: schema/table from the first `TABLE_LIST`: `lex->query_tables->db.str` /
  `->table_name.str` (grammar puts the target there, `sql_yacc.yy:7580-7585`). Do NOT read
  `Alter_info::db/table_name` (execution-time only, `sql/sql_table.cc:4913`) and do NOT read
  `lex->first_select_lex()->db` or `lex->name` (clobbered by `ALTER ... RENAME TO`,
  `sql/sql_lex.cc:13035-13055` — we ignore table renames inside ALTER per contract; taking the
  table from query_tables is immune).
- **Sentinel current-db**: the parser *requires* a current db for unqualified names —
  `add_table_to_list` → `copy_db_to` raises ER_NO_DB_ERROR and fails the parse otherwise
  (`sql/sql_parse.cc:8137-8148`, `sql/sql_class.h:5515-5532`). Driver sets
  `SENTINEL_DB = "peerdb_ddlfuzz_nodb"` once at init; at digest time `db == sentinel` ⇒ emit
  `"schema":""`, else emit the db text. (See Contract issues #3.)
- **Specs** from `lex->alter_info` (`sql/sql_alter.h:31`; members: `create_list` :100,
  `drop_list` :92, `alter_list` :94). MariaDB **flattens** specs: every ADD/CHANGE/MODIFY column
  lands in the single `create_list` in textual order with no spec-boundary markers
  (`sql_yacc.yy:6395`; grouped `ADD (a int, b int)` is byte-identical Alter_info to
  `ADD a int, ADD b int`), and drops/renames live in separate lists, losing inter-class order.
  Canonical emission order (see Contract issues #2):
  1. walk `create_list` (`List_iterator<Create_field>`): per `Create_field cf`
     (`sql/field.h:5915`):
     - `cf->change.str == NULL` ⇒ `{"op":"add","cols":[col],"has_position":hp}` (one op per
       column — grouped adds are split);
     - `cf->change.str == cf->field_name.str` (pointer-equal; MODIFY does
       `$4->change= $4->field_name`, `sql_yacc.yy:8156`) ⇒ `{"op":"modify",...}`;
     - else ⇒ `{"op":"change","old_name":cf->change.str,...}` (`sql_yacc.yy:8148`).
     `hp = cf->after.str != NULL`; FIRST is `after.str == first_keyword` (global,
     `sql/mysqld.cc:310`; server discriminates by pointer, `sql/sql_table.cc:9007`) — both FIRST
     and AFTER x just set `has_position:true`.
     `not_null = (cf->flags & NOT_NULL_FLAG) != 0` (`include/mysql_com.h:142`).
  2. walk `alter_list`: entries with `Alter_column::is_rename()` (`sql/sql_class.h:438-458`)
     ⇒ `{"op":"rename_col","old_name":ac->name.str,"new_name":ac->new_name.str}`; non-rename
     entries (SET/DROP DEFAULT) are omitted (option-ish, not column-type-relevant per contract).
  3. walk `drop_list`: only `ad->type == Alter_drop::COLUMN` (`sql/sql_class.h:409-411`)
     ⇒ `{"op":"drop","old_name":ad->name.str}`; KEY/FK/CHECK/PERIOD drops omitted.
  Index/constraint/option/partition specs never enter these lists as columns — omitted for free.
  `specs` may end up empty (e.g. `ALTER TABLE t FORCE`) — emit `"specs":[]`.
- **rename_table**: pairs are consecutive `TABLE_LIST`s (even=old, odd=new) on
  `lex->first_select_lex()->table_list.first` chained via `next_local` — same walk as
  `rename_tables` (`sql/sql_rename.cc:517-531`):
  `for (t= first; t; t= t->next_local->next_local) pair(t, t->next_local);`
  Per-name schema from `t->db.str` (sentinel-mapped) — probe-validated
  (`RENAME TABLE a TO b, s1.c TO s2.d` ⇒ `fuzzdb.a→fuzzdb.b`, `s1.c→s2.d`).

### D9. `type_str` rendering (complete mapping)

Sources: handler name table `sql/sql_type.cc:128-172`, `sql_type_geom.cc:41-48`,
`sql_type_json.cc:29-39`, `sql_type_vector.cc:21`; plugin handlers take the plugin's name at
registration (`sql_type.cc:9806-9812`). Read via `cf->type_handler()->name().ptr()`
(`sql/sql_type.h:7933,4017`). Parse-time attribute state (`Column_definition_attributes`:
`length` field.h:5358, `decimals` :5361, `charset` :5359, `flags` :5473) is final by the time
`parse_sql` returns — `set_attributes` runs in the yacc action (`sql_yacc.yy:6406-6416`,
`field.cc:10796`) and **applies type defaults immediately** (see params policy below).

Algorithm (probe-validated on every row of the table that follows):

```cpp
// returns {base, params (vector<long long>), unsigned_suffix}
render(cf):
  name = cf->type_handler()->name()          // e.g. "int unsigned", "blob", "longblob/json"
  if name ends with " unsigned": base = name minus suffix; uns = true   // int family only
  else: base = name; uns = false
  if base ends with "/json": base = base minus "/json"                  // JSON -> longblob
  bin = (cf->charset == &my_charset_bin)     // POINTER compare; bare BINARY *attribute* sets
                                             // CONTEXT_COLLATION_FLAG instead (flag 1<<17,
                                             // mysql_com.h:161) and must NOT remap
  switch base:
    tinyint|smallint|mediumint|int|bigint:  params=[length]             // defaults filled: see below
    decimal:   uns |= flags&(UNSIGNED_FLAG|ZEROFILL_FLAG)
               p = length - (decimals>0) - (uns?0:1)                    // invert my_decimal_precision_to_length
               params=[p, decimals]                                     // (my_decimal.h:296-323)
    float|double: uns |= flags&(UNSIGNED_FLAG|ZEROFILL_FLAG)
               if decimals < 39 /*NOT_FIXED_DEC, my_global.h:1183*/: params=[length, decimals]
               else params=[]                                           // bare, or FLOAT(p) (p dropped at parse)
    bit|year:  params=[length]; uns=false    // year has internal UNSIGNED|ZEROFILL (sql_type.cc:2827) — never emit
    date:      params=[]
    time|datetime|timestamp: params = decimals>0 ? [decimals] : []      // fsp; uns=false (timestamp
                                                                        // has internal UNSIGNED_FLAG, sql_type.cc:3076)
    char:      if bin: base="binary";     params=[length]               // CHAR no len -> length==1 at parse
    varchar:   if bin: base="varbinary";  params=[length]               // covers VARCHAR2/RAW (oracle) too
    tinyblob|blob|mediumblob|longblob:
               if length>0: base = promote(length)   // <=255 tinyblob, <=65535 blob,
                                                     // <=16777215 mediumblob, else longblob
                                                     // (Type_handler::blob_type_handler, sql_type.cc:1421-1431;
                                                     // server applies at DDL time, sql_table.cc:4226)
               if !bin: base = {tinyblob->tinytext, blob->text, mediumblob->mediumtext, longblob->longtext}[base]
               params=[]                             // I_S style: no params on blob/text
    enum|set:  params=[]                             // value list not rendered (see Contract issues #6)
    vector:    params=[length/4]                     // parse stores 4*n (sql_type_vector.cc:73-97)
    default:   params=[]                             // uuid(36) inet4 inet6, geometry family
                                                     // (point/linestring/...), future UDTs: emit name as-is
  type_str = base + ("(" + join(params,",") + ")" if params nonempty) + (" unsigned" if uns)
  params_written = params nonempty ? params : null
```

Reference results (all observed via probe2; `→` is emitted `type_str`):

| DDL (mode) | handler,len,dec,cs | → |
|---|---|---|
| `DECIMAL(10,2) UNSIGNED` | decimal,11,2 | `decimal(10,2) unsigned` |
| `DECIMAL` | decimal,11,0 | `decimal(10,0)` |
| `INT` / `INT ZEROFILL` | int,11 / int unsigned,10 | `int(11)` / `int(10) unsigned` |
| `BIGINT UNSIGNED` | bigint unsigned,20 | `bigint(20) unsigned` |
| `SERIAL` | bigint unsigned,20, NOT_NULL | `bigint(20) unsigned` + not_null (yy:6417-6428) |
| `BOOL` | tinyint,1 | `tinyint(1)` (yy:6672-6679) |
| `CHAR(10)` | char,10,cs=NULL | `char(10)` |
| `CHAR(10) CHARACTER SET binary` / `CHAR(10) BYTE` / `BINARY(10)` | char,10,cs=bin | `binary(10)` |
| `CHAR(10) BINARY` | char,10,ctx-collation flag | `char(10)` (no remap!) |
| `VARBINARY(5)` / `RAW(5)` (oracle) | varchar,5,bin | `varbinary(5)` |
| `VARCHAR2(10)` (oracle) | varchar,10 | `varchar(10)` |
| `TEXT` / `BLOB` | blob,0,NULL / blob,0,bin | `text` / `blob` |
| `TEXT(70000)` | blob,70000,NULL | `mediumtext` (promotion) |
| `TEXT CHARACTER SET binary` | blob,0,bin | `blob` |
| `LONG VARBINARY` | mediumblob,0,bin | `mediumblob` |
| `CLOB` (oracle) | longblob,0,NULL | `longtext` |
| `BLOB` (oracle, lengthless) | longblob,0,bin | `longblob` (yy:6789-6792) |
| `JSON` | longblob/json,0,utf8mb4 | `longtext` |
| `NUMBER(10)` (oracle) | decimal,11,0 | `decimal(10,0)` |
| `NUMBER` (oracle) | double,22,39 | `double` |
| `FLOAT(30)` | double,22,39 | `double` (p dropped, yy:6654-6667) |
| `FLOAT(7,4) ZEROFILL` | float,7,4 | `float(7,4) unsigned` |
| `YEAR ZEROFILL` | year,4 | `year(4)` |
| `DATE` (oracle) | datetime,19,0 | `datetime` (implied oracle_schema, sql_schema.cc:28-33) |
| `mariadb_schema.date` (oracle) | date,10 | `date` (identity map, sql_schema.h:34) |
| `DATETIME(6)` | datetime,26,6 | `datetime(6)` |
| `NCHAR(3)` | char,3,utf8mb3 | `char(3)` |
| `uuid` / `inet6` / `VECTOR(4)` | uuid,36 / inet6,39 / vector,16 | `uuid` / `inet6` / `vector(4)` |
| `ENUM('a','b')` | enum,0 | `enum` |

**params_written policy (deviation, see Contract issues #1)**: MariaDB applies type param
defaults *during* parse (`set_length_and_dec` only copies explicit values, `field.cc:10810-10820`,
then `fix_attributes_*` fills defaults: int widths `field.cc:10893-10899` + `sql_type.cc:2789-2812`
— signed 4/6/9/11/20, unsigned 3/5/8/10/20; decimal (10,0) + precision→display-length
`field.cc:10925-10949`, `my_decimal.cc:333-341`). Written-vs-defaulted is unrecoverable
post-parse (`int` ≡ `int(11)`, `decimal` ≡ `decimal(10)` ≡ `decimal(10,0)`). The digest emits
`params_written` = the recorded (possibly defaulted) params per the algorithm above, `null` only
where the table says no params. The only qkind-load-bearing width, `tinyint(1)`, is exact:
width 1 occurs iff written `(1)` or `BOOL`/`BOOLEAN` (bare TINYINT defaults to 4/3).

### D10. Rejects

Digest `{"verdict":"reject","error":"<errno>: <message>"}` — errno + `: ` + verbatim
`Diagnostics_area::message()`. Examples (probe-validated): 1064 syntax error, 1193 unknown
system variable (SET STATEMENT), 1065 empty query, 1049/... never seen at parse. Messages are
deterministic for fixed input+mode (required for golden validation and dedup).

## Implementation steps

Directory layout:

```
tools/ddlfuzz/oracle/mariadb/
  build.sh        # everything: hostdep bison, clone, configure, ninja, driver compile
  main.cc         # init, frame loop, PARSE_BATCH/GET_COVERAGE/HELLO, per-case loop (D6/D7)
  digest.cc/.hpp  # LEX -> digest JSON (D8/D9); the only files that include server headers besides main.cc
  smoke.py        # PEP 723 uv script; spawns the binary, drives frames, asserts expected JSON
```

### Step 1 — `build.sh`

Idempotent; every phase skipped if its output exists. `BUILD=$(cd "$(dirname "$0")/../../build" && pwd)`.

```zsh
#!/bin/zsh
set -euo pipefail
BUILD=... SRC_RO=$HOME/Code/mariadb-server
COMMIT=c3ec2dc368a8c7165cdbea58208eb828e76ebc57

# 1. bison >= 2.4 (system is 2.3)
if [ ! -x "$BUILD/hostdeps/bin/bison" ]; then
  mkdir -p "$BUILD/hostdeps/src" && cd "$BUILD/hostdeps/src"
  curl -fsSLO https://ftp.gnu.org/gnu/bison/bison-3.8.2.tar.gz
  tar xzf bison-3.8.2.tar.gz && cd bison-3.8.2
  ./configure --prefix="$BUILD/hostdeps" && make -j6 && make install
fi

# 2. shared clone (never configure the RO tree — cmake would git-submodule-update it)
if [ ! -d "$BUILD/mariadb-src/.git" ]; then
  git clone --shared --no-checkout "$SRC_RO" "$BUILD/mariadb-src"
  cd "$BUILD/mariadb-src"
  git checkout --detach "$COMMIT"
  git config cmake.update-submodules no
  git submodule update --init --depth 1 libmariadb
fi

# 3. configure + build server archive (validated: 72s + ~11min)
cmake -G Ninja -S "$BUILD/mariadb-src" -B "$BUILD/mariadb-build" \
  -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DBISON_EXECUTABLE="$BUILD/hostdeps/bin/bison" \
  -DWITH_EMBEDDED_SERVER=ON -DWITH_WSREP=OFF -DWITH_UNIT_TESTS=OFF \
  -DWITH_SSL=system -DOPENSSL_ROOT_DIR=/opt/homebrew/opt/openssl@3 \
  -DPLUGIN_INNOBASE=NO -DPLUGIN_ROCKSDB=NO -DPLUGIN_MROONGA=NO -DPLUGIN_SPIDER=NO \
  -DPLUGIN_SPHINX=NO -DPLUGIN_CONNECT=NO -DPLUGIN_COLUMNSTORE=NO -DPLUGIN_S3=NO \
  -DPLUGIN_OQGRAPH=NO -DPLUGIN_FEDERATED=NO -DPLUGIN_FEDERATEDX=NO \
  -DCMAKE_C_FLAGS="-fsanitize-coverage=inline-8bit-counters" \
  -DCMAKE_CXX_FLAGS="-fsanitize-coverage=inline-8bit-counters"
ninja -C "$BUILD/mariadb-build" -j6 mysqlserver GenError

# 4. driver (validated compile+link line; NO sancov flag here — driver edges must not count).
#    If it ever breaks after a flag change upstream, re-derive from:
#    ninja -C "$BUILD/mariadb-build" mariadb-embedded && ninja -C "$BUILD/mariadb-build" -t commands mariadb-embedded
HERE=$(cd "$(dirname "$0")" && pwd)
/usr/bin/c++ -std=c++17 -O2 -g \
  -DEMBEDDED_LIBRARY -DMYSQL_SERVER -DHAVE_CONFIG_H -DDBUG_OFF -DNDEBUG \
  -I"$BUILD/mariadb-build/include" -I"$BUILD/mariadb-src/include/providers" \
  -I"$BUILD/mariadb-src/include" -I"$BUILD/mariadb-src/libmysqld" \
  -I"$BUILD/mariadb-src/sql" -I"$BUILD/mariadb-build/sql" \
  -I/opt/homebrew/include -I"$HERE/../proto" \
  "$HERE/main.cc" "$HERE/digest.cc" -o "$BUILD/oracle-mariadb" \
  "$BUILD/mariadb-build/libmysqld/libmariadbd.a" \
  -lz \
  /opt/homebrew/opt/openssl@3/lib/libssl.dylib /opt/homebrew/opt/openssl@3/lib/libcrypto.dylib \
  -L/opt/homebrew/lib -lpcre2-8
echo "built $BUILD/oracle-mariadb"
```

(Planning-time validation used the identical configure/ninja phases — see
`build/validate-mariadb.{sh,log}`; the driver line is the probe line with sources swapped.)

### Step 2 — `main.cc`

Include order (probe-validated): `"mariadb.h"`, `"sql_class.h"`, `"sql_lex.h"`, `"sql_parse.h"`,
`"sql_alter.h"`, `"field.h"`, `"table.h"`, then `<mysql_version.h>`, then proto.hpp and std
headers. Declare `extern "C" int mysql_server_init(int,char**,char**);` and
`extern "C" void *create_embedded_thd(unsigned long);` (avoids mixing client `mysql.h` into a
server-header TU).

```cpp
// globals: THD *g_thd; std::vector<std::pair<uint8_t*,uint8_t*>> g_regions; sancov hook (D4)

int main(int argc, char **argv) {
  std::string scratch = argc > 1 ? argv[1]
      : default_scratch();            // <exe dir>/oracle-scratch/mariadb.<pid>, mkdir -p
  // locate lc-messages dir relative to the binary: ../mariadb-build/sql/share won't exist if the
  // binary is moved; build.sh knows $BUILD, so bake it in with -D or compute from argv[0].
  // Simplest robust choice: #define DDLFUZZ_BUILD_DIR set by build.sh via -DDDLFUZZ_BUILD_DIR="..."
  ... init per D1 ...
  for (;;) {                                     // frame loop; EOF on stdin => exit 0
    Frame f = proto::read_frame(stdin);          // per proto.hpp
    switch (f.msg_type) {
    case proto::MSG_HELLO:  write hello json (D "Interfaces"); break;
    case proto::MSG_GET_COVERAGE: {
      u32 n = total region bytes; write frame: type + n + concatenated regions; break; }
    case proto::MSG_PARSE_BATCH: {
      u32 count = rd_u32();
      std::vector<std::string> digests(count);
      for each i: { u64 mode = rd_u64(); u32 sl = rd_u32(); bytes = rd(sl);
                    digests[i] = run_case(bytes, sl, mode); }       // D6/D7/D8
      write response: type + count + per-entry (u32 len + json); break; }
    default: fatal to stderr, exit 2;            // client respawns
    }
    fflush(stdout);
  }
}
```

`run_case` implements D6+D7 verbatim (loop over statements, one digest entry per accepted
statement, whole-case reject on first parse error) and calls `digest_stmt(thd->lex)` between
`parse_sql` and the cleanup block.

### Step 3 — `digest.cc`

Implements D8 + D9. JSON via a local append-writer using `proto`'s string escaper:

```cpp
struct Jw { std::string s;
  void raw(const char *r){ s += r; }
  void str(const char *p, size_t n){ s += '"'; proto::json_escape(s, p, n); s += '"'; } };

std::string digest_stmt(THD *thd);   // returns one stmts[] entry
// - other: {"kind":"other"}
// - alter_table: table from lex->query_tables (D8), specs per canonical walk (D8),
//   cols via render() (D9): {"name":..,"type_str":..,"not_null":..,"params_written":[..]|null}
// - rename_table: next_local pair walk (D8)
std::string schema_out(const LEX_CSTRING &db);   // sentinel -> "", else db text
```

Emit fields in the exact key order shown in the contract (stable output aids dedup/goldens).

### Step 4 — `smoke.py` (PEP 723, stdlib only; run `uv run smoke.py [--soak N]`)

Spawns `$BUILD/oracle-mariadb`, speaks frames (little-endian `struct.pack`), asserts:

1. HELLO → `{"engine":"mariadb","server_version":"13.1.0","protocol":1}`.
2. PARSE_BATCH with these cases (sql_mode, statement → expected digest, spelled out):

```
(0, "ALTER TABLE t ADD COLUMN c DECIMAL(10,2) UNSIGNED NOT NULL AFTER x") ->
{"verdict":"accept","stmts":[{"kind":"alter_table","schema":"","table":"t","specs":[
 {"op":"add","cols":[{"name":"c","type_str":"decimal(10,2) unsigned","not_null":true,
  "params_written":[10,2]}],"has_position":true}]}]}

(0, "ALTER TABLE db1.t ADD (a int, b varchar(20)), DROP COLUMN old, RENAME COLUMN p TO q") ->
{"verdict":"accept","stmts":[{"kind":"alter_table","schema":"db1","table":"t","specs":[
 {"op":"add","cols":[{"name":"a","type_str":"int(11)","not_null":false,"params_written":[11]}],"has_position":false},
 {"op":"add","cols":[{"name":"b","type_str":"varchar(20)","not_null":false,"params_written":[20]}],"has_position":false},
 {"op":"rename_col","old_name":"p","new_name":"q"},
 {"op":"drop","old_name":"old"}]}]}
   # note canonical order: adds, then rename_col, then drop (D8 / Contract issues #2)

(1<<9 /*ORACLE*/, "ALTER TABLE t ADD c VARCHAR2(10)") ->
{"verdict":"accept","stmts":[{"kind":"alter_table","schema":"","table":"t","specs":[
 {"op":"add","cols":[{"name":"c","type_str":"varchar(10)","not_null":false,"params_written":[10]}],"has_position":false}]}]}

(1<<9, "ALTER TABLE t ADD c NUMBER") -> ... type_str "double", params_written null

(0, "/*M!100000 ALTER TABLE t ADD c inet6 */") ->
 ... one alter_table spec, type_str "inet6", params_written null

(0, "SET STATEMENT max_statement_time=0 FOR ALTER TABLE t ADD c INT") ->
{"verdict":"accept","stmts":[{"kind":"alter_table","schema":"","table":"t","specs":[
 {"op":"add","cols":[{"name":"c","type_str":"int(11)","not_null":false,"params_written":[11]}],"has_position":false}]}]}

(0, "ALTER TABLE s.t MODIFY c BIGINT UNSIGNED FIRST; SELECT 1") ->
{"verdict":"accept","stmts":[{"kind":"alter_table","schema":"s","table":"t","specs":[
 {"op":"modify","cols":[{"name":"c","type_str":"bigint(20) unsigned","not_null":false,
  "params_written":[20]}],"has_position":true}]},{"kind":"other"}]}

(0, "RENAME TABLE a TO b, s1.c TO s2.d") ->
{"verdict":"accept","stmts":[{"kind":"rename_table","pairs":[
 {"old_schema":"","old_table":"a","new_schema":"","new_table":"b"},
 {"old_schema":"s1","old_table":"c","new_schema":"s2","new_table":"d"}]}]}

(1<<10 /*MSSQL*/, "ALTER TABLE [t] ADD [c] INT") -> ... schema "", table "t", col c int(11)

(0, "SELECT 1") -> {"verdict":"accept","stmts":[{"kind":"other"}]}
(0, "ALTER TABLE t GARBAGE") -> {"verdict":"reject","error":"1064: ..."}   # prefix-match "1064: "
(0, "ALTER TABLE t ADD d INT")  # immediately after the reject: must accept (recovery)
(0, "ALTER TABLE t ADD c INT; ") -> {"verdict":"reject","error":"1065: Query was empty"}
(0, "SET STATEMENT nonexistent_var=1 FOR ALTER TABLE t ADD c INT") -> reject "1193: ..."
```

3. GET_COVERAGE → `n > 1_000_000` bytes and at least one nonzero counter; a second call after
   another batch returns the same `n` and a counter-sum ≥ the first (cumulative).
4. Determinism: re-send the whole batch; byte-identical response.
5. `--soak N`: sends the batch ⌈N/len⌉ times; reports wall time and driver RSS
   (`ps -o rss= -p <pid>`) at 10% and 100%; asserts RSS growth < 64 MB.

## Build & commands

```
tools/ddlfuzz/oracle/mariadb/build.sh          # full build; ~16 min cold (bison 2m, clone 1m,
                                               # configure 72s, ninja ~11m @ -j6), seconds warm
uv run tools/ddlfuzz/oracle/mariadb/smoke.py   # smoke (assumes binary built)
uv run tools/ddlfuzz/oracle/mariadb/smoke.py --soak 1000000   # memory/throughput soak
```

Already materialized during planning (build.sh must detect and reuse them): `build/hostdeps`
(bison 3.8.2), `build/mariadb-src` (clone at pinned commit, libmariadb inited),
`build/mariadb-build` (configured + `mysqlserver`/`GenError` built, `libmariadbd.a` 583 MB).
`tools/ddlfuzz/.gitignore` covering `build/` and `state/` exists.

## Acceptance checks

1. `build.sh` exits 0 from both the current (warm) state and — spot-checked once if time allows —
   after `rm -rf build/mariadb-build` (reconfigure path).
2. `smoke.py` passes all assertions above (they encode D5-D10 behaviorally, including
   sql_mode=ORACLE, `/*M!`, SET STATEMENT-wrapping-ALTER, MSSQL brackets, multi-statement,
   reject-then-recover, coverage growth, determinism).
3. Soak: 1M statements, RSS growth < 64 MB, throughput reported (probe parse rate suggests
   > 10k stmts/s; no hard floor asserted).
4. Golden gate (component 20 owns the harness; this component must merely survive it):
   `ddlfuzz golden` replays all ~700 existing test-corpus statements through this oracle. The
   oracle side is done when every mismatch is attributable to a documented reconciliation rule
   (Contract issues below / 20's rules), not to driver bugs. Re-run after ANY oracle change
   (overview requirement).
5. No writes outside `/Users/ilia/Code/peerdb` at any point (verify: `git -C ~/Code/mariadb-server
   status --porcelain` is empty and no new files in that tree).

## Risks & fallbacks

- **proto.hpp drift** (spec owned by 10, parallel): only main.cc touches it; digest JSON writer
  is local. If the header is late, stub `read_frame/write_frame/json_escape` locally behind the
  same names and reconcile when it lands.
- **Apple-clang weak sancov hook**: relied-on darwin behavior, validated on Apple clang 21. If a
  future toolchain hard-errors on the undefined symbol for build tools, fallback: pre-compile a
  no-op `__sanitizer_cov_8bit_counters_init` stub object and append it via
  `-DCMAKE_EXE_LINKER_FLAGS=<stub.o>` (driver still links its strong definition, no stub).
- **Embedded init failure on future flag changes**: fallback bootstrap (c) — mariabackup-style
  minimal init (`xtrabackup.cc:7410`) + `create_embedded_thd` internals; only if (a) breaks.
- **Oracle crash on a fuzz input** (ORAparse and newer type paths are the likeliest): by
  protocol contract the client respawns and bisects the in-flight batch; the driver needs no
  in-process recovery. Keep init fast (~1s, no InnoDB) so respawns are cheap.
- **Diagnostics-area double-error aborts**: only enforced under debug asserts; we build NDEBUG
  and follow the reset discipline (D6) anyway.
- **Counter wrap transient zeros**: client OR-accumulates; acceptable by design (D4).
- **Datadir contention with 5 processes**: per-pid scratch default; document that the client may
  also pass distinct dirs via argv[1]. Stale scratch dirs are bounded (~KBs each); supervisor may
  clean `oracle-scratch/` on restart.

## Effort

- Build infra: **done** (validated this session); wiring into build.sh ~1h.
- main.cc + digest.cc: ~700 LOC, most of it transcribed from D6-D9 and the probe sources
  (`build/probe/probe*.cc` are working references for every server call) — 0.5-1 day.
- smoke.py + soak: ~250 LOC, 2-3h.
- Total: ~1-1.5 days.

## Contract issues

1. **`params_written` "exactly as written" is unachievable for MariaDB.** Param defaults are
   applied inside parse actions (`field.cc:10796-10971`); `int` vs `int(11)` and `decimal` vs
   `decimal(10,0)` are indistinguishable post-parse. This oracle emits recorded values per the
   D9 table (never null for int/decimal/char/varchar/bit/year/vector; null for
   blob/text/enum/set/date/uuid/inet*/geometry and paramless float/double/temporals).
   Component 20 must treat written-vs-defaulted as an equivalence (it already plans to, per the
   tidb-diff precedent); the load-bearing `tinyint(1)` case is exact.
2. **ALTER spec grouping and inter-class order are not preserved by the server.**
   `create_list` is flat (grouped `ADD (a,b)` ≡ `ADD a, ADD b`, `sql_yacc.yy:6395`) and
   drops/renames live in separate lists, so written interleaving of e.g. `DROP a, ADD b` is
   unrecoverable. Digest emits the canonical order: add/modify/change (create_list textual
   order, one op per column), then rename_col (alter_list order), then drop (drop_list order).
   20's reduction must canonicalize the parser-under-test side identically (split multi-column
   ADD specs; bucket-sort spec classes in the same order) before comparing. The MySQL oracle has
   the same Alter_info shape; 10 should adopt the identical canonical order.
3. **`"schema":""` needs a shared sentinel-db convention.** The MariaDB parser rejects
   unqualified table names without a current db (ER_NO_DB_ERROR at parse time,
   `sql_parse.cc:8137-8148`), so the driver installs current db `peerdb_ddlfuzz_nodb` and maps
   it back to `""` in the digest. A statement that literally writes that schema name would be
   mis-digested as unqualified; 10 (same problem in MySQL) and 20 must use the same sentinel and
   keep it out of generator dictionaries.
4. **Internally-flagged UNSIGNED/ZEROFILL types**: YEAR and TIMESTAMP carry UNSIGNED/ZEROFILL
   flags at parse regardless of the DDL (`sql_type.cc:2827,3076`), so written `YEAR ZEROFILL` is
   invisible; this oracle never emits `" unsigned"` for them (D9). If the parser under test
   renders `year ... unsigned` for written ZEROFILL, 20 needs a reconciliation entry.
5. **Reject semantics for multi-statement inputs**: contract implies per-case verdict; this
   oracle emits whole-case `reject` on the first failing statement (including the
   faithful-but-surprising 1065 on `"stmt; "` trailing whitespace — the real dispatcher parses
   the empty tail). This makes some inputs that the parser under test accepts compare as
   unreachable-by-invariant rather than as digests; that is the correct reading (the full event
   text could not have been binlogged), but 20 should be aware `"stmt;"` (accept, 1 stmt) and
   `"stmt; "` (reject) differ.
6. **`type_str` for enum/set omits the value list** (`"enum"`, `"set"` bare). The contract's
   `params_written` is numeric-only so nothing is lost for comparison (qkind reads the base),
   but if 10 emits value lists for MySQL, 20 must strip them; recommend both oracles emit bare.
