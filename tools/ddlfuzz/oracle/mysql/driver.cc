#include <algorithm>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>

#include "field_types.h"
#include "mysql/strings/m_ctype.h"
#include "mysql_com.h"
#include "sql-common/my_decimal.h"
#include "sql/check_stack.h"
#include "sql/create_field.h"
#include "sql/handler.h"
#include "sql/protocol_classic.h"
#include "sql/sql_alter.h"
#include "sql/sql_class.h"
#include "sql/sql_error.h"
#include "sql/sql_lex.h"
#include "sql/sql_list.h"
#include "sql/sql_parse.h"
#include "sql/sql_plugin.h"
#include "sql/sql_plugin_ref.h"
#include "sql/table.h"
#include "sql/mysqld.h"
#include "unittest/gunit/fake_table.h"
#include "unittest/gunit/test_utils.h"

#include "../proto/proto.hpp"

using ddlproto::GET_COVERAGE;
using ddlproto::HELLO;
using ddlproto::PARSE_BATCH;
using ddlproto::Reader;
using ddlproto::json_escape;
using ddlproto::put_u32;
using ddlproto::read_frame;
using ddlproto::write_frame;

int Fake_TABLE::highest_table_id = 5;

struct CovRegion {
  uint8_t *start;
  uint8_t *stop;
};

static constexpr size_t kMaxCoverageBytes = 64 * 1024 * 1024;
static CovRegion g_cov_regions[4096];
static size_t g_cov_region_count;

extern "C" void __sanitizer_cov_8bit_counters_init(uint8_t *start,
                                                    uint8_t *stop) {
  constexpr size_t kMaxCovRegions =
      sizeof(g_cov_regions) / sizeof(g_cov_regions[0]);
  if (start >= stop || g_cov_region_count >= kMaxCovRegions) return;
  const size_t len = static_cast<size_t>(stop - start);
  if (len > kMaxCoverageBytes) return;
  // On Mach-O every instrumented TU's module ctor passes the same
  // section$start/section$end boundary pair for the merged __sancov_cntrs
  // section; without dedup GET_COVERAGE would ship thousands of identical
  // copies of the bitmap.
  for (size_t i = 0; i < g_cov_region_count; ++i) {
    if (g_cov_regions[i].start == start && g_cov_regions[i].stop == stop)
      return;
  }
  g_cov_regions[g_cov_region_count++] = {start, stop};
}

struct RenderedType {
  std::string type_str;
  std::string params_json{"null"};
};

static void append_json_bool(std::string &out, bool v) {
  out += v ? "true" : "false";
}

static void append_json_cstr(std::string &out, const char *s) {
  if (s == nullptr) {
    json_escape(out, "", 0);
    return;
  }
  json_escape(out, s, std::strlen(s));
}

static void append_json_bytes(std::string &out, const char *s, size_t n) {
  if (s == nullptr) n = 0;
  json_escape(out, s == nullptr ? "" : s, n);
}

static bool is_binary_charset(const Create_field &cf) {
  return cf.charset == &my_charset_bin;
}

static bool oracle_is_integer_type(enum_field_types t) {
  return t == MYSQL_TYPE_TINY || t == MYSQL_TYPE_SHORT ||
         t == MYSQL_TYPE_INT24 || t == MYSQL_TYPE_LONG ||
         t == MYSQL_TYPE_LONGLONG || t == MYSQL_TYPE_BOOL;
}

static std::string base_type_name(const Create_field &cf) {
  switch (cf.sql_type) {
    case MYSQL_TYPE_TINY:
    case MYSQL_TYPE_BOOL:
      return "tinyint";
    case MYSQL_TYPE_SHORT:
      return "smallint";
    case MYSQL_TYPE_INT24:
      return "mediumint";
    case MYSQL_TYPE_LONG:
      return "int";
    case MYSQL_TYPE_LONGLONG:
      return "bigint";
    case MYSQL_TYPE_NEWDECIMAL:
      return "decimal";
    case MYSQL_TYPE_FLOAT:
      return "float";
    case MYSQL_TYPE_DOUBLE:
      return "double";
    case MYSQL_TYPE_BIT:
      return "bit";
    case MYSQL_TYPE_STRING:
      return is_binary_charset(cf) ? "binary" : "char";
    case MYSQL_TYPE_VARCHAR:
      return is_binary_charset(cf) ? "varbinary" : "varchar";
    case MYSQL_TYPE_TINY_BLOB:
      return is_binary_charset(cf) ? "tinyblob" : "tinytext";
    case MYSQL_TYPE_BLOB:
      return is_binary_charset(cf) ? "blob" : "text";
    case MYSQL_TYPE_MEDIUM_BLOB:
      return is_binary_charset(cf) ? "mediumblob" : "mediumtext";
    case MYSQL_TYPE_LONG_BLOB:
      return is_binary_charset(cf) ? "longblob" : "longtext";
    case MYSQL_TYPE_ENUM:
      return "enum";
    case MYSQL_TYPE_SET:
      return "set";
    case MYSQL_TYPE_JSON:
      return "json";
    case MYSQL_TYPE_YEAR:
      return "year";
    case MYSQL_TYPE_NEWDATE:
    case MYSQL_TYPE_DATE:
      return "date";
    case MYSQL_TYPE_TIME:
    case MYSQL_TYPE_TIME2:
      return "time";
    case MYSQL_TYPE_DATETIME:
    case MYSQL_TYPE_DATETIME2:
      return "datetime";
    case MYSQL_TYPE_TIMESTAMP:
    case MYSQL_TYPE_TIMESTAMP2:
      return "timestamp";
    case MYSQL_TYPE_VECTOR:
      return "vector";
    case MYSQL_TYPE_GEOMETRY:
      return "geometry";
    default:
      return "type_" + std::to_string(static_cast<int>(cf.sql_type));
  }
}

static RenderedType render_type(const Create_field &cf) {
  RenderedType rendered;
  rendered.type_str = base_type_name(cf);

  // YEAR carries UNSIGNED_FLAG internally but information_schema renders it
  // as plain "year"; suffixing it would flip the qkind to uint16.
  const bool unsigned_flag = cf.sql_type != MYSQL_TYPE_YEAR &&
                             (cf.flags & (UNSIGNED_FLAG | ZEROFILL_FLAG)) != 0;

  if (cf.sql_type == MYSQL_TYPE_NEWDECIMAL) {
    const size_t width = cf.max_display_width_in_codepoints();
    const uint scale = cf.decimals;
    const uint precision = my_decimal_length_to_precision(
        static_cast<uint>(width), scale, (cf.flags & UNSIGNED_FLAG) != 0);
    rendered.type_str += "(" + std::to_string(precision) + "," +
                         std::to_string(scale) + ")";
    rendered.params_json = "[" + std::to_string(precision) + "," +
                           std::to_string(scale) + "]";
  } else if (oracle_is_integer_type(cf.sql_type)) {
    if (cf.explicit_display_width()) {
      const size_t width = cf.max_display_width_in_codepoints();
      rendered.type_str += "(" + std::to_string(width) + ")";
      rendered.params_json = "[" + std::to_string(width) + "]";
    }
  } else if (cf.sql_type == MYSQL_TYPE_BIT) {
    const size_t width = cf.max_display_width_in_codepoints();
    rendered.type_str += "(" + std::to_string(width) + ")";
    rendered.params_json = "[" + std::to_string(width) + "]";
  } else if (cf.sql_type == MYSQL_TYPE_STRING ||
             cf.sql_type == MYSQL_TYPE_VARCHAR) {
    const size_t width = cf.max_display_width_in_codepoints();
    rendered.type_str += "(" + std::to_string(width) + ")";
    rendered.params_json = "[" + std::to_string(width) + "]";
  }

  if (unsigned_flag) rendered.type_str += " unsigned";
  return rendered;
}

static void emit_col(std::string &out, const Create_field &cf) {
  const RenderedType rendered = render_type(cf);
  out += "{\"name\":";
  append_json_cstr(out, cf.field_name);
  out += ",\"type_str\":";
  json_escape(out, rendered.type_str);
  out += ",\"not_null\":";
  append_json_bool(out, (cf.flags & NOT_NULL_FLAG) != 0);
  out += ",\"params_written\":";
  out += rendered.params_json;
  out += "}";
}

static std::string table_schema(const Table_ref *tr) {
  if (tr == nullptr || !tr->is_fqtn || tr->db == nullptr) return "";
  return std::string(tr->db, tr->db_length);
}

static std::string cstring_to_string(const LEX_CSTRING &s) {
  if (s.str == nullptr || s.length == 0) return "";
  return std::string(s.str, s.length);
}

static void emit_table_name(std::string &out, const Table_ref *tr) {
  append_json_bytes(out, tr == nullptr ? "" : tr->table_name,
                    tr == nullptr ? 0 : tr->table_name_length);
}

static void emit_alter(std::string &out, THD *thd) {
  LEX *lex = thd->lex;
  Alter_info *ai = lex->alter_info;
  Table_ref *tr = lex->query_block == nullptr
                      ? nullptr
                      : lex->query_block->get_table_list();

  out += "{\"kind\":\"alter_table\",\"schema\":";
  json_escape(out, table_schema(tr));
  out += ",\"table\":";
  emit_table_name(out, tr);
  if (ai != nullptr && (ai->flags & Alter_info::ALTER_RENAME) != 0 &&
      ai->new_table_name.str != nullptr) {
    // An unqualified rename target copies the session db (our sentinel) into
    // new_db_name; map it back to "" so the schema stays as written, like
    // table_schema does for the table itself.
    std::string new_schema = cstring_to_string(ai->new_db_name);
    if (new_schema == cstring_to_string(thd->db())) new_schema.clear();
    out += ",\"new_schema\":";
    json_escape(out, new_schema);
    out += ",\"new_table\":";
    json_escape(out, cstring_to_string(ai->new_table_name));
  }
  out += ",\"specs\":[";

  bool first_spec = true;
  if (ai != nullptr) {
    List_iterator_fast<Create_field> it(ai->create_list);
    Create_field *cf = nullptr;
    while ((cf = it++)) {
      if (!first_spec) out += ",";
      first_spec = false;
      const bool has_position = cf->after != nullptr;
      if (cf->change == nullptr) {
        out += "{\"op\":\"add\",\"cols\":[";
        emit_col(out, *cf);
        out += "],\"has_position\":";
        append_json_bool(out, has_position);
        out += "}";
      } else if (std::strcmp(cf->change, cf->field_name) == 0) {
        out += "{\"op\":\"modify\",\"cols\":[";
        emit_col(out, *cf);
        out += "],\"has_position\":";
        append_json_bool(out, has_position);
        out += "}";
      } else {
        out += "{\"op\":\"change\",\"old_name\":";
        append_json_cstr(out, cf->change);
        out += ",\"cols\":[";
        emit_col(out, *cf);
        out += "],\"has_position\":";
        append_json_bool(out, has_position);
        out += "}";
      }
    }

    for (const Alter_drop *drop : ai->drop_list) {
      if (drop == nullptr || drop->type != Alter_drop::COLUMN) continue;
      if (!first_spec) out += ",";
      first_spec = false;
      out += "{\"op\":\"drop\",\"old_name\":";
      append_json_cstr(out, drop->name);
      out += "}";
    }

    for (const Alter_column *alter : ai->alter_list) {
      if (alter == nullptr ||
          alter->change_type() != Alter_column::Type::RENAME_COLUMN) {
        continue;
      }
      if (!first_spec) out += ",";
      first_spec = false;
      out += "{\"op\":\"rename_col\",\"old_name\":";
      append_json_cstr(out, alter->name);
      out += ",\"new_name\":";
      append_json_cstr(out, alter->m_new_name);
      out += "}";
    }
  }

  out += "]}";
}

static void emit_rename(std::string &out, THD *thd) {
  LEX *lex = thd->lex;
  Table_ref *tr = lex->query_block == nullptr
                      ? nullptr
                      : lex->query_block->get_table_list();

  out += "{\"kind\":\"rename_table\",\"pairs\":[";
  bool first_pair = true;
  while (tr != nullptr && tr->next_local != nullptr) {
    const Table_ref *old_tr = tr;
    const Table_ref *new_tr = tr->next_local;
    if (!first_pair) out += ",";
    first_pair = false;
    out += "{\"old_schema\":";
    json_escape(out, table_schema(old_tr));
    out += ",\"old_table\":";
    emit_table_name(out, old_tr);
    out += ",\"new_schema\":";
    json_escape(out, table_schema(new_tr));
    out += ",\"new_table\":";
    emit_table_name(out, new_tr);
    out += "}";
    tr = new_tr->next_local;
  }
  out += "]}";
}

static std::string emit_stmt(THD *thd) {
  std::string out;
  switch (thd->lex->sql_command) {
    case SQLCOM_ALTER_TABLE:
      emit_alter(out, thd);
      break;
    case SQLCOM_RENAME_TABLE:
      emit_rename(out, thd);
      break;
    default:
      out = "{\"kind\":\"other\"}";
      break;
  }
  return out;
}

static std::string da_error(THD *thd) {
  Diagnostics_area *da = thd->get_stmt_da();
  if (da != nullptr && da->is_error()) {
    return std::to_string(da->mysql_errno()) + ": " + da->message_text();
  }
  return "0: parse error";
}

static std::string reject_digest(const std::string &err) {
  std::string out = "{\"verdict\":\"reject\",\"error\":";
  json_escape(out, err);
  out += "}";
  return out;
}

static std::string accept_digest(const std::vector<std::string> &stmts) {
  std::string out = "{\"verdict\":\"accept\",\"stmts\":[";
  for (size_t i = 0; i < stmts.size(); ++i) {
    if (i != 0) out += ",";
    out += stmts[i];
  }
  out += "]}";
  return out;
}

static void reclaim_after_event(THD *thd) {
  thd->cleanup_after_query();
  if (thd->mem_root->allocated_size() < 40960) {
    thd->mem_root->ClearForReuse();
  } else {
    thd->mem_root->Clear();
  }
}

static std::string parse_event(THD *thd, uint64_t sql_mode, const char *stmt,
                               size_t len) {
  thd->variables.sql_mode = sql_mode;

  std::string mut(stmt, len);
  char *p = mut.data();
  char *end = mut.data() + mut.size();
  size_t remaining = static_cast<size_t>(end - p);
  std::vector<std::string> stmts;

  if (len == 0) {
    Parser_state st;
    if (st.init(thd, p, 0)) {
      reclaim_after_event(thd);
      return reject_digest("0: parser init failed");
    }
    mysql_reset_thd_for_next_command(thd);
    thd->reset_rewritten_query();
    if (lex_start(thd)) {
      reclaim_after_event(thd);
      return reject_digest("0: lex_start failed");
    }
    thd->set_query(p, 0);
    thd->set_query_id(next_query_id());
    const bool failed = parse_sql(thd, &st, nullptr) || thd->is_error();
    const std::string result =
        failed ? reject_digest(da_error(thd)) : accept_digest({emit_stmt(thd)});
    reclaim_after_event(thd);
    return result;
  }

  while (remaining > 0) {
    Parser_state st;
    if (st.init(thd, p, remaining)) {
      reclaim_after_event(thd);
      return reject_digest("0: parser init failed");
    }

    mysql_reset_thd_for_next_command(thd);
    thd->reset_rewritten_query();
    if (lex_start(thd)) {
      reclaim_after_event(thd);
      return reject_digest("0: lex_start failed");
    }
    thd->set_query(p, remaining);
    thd->set_query_id(next_query_id());

    if (parse_sql(thd, &st, nullptr) || thd->is_error()) {
      const std::string err = da_error(thd);
      reclaim_after_event(thd);
      return reject_digest(err);
    }

    stmts.push_back(emit_stmt(thd));

    const char *next = st.m_lip.found_semicolon;
    if (next == nullptr) break;
    if (next <= p || next > end) break;

    p = const_cast<char *>(next);
    remaining = static_cast<size_t>(end - p);
    while (remaining > 0 &&
           (*p == ';' || my_isspace(thd->charset(), *p))) {
      ++p;
      --remaining;
    }
  }

  std::string result = accept_digest(stmts);
  reclaim_after_event(thd);
  return result;
}

static void append_coverage(std::string &resp) {
  size_t total = 0;
  for (size_t i = 0; i < g_cov_region_count; ++i) {
    const size_t len =
        static_cast<size_t>(g_cov_regions[i].stop - g_cov_regions[i].start);
    if (len > kMaxCoverageBytes - total) break;
    total += len;
  }
  put_u32(resp, static_cast<uint32_t>(total));
  size_t written = 0;
  for (size_t i = 0; i < g_cov_region_count; ++i) {
    const size_t len =
        static_cast<size_t>(g_cov_regions[i].stop - g_cov_regions[i].start);
    if (len > kMaxCoverageBytes - written) break;
    resp.append(reinterpret_cast<const char *>(g_cov_regions[i].start),
                len);
    written += len;
  }
}

// Bootstraps the component registry. The gunit harness leaves srv_registry
// null, and stored-program teardown does a my_service lookup through it, so
// parse-error cleanup of any stored-program statement (CREATE
// PROCEDURE/FUNCTION/TRIGGER/EVENT) would SIGSEGV without it.
extern bool initialize_minimal_chassis(SERVICE_TYPE_NO_CONST(registry) *
                                       *registry);

// The gunit bootstrap loads no storage-engine plugins, so
// global_system_variables.table_plugin / temp_table_plugin stay null. Any
// CREATE TABLE whose ENGINE clause fails to resolve (every named engine here
// — none are registered) takes the engine-substitution path in
// PT_create_table_stmt::make_cmd -> HA_CREATE_INFO::set_db_type ->
// ha_default_handlerton -> plugin_lock, which derefs the null default plugin
// ref and SIGSEGVs (finding 64ce75c8aad4). Register a parse-only stand-in
// engine via the same insert_hton2plugin hook the server's own unit tests use
// (unittest/gunit/opt_costconstants-t.cc) and install it as the default:
// ha_default_plugin then short-circuits on the THD-cached ref before ever
// touching plugin_lock. The digest never reads the handlerton (CREATE TABLE
// digests as kind:"other"), so a zeroed hton is safe for parse +
// contextualize; downstream parse-time consumers are null-tolerant
// (ha_resolve_storage_engine_name already special-cases unit tests).
static handlerton g_stand_in_hton{};
static st_plugin_int g_stand_in_se_plugin{};

static void install_default_se_stand_in(THD *thd) {
  if (global_system_variables.table_plugin != nullptr &&
      global_system_variables.temp_table_plugin != nullptr) {
    return;
  }
  const uint slot = static_cast<uint>(num_hton2plugins());
  g_stand_in_hton.slot = slot;
  g_stand_in_hton.state = SHOW_OPTION_YES;
  g_stand_in_hton.db_type = DB_TYPE_UNKNOWN;
  g_stand_in_se_plugin.name = {STRING_WITH_LEN("ddlfuzz_stand_in")};
  g_stand_in_se_plugin.state = PLUGIN_IS_READY;
  g_stand_in_se_plugin.ref_count = 1;
  g_stand_in_se_plugin.data = &g_stand_in_hton;
  insert_hton2plugin(slot, &g_stand_in_se_plugin);
#ifdef NDEBUG
  const plugin_ref ref = &g_stand_in_se_plugin;
#else
  static st_plugin_int *ref_holder = &g_stand_in_se_plugin;
  const plugin_ref ref = &ref_holder;
#endif
  if (global_system_variables.table_plugin == nullptr)
    global_system_variables.table_plugin = ref;
  if (global_system_variables.temp_table_plugin == nullptr)
    global_system_variables.temp_table_plugin = ref;
  // thd->variables was copied from the globals at THD construction, before
  // this ran; patch the session copies the fast path actually reads.
  if (thd->variables.table_plugin == nullptr)
    thd->variables.table_plugin = ref;
  if (thd->variables.temp_table_plugin == nullptr)
    thd->variables.temp_table_plugin = ref;
}

int main(int, char **argv) {
  const char *progname =
      (argv != nullptr && argv[0] != nullptr) ? argv[0] : "oracle-mysql";
  initialize_stack_direction();
  MY_INIT(progname);
  if (initialize_minimal_chassis(&srv_registry)) {
    std::fprintf(stderr, "oracle-mysql: minimal chassis init failed\n");
    return 2;
  }
  my_testing::setup_server_for_unit_tests();
  error_handler_hook = my_message_sql;

  auto *init = new my_testing::Server_initializer();
  init->SetUp();
  THD *thd = init->thd();
  install_default_se_stand_in(thd);

  // macOS autosizes lower_case_table_names=2 on a case-insensitive datadir
  // (mysqld.cc), folding schema/table idents at parse — unfaithful to Linux
  // production (lctn=0). This is a parse-only process (no table files ever
  // opened), so override the exported globals post-init, mirroring the MariaDB
  // oracle (plan 12 part 1 step 3).
  lower_case_table_names = 0;
  table_alias_charset = &my_charset_bin;

  thd->get_protocol_classic()->add_client_capability(CLIENT_MULTI_QUERIES);
  thd->variables.character_set_client = &my_charset_utf8mb4_0900_ai_ci;
  thd->variables.character_set_results = &my_charset_utf8mb4_0900_ai_ci;
  thd->variables.collation_connection = &my_charset_utf8mb4_0900_ai_ci;
  thd->update_charset();
  // Sentinel session db so an unqualified rename target (which copies the
  // session db) can be told apart from one written as a real schema name;
  // mirrors kSentinelDb in the MariaDB oracle.
  static const char kSentinelDb[] = "peerdb_ddlfuzz_nodb";
  LEX_CSTRING db{kSentinelDb, sizeof(kSentinelDb) - 1};
  thd->reset_db(db);

  std::vector<uint8_t> body;
  while (read_frame(STDIN_FILENO, body)) {
    if (body.empty()) continue;

    const uint8_t msg = body[0];
    Reader r(body);
    const uint8_t *skip = nullptr;
    if (!r.bytes(skip, 1)) break;

    std::string resp;
    resp.push_back(static_cast<char>(msg));

    if (msg == HELLO) {
      const std::string hello =
          "{\"engine\":\"mysql\",\"server_version\":\"9.7.0\",\"protocol\":1}";
      put_u32(resp, static_cast<uint32_t>(hello.size()));
      resp += hello;
    } else if (msg == GET_COVERAGE) {
      append_coverage(resp);
    } else if (msg == PARSE_BATCH) {
      uint32_t count = 0;
      if (!r.u32(count)) break;
      put_u32(resp, count);
      for (uint32_t i = 0; i < count; ++i) {
        uint64_t mode = 0;
        uint32_t slen = 0;
        const uint8_t *sp = nullptr;
        if (!r.u64(mode) || !r.u32(slen) || !r.bytes(sp, slen)) {
          _exit(2);
        }
        const std::string digest =
            parse_event(thd, mode, reinterpret_cast<const char *>(sp), slen);
        put_u32(resp, static_cast<uint32_t>(digest.size()));
        resp += digest;
      }
    }

    if (!write_frame(STDOUT_FILENO, resp)) break;
  }

  // MySQL's gunit teardown destructs THD state that the parser leaves pointing
  // into process-lifetime globals on this standalone link. The oracle has no
  // reusable state after EOF, so exit directly and let the OS reclaim it.
  _exit(0);
}
