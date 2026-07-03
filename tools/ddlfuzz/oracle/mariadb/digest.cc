#include "digest.hpp"

#include "mariadb.h"
#include "sql_class.h"
#include "sql_lex.h"
#include "sql_alter.h"
#include "field.h"
#include "table.h"

#include <algorithm>
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

namespace ddlfuzz_mariadb {
namespace {

struct Jw {
  std::string s;

  void raw(const char *v) { s += v; }

  void str(const char *p, size_t n) {
    s.push_back('"');
    if (p != nullptr && n != 0)
      proto::json_escape(s, p, n);
    s.push_back('"');
  }

  void str(const LEX_CSTRING &v) { str(v.str, v.length); }

  void boolean(bool v) { s += v ? "true" : "false"; }
};

struct RenderedType {
  std::string type_str;
  std::vector<long long> params;
};

bool ends_with(const std::string &s, const char *suffix) {
  size_t n = std::strlen(suffix);
  return s.size() >= n && s.compare(s.size() - n, n, suffix) == 0;
}

bool is_sentinel(const LEX_CSTRING &db) {
  size_t n = std::strlen(kSentinelDb);
  return db.str != nullptr && db.length == n &&
         std::memcmp(db.str, kSentinelDb, n) == 0;
}

void write_schema(Jw &jw, const LEX_CSTRING &db) {
  if (db.str == nullptr || db.length == 0 || is_sentinel(db))
    jw.str("", 0);
  else
    jw.str(db);
}

std::string promoted_blob_base(ulonglong length) {
  if (length <= 255)
    return "tinyblob";
  if (length <= 65535)
    return "blob";
  if (length <= 16777215)
    return "mediumblob";
  return "longblob";
}

std::string text_base_for_blob(const std::string &base) {
  if (base == "tinyblob")
    return "tinytext";
  if (base == "blob")
    return "text";
  if (base == "mediumblob")
    return "mediumtext";
  if (base == "longblob")
    return "longtext";
  return base;
}

void push_param(std::vector<long long> &params, ulonglong v) {
  params.push_back(static_cast<long long>(v));
}

RenderedType render_type(const Create_field *cf) {
  std::string base = cf->type_handler()->name().ptr();
  bool uns = false;
  if (ends_with(base, " unsigned")) {
    base.resize(base.size() - std::strlen(" unsigned"));
    uns = true;
  }
  if (ends_with(base, "/json"))
    base.resize(base.size() - std::strlen("/json"));

  const bool bin = cf->charset == &my_charset_bin;
  std::vector<long long> params;

  if (base == "boolean")
    base = "tinyint";

  if (base == "tinyint" || base == "smallint" || base == "mediumint" ||
      base == "int" || base == "bigint") {
    push_param(params, cf->length);
  } else if (base == "decimal") {
    uns = uns || (cf->flags & (UNSIGNED_FLAG | ZEROFILL_FLAG));
    long long precision = static_cast<long long>(cf->length) -
                          (cf->decimals > 0 ? 1 : 0) - (uns ? 0 : 1);
    if (precision < 0)
      precision = 0;
    params.push_back(precision);
    params.push_back(static_cast<long long>(cf->decimals));
  } else if (base == "float" || base == "double") {
    uns = uns || (cf->flags & (UNSIGNED_FLAG | ZEROFILL_FLAG));
    if (cf->decimals < NOT_FIXED_DEC) {
      push_param(params, cf->length);
      params.push_back(static_cast<long long>(cf->decimals));
    }
  } else if (base == "bit" || base == "year") {
    push_param(params, cf->length);
    uns = false;
  } else if (base == "date") {
    uns = false;
  } else if (base == "time" || base == "datetime" || base == "timestamp") {
    if (cf->decimals > 0)
      params.push_back(static_cast<long long>(cf->decimals));
    uns = false;
  } else if (base == "char") {
    if (bin)
      base = "binary";
    push_param(params, cf->length);
  } else if (base == "varchar") {
    if (bin)
      base = "varbinary";
    push_param(params, cf->length);
  } else if (base == "tinyblob" || base == "blob" ||
             base == "mediumblob" || base == "longblob") {
    if (cf->length > 0)
      base = promoted_blob_base(cf->length);
    if (!bin)
      base = text_base_for_blob(base);
    uns = false;
  } else if (base == "enum" || base == "set") {
    uns = false;
  } else if (base == "vector") {
    push_param(params, cf->length / 4);
    uns = false;
  } else {
    uns = false;
  }

  std::string type_str = base;
  if (!params.empty()) {
    type_str.push_back('(');
    for (size_t i = 0; i < params.size(); i++) {
      if (i != 0)
        type_str.push_back(',');
      type_str += std::to_string(params[i]);
    }
    type_str.push_back(')');
  }
  if (uns)
    type_str += " unsigned";

  return {type_str, params};
}

void write_params(Jw &jw, const std::vector<long long> &params) {
  if (params.empty()) {
    jw.raw("null");
    return;
  }
  jw.raw("[");
  for (size_t i = 0; i < params.size(); i++) {
    if (i != 0)
      jw.raw(",");
    jw.raw(std::to_string(params[i]).c_str());
  }
  jw.raw("]");
}

void write_col(Jw &jw, const Create_field *cf) {
  RenderedType rt = render_type(cf);
  jw.raw("{\"name\":");
  jw.str(cf->field_name);
  jw.raw(",\"type_str\":");
  jw.str(rt.type_str.data(), rt.type_str.size());
  jw.raw(",\"not_null\":");
  jw.boolean((cf->flags & NOT_NULL_FLAG) != 0);
  jw.raw(",\"params_written\":");
  write_params(jw, rt.params);
  jw.raw("}");
}

void write_create_spec(Jw &jw, const Create_field *cf) {
  const bool has_position = cf->after.str != nullptr;
  if (cf->change.str == nullptr) {
    jw.raw("{\"op\":\"add\",\"cols\":[");
    write_col(jw, cf);
    jw.raw("],\"has_position\":");
    jw.boolean(has_position);
    jw.raw("}");
    return;
  }

  if (cf->change.str == cf->field_name.str) {
    jw.raw("{\"op\":\"modify\",\"cols\":[");
    write_col(jw, cf);
    jw.raw("],\"has_position\":");
    jw.boolean(has_position);
    jw.raw("}");
    return;
  }

  jw.raw("{\"op\":\"change\",\"old_name\":");
  jw.str(cf->change);
  jw.raw(",\"cols\":[");
  write_col(jw, cf);
  jw.raw("],\"has_position\":");
  jw.boolean(has_position);
  jw.raw("}");
}

void write_alter_table(Jw &jw, LEX *lex) {
  TABLE_LIST *tl = lex->query_tables;
  if (tl == nullptr) {
    jw.raw("{\"kind\":\"other\"}");
    return;
  }

  jw.raw("{\"kind\":\"alter_table\",\"schema\":");
  write_schema(jw, tl->db);
  jw.raw(",\"table\":");
  jw.str(tl->table_name);
  if ((lex->alter_info.flags & ALTER_RENAME) != 0 &&
      lex->name.str != nullptr) {
    jw.raw(",\"new_schema\":");
    write_schema(jw, lex->first_select_lex()->db);
    jw.raw(",\"new_table\":");
    jw.str(lex->name);
  }
  jw.raw(",\"specs\":[");

  bool first = true;
  auto sep = [&]() {
    if (!first)
      jw.raw(",");
    first = false;
  };

  List_iterator<Create_field> create_it(lex->alter_info.create_list);
  while (Create_field *cf = create_it++) {
    sep();
    write_create_spec(jw, cf);
  }

  List_iterator<Alter_column> alter_it(lex->alter_info.alter_list);
  while (Alter_column *ac = alter_it++) {
    if (!ac->is_rename())
      continue;
    sep();
    jw.raw("{\"op\":\"rename_col\",\"old_name\":");
    jw.str(ac->name);
    jw.raw(",\"new_name\":");
    jw.str(ac->new_name);
    jw.raw("}");
  }

  List_iterator<Alter_drop> drop_it(lex->alter_info.drop_list);
  while (Alter_drop *ad = drop_it++) {
    if (ad->type != Alter_drop::COLUMN)
      continue;
    sep();
    jw.raw("{\"op\":\"drop\",\"old_name\":");
    jw.str(ad->name);
    jw.raw("}");
  }

  jw.raw("]}");
}

void write_rename_table(Jw &jw, LEX *lex) {
  jw.raw("{\"kind\":\"rename_table\",\"pairs\":[");
  bool first = true;
  for (TABLE_LIST *t = lex->first_select_lex()->table_list.first;
       t != nullptr && t->next_local != nullptr; t = t->next_local->next_local) {
    TABLE_LIST *n = t->next_local;
    if (!first)
      jw.raw(",");
    first = false;
    jw.raw("{\"old_schema\":");
    write_schema(jw, t->db);
    jw.raw(",\"old_table\":");
    jw.str(t->table_name);
    jw.raw(",\"new_schema\":");
    write_schema(jw, n->db);
    jw.raw(",\"new_table\":");
    jw.str(n->table_name);
    jw.raw("}");
  }
  jw.raw("]}");
}

} // namespace

std::string digest_stmt(THD *thd) {
  Jw jw;
  LEX *lex = thd->lex;
  if (lex == nullptr) {
    jw.raw("{\"kind\":\"other\"}");
    return jw.s;
  }

  switch (lex->sql_command) {
  case SQLCOM_ALTER_TABLE:
    write_alter_table(jw, lex);
    break;
  case SQLCOM_RENAME_TABLE:
    write_rename_table(jw, lex);
    break;
  default:
    jw.raw("{\"kind\":\"other\"}");
    break;
  }
  return jw.s;
}

} // namespace ddlfuzz_mariadb
