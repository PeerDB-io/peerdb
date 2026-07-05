#include "mariadb.h"
#include "sql_class.h"
#include "sql_lex.h"
#include "sql_parse.h"
#include "sql_alter.h"
#include "mysqld.h"
#include "field.h"
#include "table.h"

#include <mysql_version.h>

#include "digest.hpp"

#include "../proto/proto.hpp"

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <limits>
#include <stdexcept>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#ifndef DDLFUZZ_BUILD_DIR
#define DDLFUZZ_BUILD_DIR "/Users/ilia/Code/peerdb/tools/ddlfuzz/build"
#endif

extern "C" int mysql_server_init(int argc, char **argv, char **groups);
extern "C" void *create_embedded_thd(unsigned long client_flag);

namespace {

using ddlfuzz_mariadb::digest_stmt;
using ddlfuzz_mariadb::kSentinelDb;

constexpr uint8_t kMsgParseBatch = 1;
constexpr uint8_t kMsgGetCoverage = 2;
constexpr uint8_t kMsgHello = 3;
constexpr unsigned long kClientMultiStatements = 1UL << 16;

THD *g_thd = nullptr;
std::vector<std::pair<uint8_t *, uint8_t *>> g_regions;
// Virgin twin of each counter region (see ddlproto::drain_region). Allocated
// lazily so it works no matter which message arrives first.
std::vector<std::vector<uint8_t>> g_virgin_regions;

uint32_t drain_new_edges() {
  if (g_virgin_regions.size() != g_regions.size()) {
    g_virgin_regions.resize(g_regions.size());
    for (size_t i = 0; i < g_regions.size(); i++) {
      g_virgin_regions[i].assign(
          static_cast<size_t>(g_regions[i].second - g_regions[i].first), 0);
    }
  }
  uint32_t fresh = 0;
  for (size_t i = 0; i < g_regions.size(); i++) {
    fresh += ddlproto::drain_region(g_regions[i].first, g_regions[i].second,
                                    g_virgin_regions[i].data());
  }
  return fresh;
}

[[noreturn]] void fatal(const std::string &msg) {
  std::fprintf(stderr, "oracle-mariadb: %s\n", msg.c_str());
  std::fflush(stderr);
  std::exit(2);
}

void append_u32(std::string &out, uint32_t v) {
  for (int i = 0; i < 4; i++)
    out.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}

void append_u64(std::string &out, uint64_t v) {
  for (int i = 0; i < 8; i++)
    out.push_back(static_cast<char>((v >> (8 * i)) & 0xff));
}

struct Reader {
  const std::string &b;
  size_t off = 0;

  explicit Reader(const std::string &body) : b(body) {}

  void require(size_t n) {
    if (n > b.size() - off)
      fatal("malformed frame payload");
  }

  uint8_t u8() {
    require(1);
    return static_cast<uint8_t>(b[off++]);
  }

  uint32_t u32() {
    require(4);
    uint32_t v = 0;
    for (int i = 0; i < 4; i++)
      v |= static_cast<uint32_t>(static_cast<uint8_t>(b[off++])) << (8 * i);
    return v;
  }

  uint64_t u64() {
    require(8);
    uint64_t v = 0;
    for (int i = 0; i < 8; i++)
      v |= static_cast<uint64_t>(static_cast<uint8_t>(b[off++])) << (8 * i);
    return v;
  }

  std::string bytes(size_t n) {
    require(n);
    std::string v = b.substr(off, n);
    off += n;
    return v;
  }

  bool done() const { return off == b.size(); }
};

bool read_exact(void *dst, size_t n) {
  char *p = static_cast<char *>(dst);
  size_t got = 0;
  while (got < n) {
    size_t r = std::fread(p + got, 1, n - got, stdin);
    if (r == 0) {
      if (std::feof(stdin))
        return false;
      fatal(std::string("stdin read failed: ") + std::strerror(errno));
    }
    got += r;
  }
  return true;
}

bool read_frame(std::string &body) {
  uint8_t len_bytes[4];
  if (!read_exact(len_bytes, sizeof(len_bytes))) {
    if (std::feof(stdin))
      return false;
    fatal("partial frame length");
  }
  uint32_t len = static_cast<uint32_t>(len_bytes[0]) |
                 (static_cast<uint32_t>(len_bytes[1]) << 8) |
                 (static_cast<uint32_t>(len_bytes[2]) << 16) |
                 (static_cast<uint32_t>(len_bytes[3]) << 24);
  body.assign(len, '\0');
  if (len != 0 && !read_exact(body.data(), len))
    fatal("partial frame body");
  return true;
}

void write_frame(const std::string &body) {
  if (body.size() > std::numeric_limits<uint32_t>::max())
    fatal("response too large");
  std::string prefix;
  append_u32(prefix, static_cast<uint32_t>(body.size()));
  if (std::fwrite(prefix.data(), 1, prefix.size(), stdout) != prefix.size() ||
      (!body.empty() &&
       std::fwrite(body.data(), 1, body.size(), stdout) != body.size()) ||
      std::fflush(stdout) != 0) {
    std::exit(0);
  }
}

std::string json_string(const std::string &v) {
  std::string out = "\"";
  proto::json_escape(out, v.data(), v.size());
  out.push_back('"');
  return out;
}

std::string reject_json(const std::string &err) {
  return std::string("{\"verdict\":\"reject\",\"error\":") + json_string(err) +
         "}";
}

std::string error_string(const char *fallback) {
  uint err = 0;
  const char *msg = fallback;
  if (g_thd != nullptr && g_thd->get_stmt_da() != nullptr) {
    err = g_thd->get_stmt_da()->sql_errno();
    const char *da_msg = g_thd->get_stmt_da()->message();
    if (da_msg != nullptr && da_msg[0] != '\0')
      msg = da_msg;
  }
  return std::to_string(err) + ": " + (msg != nullptr ? msg : "unknown error");
}

void cleanup_statement() {
  g_thd->end_statement();
  g_thd->Item_change_list::rollback_item_tree_changes();
  g_thd->cleanup_after_query();
  if (g_thd->lex != nullptr) {
    g_thd->lex->restore_set_statement_var();
    g_thd->lex->m_sql_cmd = nullptr;
  }
  free_root(g_thd->mem_root, MYF(MY_KEEP_PREALLOC));
}

std::string run_case(const std::string &stmt, uint64_t sql_mode) {
  std::vector<char> buf(stmt.begin(), stmt.end());
  buf.push_back('\0');

  char *cur = buf.data();
  char *end = buf.data() + stmt.size();
  std::vector<std::string> stmts;

  for (;;) {
    size_t len = static_cast<size_t>(end - cur);
    g_thd->variables.sql_mode = static_cast<sql_mode_t>(sql_mode);
    lex_start(g_thd);
    g_thd->reset_for_next_command();
    g_thd->set_query(cur, len);

    Parser_state ps;
    if (ps.init(g_thd, cur, len)) {
      std::string err = error_string("parser state initialization failed");
      cleanup_statement();
      return reject_json(err);
    }

    bool err = parse_sql(g_thd, &ps, nullptr);
    if (err) {
      std::string out = reject_json(error_string("parse failed"));
      cleanup_statement();
      return out;
    }

    stmts.push_back(digest_stmt(g_thd));
    const char *fsemi = ps.m_lip.found_semicolon;
    cleanup_statement();

    if (fsemi == nullptr)
      break;
    if (fsemi < buf.data() || fsemi > end)
      fatal("parser returned invalid semicolon pointer");

    cur = const_cast<char *>(fsemi);
    while (cur < end &&
           my_isspace(g_thd->charset(), static_cast<unsigned char>(*cur))) {
      cur++;
    }
  }

  std::string out = "{\"verdict\":\"accept\",\"stmts\":[";
  for (size_t i = 0; i < stmts.size(); i++) {
    if (i != 0)
      out.push_back(',');
    out += stmts[i];
  }
  out += "]}";
  return out;
}

std::string default_scratch(const char *argv0) {
  std::filesystem::path exe;
  try {
    exe = std::filesystem::canonical(argv0);
  } catch (...) {
    exe = std::filesystem::absolute(argv0);
  }
  std::filesystem::path dir =
      exe.parent_path() / "oracle-scratch" /
      ("mariadb." + std::to_string(static_cast<long long>(getpid())));
  std::filesystem::create_directories(dir);
  return dir.string();
}

void init_server(const std::string &scratch) {
  std::filesystem::create_directories(scratch);

  std::string datadir_arg = "--datadir=" + scratch;
  std::string lcdir_arg =
      std::string("--lc-messages-dir=") + DDLFUZZ_BUILD_DIR +
      "/mariadb-build/sql/share";
  std::vector<std::string> args = {
      "oracle-mariadb",
      datadir_arg,
      lcdir_arg,
      "--character-set-server=utf8mb4",
      "--skip-networking",
  };
  std::vector<char *> argv;
  argv.reserve(args.size() + 1);
  for (std::string &arg : args)
    argv.push_back(arg.data());
  argv.push_back(nullptr);

  char *groups[] = {const_cast<char *>("server"), const_cast<char *>("embedded"),
                    nullptr};
  if (mysql_server_init(static_cast<int>(args.size()), argv.data(), groups))
    fatal("server init failed");

  lower_case_table_names = 0;
  table_alias_charset = &my_charset_bin;

  g_thd = static_cast<THD *>(create_embedded_thd(kClientMultiStatements));
  if (g_thd == nullptr)
    fatal("create_embedded_thd failed");

  g_thd->store_globals();
  LEX_CSTRING db = {kSentinelDb, std::strlen(kSentinelDb)};
  if (g_thd->set_db(&db))
    fatal("failed to set sentinel db");

  g_thd->update_charset(&my_charset_utf8mb4_general_ci,
                        &my_charset_utf8mb4_general_ci,
                        &my_charset_utf8mb4_general_ci);
}

std::string server_version_json() {
  std::string version = MYSQL_SERVER_VERSION;
  size_t dash = version.find('-');
  if (dash != std::string::npos)
    version.resize(dash);
  return std::string("{\"engine\":\"mariadb\",\"server_version\":\"") +
         version + "\",\"protocol\":2}";
}

void handle_hello(Reader &r) {
  if (!r.done())
    fatal("HELLO request payload must be empty");
  std::string json = server_version_json();
  std::string out;
  out.push_back(static_cast<char>(kMsgHello));
  append_u32(out, static_cast<uint32_t>(json.size()));
  out += json;
  write_frame(out);
}

void handle_coverage(Reader &r) {
  if (!r.done())
    fatal("GET_COVERAGE request payload must be empty");
  // Fold any counters fired since the last case (startup, frame handling)
  // into the virgin map, which is now the cumulative coverage snapshot —
  // per-case draining leaves the live counters ~empty between cases.
  drain_new_edges();
  size_t total = 0;
  for (const auto &virgin : g_virgin_regions)
    total += virgin.size();
  if (total > std::numeric_limits<uint32_t>::max())
    fatal("coverage bitmap too large");

  std::string out;
  out.reserve(1 + 4 + total);
  out.push_back(static_cast<char>(kMsgGetCoverage));
  append_u32(out, static_cast<uint32_t>(total));
  for (const auto &virgin : g_virgin_regions)
    out.append(reinterpret_cast<const char *>(virgin.data()), virgin.size());
  write_frame(out);
}

void handle_parse_batch(Reader &r) {
  uint32_t count = r.u32();
  std::vector<std::string> digests;
  std::vector<uint32_t> new_edges;
  digests.reserve(count);
  new_edges.reserve(count);
  for (uint32_t i = 0; i < count; i++) {
    uint64_t mode = r.u64();
    uint32_t len = r.u32();
    std::string stmt = r.bytes(len);
    digests.push_back(run_case(stmt, mode));
    new_edges.push_back(drain_new_edges());
  }
  if (!r.done())
    fatal("trailing bytes in PARSE_BATCH request");

  std::string out;
  out.push_back(static_cast<char>(kMsgParseBatch));
  append_u32(out, count);
  for (uint32_t i = 0; i < count; i++) {
    append_u32(out, new_edges[i]);
    append_u32(out, static_cast<uint32_t>(digests[i].size()));
    out += digests[i];
  }
  write_frame(out);
}

} // namespace

extern "C" void __sanitizer_cov_8bit_counters_init(uint8_t *start,
                                                    uint8_t *stop) {
  if (start == stop)
    return;
  // On Mach-O every instrumented TU's module ctor passes the same
  // section$start/section$end boundary pair for the merged __sancov_cntrs
  // section (645 identical calls observed); dedup so GET_COVERAGE ships the
  // bitmap once instead of 244 MB of copies.
  for (const auto &region : g_regions)
    if (region.first == start && region.second == stop)
      return;
  g_regions.emplace_back(start, stop);
}

int main(int argc, char **argv) {
  try {
    std::string scratch =
        argc > 1 ? std::string(argv[1]) : default_scratch(argv[0]);
    init_server(scratch);

    std::string body;
    while (read_frame(body)) {
      if (body.empty())
        fatal("empty frame body");
      Reader r(body);
      uint8_t msg = r.u8();
      switch (msg) {
      case kMsgHello:
        handle_hello(r);
        break;
      case kMsgGetCoverage:
        handle_coverage(r);
        break;
      case kMsgParseBatch:
        handle_parse_batch(r);
        break;
      default:
        fatal("unknown message type");
      }
    }
    return 0;
  } catch (const std::exception &e) {
    fatal(e.what());
  }
}
