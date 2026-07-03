#pragma once

#include <cstddef>
#include <string>

class THD;

namespace proto {

inline void json_escape(std::string &out, const char *p, size_t n) {
  static const char hex[] = "0123456789abcdef";
  for (size_t i = 0; i < n; i++) {
    unsigned char c = static_cast<unsigned char>(p[i]);
    switch (c) {
    case '"':
      out += "\\\"";
      break;
    case '\\':
      out += "\\\\";
      break;
    case '\b':
      out += "\\b";
      break;
    case '\f':
      out += "\\f";
      break;
    case '\n':
      out += "\\n";
      break;
    case '\r':
      out += "\\r";
      break;
    case '\t':
      out += "\\t";
      break;
    default:
      if (c < 0x20) {
        out += "\\u00";
        out.push_back(hex[c >> 4]);
        out.push_back(hex[c & 0x0f]);
      } else {
        out.push_back(static_cast<char>(c));
      }
      break;
    }
  }
}

} // namespace proto

namespace ddlfuzz_mariadb {

inline constexpr const char *kSentinelDb = "peerdb_ddlfuzz_nodb";

std::string digest_stmt(THD *thd);

} // namespace ddlfuzz_mariadb
