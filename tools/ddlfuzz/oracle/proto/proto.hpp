#pragma once

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include <unistd.h>

namespace ddlproto {

enum MsgType : uint8_t { PARSE_BATCH = 1, GET_COVERAGE = 2, HELLO = 3 };

inline bool read_full(int fd, void *buf, size_t n) {
  auto *p = static_cast<uint8_t *>(buf);
  while (n != 0) {
    const ssize_t r = ::read(fd, p, n);
    if (r < 0 && errno == EINTR) continue;
    if (r <= 0) return false;
    p += static_cast<size_t>(r);
    n -= static_cast<size_t>(r);
  }
  return true;
}

inline bool write_full(int fd, const void *buf, size_t n) {
  const auto *p = static_cast<const uint8_t *>(buf);
  while (n != 0) {
    const ssize_t w = ::write(fd, p, n);
    if (w < 0 && errno == EINTR) continue;
    if (w <= 0) return false;
    p += static_cast<size_t>(w);
    n -= static_cast<size_t>(w);
  }
  return true;
}

struct Reader {
  const uint8_t *p;
  const uint8_t *end;

  explicit Reader(const std::vector<uint8_t> &v)
      : p(v.data()), end(v.data() + v.size()) {}

  bool u32(uint32_t &o) {
    if (end - p < 4) return false;
    std::memcpy(&o, p, 4);
    p += 4;
    return true;
  }

  bool u64(uint64_t &o) {
    if (end - p < 8) return false;
    std::memcpy(&o, p, 8);
    p += 8;
    return true;
  }

  bool bytes(const uint8_t *&o, uint32_t n) {
    if (static_cast<size_t>(end - p) < n) return false;
    o = p;
    p += n;
    return true;
  }

  bool eof() const { return p == end; }
};

inline void put_u32(std::string &s, uint32_t v) {
  s.append(reinterpret_cast<const char *>(&v), 4);
}

inline bool read_frame(int fd, std::vector<uint8_t> &body) {
  uint32_t len = 0;
  if (!read_full(fd, &len, 4)) return false;
  body.resize(len);
  return len == 0 || read_full(fd, body.data(), len);
}

inline bool write_frame(int fd, const std::string &body) {
  const uint32_t len = static_cast<uint32_t>(body.size());
  return write_full(fd, &len, 4) && write_full(fd, body.data(), body.size());
}

inline void json_escape(std::string &out, const char *s, size_t n) {
  static constexpr char hex[] = "0123456789abcdef";
  out.push_back('"');
  for (size_t i = 0; i < n; ++i) {
    const auto c = static_cast<unsigned char>(s[i]);
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
  out.push_back('"');
}

inline void json_escape(std::string &out, const std::string &s) {
  json_escape(out, s.data(), s.size());
}

}  // namespace ddlproto
