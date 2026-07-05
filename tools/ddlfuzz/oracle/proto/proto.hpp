#pragma once

#include <cerrno>
#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>
#include <vector>

#include <unistd.h>

#if defined(__aarch64__)
#include <arm_neon.h>
#endif

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

// Per-case oracle edge attribution (protocol 2): after each PARSE_BATCH case
// the driver drains its inline-8bit counters into a virgin map and replies
// with how many counter bytes went nonzero for the first time in this
// process. Draining (zeroing) the counters is what makes the next case's scan
// see only its own edges; `virgin` accumulates every fired counter and is
// what GET_COVERAGE serializes. no_sanitize keeps the scan itself out of the
// counter section — instrumenting this loop makes it ~10x slower and would
// re-fire its own edges every case.
//
// The scan is the per-case cost floor (the mysql map is ~1.7 MB), so both the
// all-zero skip and the fired-block path are vectorized on arm64: measured
// ~23 us/scan (~75 GB/s) regardless of how many counters fired, vs ~100 us
// for the byte-at-a-time fired path.
#if defined(__clang__)
__attribute__((no_sanitize("coverage")))
#endif
inline uint32_t
drain_region(uint8_t *cnt, uint8_t *stop, uint8_t *virgin) {
  uint32_t fresh = 0;
  const size_t len = static_cast<size_t>(stop - cnt);
  size_t i = 0;
#if defined(__aarch64__)
  const uint8x16_t zero = vdupq_n_u8(0);
  const uint8x16_t one = vdupq_n_u8(1);
  for (; i + 64 <= len; i += 64) {
    uint8x16_t c0 = vld1q_u8(cnt + i);
    uint8x16_t c1 = vld1q_u8(cnt + i + 16);
    uint8x16_t c2 = vld1q_u8(cnt + i + 32);
    uint8x16_t c3 = vld1q_u8(cnt + i + 48);
    uint8x16_t any = vorrq_u8(vorrq_u8(c0, c1), vorrq_u8(c2, c3));
    if (vmaxvq_u8(any) == 0) continue;
    const uint8x16_t cs[4] = {c0, c1, c2, c3};
    for (int q = 0; q < 4; ++q) {
      const uint8x16_t c = cs[q];
      if (vmaxvq_u8(c) == 0) continue;
      uint8_t *vp = virgin + i + q * 16;
      const uint8x16_t vg = vld1q_u8(vp);
      const uint8x16_t newnz = vandq_u8(vtstq_u8(c, c), vceqq_u8(vg, zero));
      fresh += vaddvq_u8(vandq_u8(newnz, one));
      vst1q_u8(vp, vorrq_u8(vg, c));
      vst1q_u8(cnt + i + q * 16, zero);
    }
  }
#else
  for (; i + 64 <= len; i += 64) {
    uint64_t w[8];
    std::memcpy(w, cnt + i, 64);
    if ((w[0] | w[1] | w[2] | w[3] | w[4] | w[5] | w[6] | w[7]) == 0) continue;
    for (size_t k = i; k < i + 64; ++k) {
      const uint8_t v = cnt[k];
      if (v == 0) continue;
      if (virgin[k] == 0) ++fresh;
      virgin[k] |= v;
      cnt[k] = 0;
    }
  }
#endif
  for (; i < len; ++i) {
    const uint8_t v = cnt[i];
    if (v == 0) continue;
    if (virgin[i] == 0) ++fresh;
    virgin[i] |= v;
    cnt[i] = 0;
  }
  return fresh;
}

}  // namespace ddlproto
