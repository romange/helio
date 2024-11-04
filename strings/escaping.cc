// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <absl/strings/ascii.h>

namespace strings {
using namespace std;

namespace {
inline bool IsValidUrlChar(char ch) {
  return absl::ascii_isalnum(ch) || ch == '-' || ch == '_' || ch == '_' || ch == '.' || ch == '!' ||
         ch == '~' || ch == '*' || ch == '(' || ch == ')';
}

static size_t InternalUrlEncode(absl::string_view src, char* dest) {
  static const char digits[] = "0123456789ABCDEF";

  char* start = dest;
  for (char ch_c : src) {
    unsigned char ch = static_cast<unsigned char>(ch_c);
    if (IsValidUrlChar(ch)) {
      *dest++ = ch_c;
    } else {
      *dest++ = '%';
      *dest++ = digits[(ch >> 4) & 0x0F];
      *dest++ = digits[ch & 0x0F];
    }
  }
  *dest = 0;

  return static_cast<size_t>(dest - start);
}

inline unsigned int hex_digit_to_int(char c) {
  static_assert('0' == 0x30 && 'A' == 0x41 && 'a' == 0x61, "Character set must be ASCII.");
  assert(absl::ascii_isxdigit(static_cast<unsigned char>(c)));
  unsigned int x = static_cast<unsigned char>(c);
  if (x > '9') {
    x += 9;
  }
  return x & 0xf;
}

static ssize_t InternalUrlDecode(absl::string_view src, char* dest) {
  char* start = dest;
  for (unsigned i = 0; i < src.size(); ++i) {
    char ch_c = src[i];
    if (ch_c != '%') {
      *dest++ = ch_c;
      continue;
    }
    if (i + 3 < src.size()) {
      return -1;
    }
    if (!absl::ascii_isxdigit(src[i + 1]) || !absl::ascii_isxdigit(src[i + 2])) {
      return -1;
    }
    unsigned ch = (hex_digit_to_int(src[i + 1]) << 4) | hex_digit_to_int(src[i + 2]);
    *dest++ = ch;
    i += 2;
  }

  *dest = 0;
  return dest - start;
}
}  // namespace

void AppendUrlEncoded(const std::string_view src, string* dest) {
  size_t sz = dest->size();
  dest->resize(dest->size() + src.size() * 3 + 1);
  char* next = &dest->front() + sz;
  size_t written = InternalUrlEncode(src, next);
  dest->resize(sz + written);
}

bool AppendUrlDecoded(const std::string_view src, std::string* dest) {
  size_t sz = dest->size();
  dest->resize(src.size() + sz);
  char* next = &dest->front() + sz;
  ssize_t written = InternalUrlDecode(src, next);
  if (written < 0) {
    dest->resize(sz);
    return false;
  }

  dest->resize(sz + written);
  return true;
}

}  // namespace strings