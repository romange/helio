// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include <absl/strings/ascii.h>

namespace strings {
using namespace std;

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

void AppendUrlEncoded(const std::string_view src, string* dest) {
  size_t sz = dest->size();
  dest->resize(dest->size() + src.size() * 3 + 1);
  char* next = &dest->front() + sz;
  size_t written = InternalUrlEncode(src, next);
  dest->resize(sz + written);
}

}  // namespace strings