// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/http/encoding.h"

#include <algorithm>
#include <array>
#include <cctype>

namespace {

inline bool IsValidUrlChar(char ch) {
  static constexpr std::array kvalid{'-', '_', '.', '!', '\'', '(', ')'};
  return std::isalnum(ch) || std::find(kvalid.begin(), kvalid.end(), ch) != kvalid.end();
}

}  // namespace

namespace util::http {

std::string UrlEncode(std::string_view src) {
  static const char digits[] = "0123456789ABCDEF";

  std::string out;
  out.reserve(src.size() * 2 + 1);
  for (char ch_c : src) {
    unsigned char ch = static_cast<unsigned char>(ch_c);
    if (IsValidUrlChar(ch)) {
      out.push_back(ch_c);
    } else {
      out.push_back('%');
      out.push_back(digits[(ch >> 4) & 0x0F]);
      out.push_back(digits[ch & 0x0F]);
    }
  }

  return out;
}

}  // namespace util::http
