/* Copyright 2015 The TensorFlow Authors. All Rights Reserved.
Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
==============================================================================*/

#include "strings/human_readable.h"

#include <absl/strings/str_format.h>

#include "base/integral_types.h"
#include "base/logging.h"

namespace strings {
using absl::StrAppendFormat;

std::string HumanReadableNum(int64 value) {
  std::string s;
  if (value < 0) {
    s += "-";
    value = -value;
  }
  if (value < 1000) {
    StrAppendFormat(&s, "%lld", static_cast<long long>(value));
  } else if (value >= static_cast<int64>(1e15)) {
    // Number bigger than 1E15; use that notation.
    StrAppendFormat(&s, "%0.3G", static_cast<double>(value));
  } else {
    static const char units[] = "kMBT";

    const char* unit = units;
    while (value >= static_cast<int64>(1000000)) {
      value /= static_cast<int64>(1000);
      ++unit;
      CHECK(unit < units + ABSL_ARRAYSIZE(units));
    }
    StrAppendFormat(&s, "%.2f%c", value / 1000.0, *unit);
  }
  return s;
}

std::string HumanReadableNumBytes(int64_t num_bytes) {
  if (num_bytes == kint64min) {
    // Special case for number with not representable negation.
    return "-8E";
  }

  const char* neg_str = (num_bytes < 0) ? "-" : "";
  if (num_bytes < 0) {
    num_bytes = -num_bytes;
  }

  // Special case for bytes.
  if (num_bytes < 1024) {
    // No fractions for bytes.
    char buf[8];  // Longest possible string is '-XXXXB'
    snprintf(buf, sizeof(buf), "%s%lldB", neg_str, static_cast<long long>(num_bytes));
    return std::string(buf);
  }

  static const char units[] = "KMGTPE";  // int64 only goes up to E.
  const char* unit = units;
  while (num_bytes >= static_cast<int64>(1024) * 1024) {
    num_bytes /= 1024;
    ++unit;
    CHECK(unit < units + ABSL_ARRAYSIZE(units));
  }

  // We use SI prefixes.
  char buf[16];
  snprintf(buf, sizeof(buf), ((*unit == 'K') ? "%s%.1f%ciB" : "%s%.2f%ciB"), neg_str,
           num_bytes / 1024.0, *unit);
  return std::string(buf);
}

std::string HumanReadableElapsedTime(double seconds) {
  std::string human_readable;

  if (seconds < 0) {
    human_readable = "-";
    seconds = -seconds;
  }

  // Start with us and keep going up to years.
  // The comparisons must account for rounding to prevent the format breaking
  // the tested condition and returning, e.g., "1e+03 us" instead of "1 ms".
  const double microseconds = seconds * 1.0e6;
  if (microseconds < 999.5) {
    StrAppendFormat(&human_readable, "%0.3g us", microseconds);
    return human_readable;
  }
  double milliseconds = seconds * 1e3;
  if (milliseconds >= .995 && milliseconds < 1) {
    // Round half to even in Appendf would convert this to 0.999 ms.
    milliseconds = 1.0;
  }
  if (milliseconds < 999.5) {
    StrAppendFormat(&human_readable, "%0.3g ms", milliseconds);
    return human_readable;
  }
  if (seconds < 60.0) {
    StrAppendFormat(&human_readable, "%0.3g s", seconds);
    return human_readable;
  }
  seconds /= 60.0;
  if (seconds < 60.0) {
    StrAppendFormat(&human_readable, "%0.3g min", seconds);
    return human_readable;
  }
  seconds /= 60.0;
  if (seconds < 24.0) {
    StrAppendFormat(&human_readable, "%0.3g h", seconds);
    return human_readable;
  }
  seconds /= 24.0;
  if (seconds < 30.0) {
    StrAppendFormat(&human_readable, "%0.3g days", seconds);
    return human_readable;
  }
  if (seconds < 365.2425) {
    StrAppendFormat(&human_readable, "%0.3g months", seconds / 30.436875);
    return human_readable;
  }
  seconds /= 365.2425;
  StrAppendFormat(&human_readable, "%0.3g years", seconds);
  return human_readable;
}

}  // namespace strings
