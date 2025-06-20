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

#ifndef HUMAN_READABLE_NUMBERS_H_
#define HUMAN_READABLE_NUMBERS_H_

#include <cstdint>
#include <string_view>

namespace strings {

// Converts from an int64 to a human readable string representing the
// same number, using decimal powers.  e.g. 1200000 -> "1.20M".
std::string HumanReadableNum(int64_t value);

// Converts from an int64 representing a number of bytes to a
// human readable string representing the same number.
// e.g. 12345678 -> "11.77MiB".
std::string HumanReadableNumBytes(int64_t num_bytes);

// Converts a time interval as double to a human readable
// string. For example:
//   0.001       -> "1 ms"
//   10.0        -> "10 s"
//   933120.0    -> "10.8 days"
//   39420000.0  -> "1.25 years"
//   -10         -> "-10 s"
std::string HumanReadableElapsedTime(double seconds);

// Parses a human readable string representing a number of bytes into an int64_t.
// The string can contain a number followed by a unit suffix, such as "1K", "2M",
// "3G", "4T", "5P", "6E", or "7B". The suffixes are case-insensitive.
bool ParseHumanReadableBytes(std::string_view str, int64_t* num_bytes);

struct MemoryBytesFlag {
  size_t value = 0;

  MemoryBytesFlag(size_t s = 0) : value(s) {  // NOLINT
  }

  operator size_t() const {  // NOLINT
    return value;
  }
};

bool AbslParseFlag(std::string_view in, MemoryBytesFlag* flag, std::string* err);
std::string AbslUnparseFlag(const MemoryBytesFlag& flag);

}  // namespace strings

#endif  // HUMAN_READABLE_NUMBERS_H_
