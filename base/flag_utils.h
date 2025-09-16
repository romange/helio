// Copyright 2025, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <absl/strings/str_cat.h>

#include <limits>
#include <string>
#include <vector>

#include "base/flags.h"

namespace base {

// Numeric flag that is valid only in the range [L, R]
template <typename T, T L = std::numeric_limits<T>::min(), T R = std::numeric_limits<T>::max()>
struct ConstrainedNumericFlagValue {
  ConstrainedNumericFlagValue(T&& v) : value(v) {
  }
  operator T() const {
    return value;
  }
  T value;
};

template <typename T, T L, T R>
std::string AbslUnparseFlag(ConstrainedNumericFlagValue<T, L, R> v) {
  return absl::UnparseFlag(v.value);
}

template <typename T, T L, T R>
bool AbslParseFlag(absl::string_view text, ConstrainedNumericFlagValue<T, L, R>* cnf,
                   std::string* error) {
  if (!absl::ParseFlag(text, &cnf->value, error))
    return false;

  if (cnf->value < L || cnf->value > R) {
    *error = absl::StrCat("Expected value between ", L, " and ", R);
    return false;
  }

  return true;
}

// Get flag names as strings from list of references
template <typename... Ts> std::vector<std::string> GetFlagNames(const absl::Flag<Ts>&... flags) {
  return {std::string{absl::GetFlagReflectionHandle(flags).Name()}...};
}
}  // namespace base
