// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <absl/numeric/bits.h>

class [[deprecated]] Bits {
 public:
  static int CountOnes(uint32_t n) {
    return __builtin_popcount(n);
  }

  static inline constexpr int CountOnes64(uint64_t n) {
    return __builtin_popcountll(n);
  }


  static uint32_t RoundUp(uint32_t x) {
    return absl::bit_ceil(x);
  }

  static uint64_t RoundUp64(uint64_t x) {
    return absl::bit_ceil(x);
  }

 private:
  Bits(const Bits&) = delete;
  void operator=(const Bits&) = delete;
};
