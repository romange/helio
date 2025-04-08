// Copyright 2025, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

#if !defined(__x86_64__) && !defined(__aarch64__)
#include <absl/base/internal/cycleclock.h>
#endif

namespace base {

// CycleClock is a class that provides a high-resolution cycle counter based on TSC.
// It is used to measure very short time intervals.

class CycleClock {
 public:
  // Called once to initialize the frequency of the cycle counter.
  static void InitOnce();

  // Returns the current value of the cycle counter.
  static uint64_t Now() {
#if defined(__x86_64__)
    unsigned long low, high;
    __asm__ volatile("rdtsc" : "=a"(low), "=d"(high));
    return ((static_cast<uint64_t>(high) << 32) + low);
#elif defined(__aarch64__)
    int64_t tv;
    asm volatile("mrs %0, cntvct_el0" : "=r"(tv));
    return tv;
#else
    return absl::base_internal::CycleClock::Now();
#endif
  }

  // Returns the frequency of the cycle counter in Hz.
  static uint64_t Frequency() {
    return frequency_;
  }

 private:
  static uint64_t frequency_;
};

}  // namespace base
