// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>

namespace util {
namespace fb2 {

namespace detail {

inline void CpuPause() {
#if defined(__i386__) || defined(__amd64__)
  __asm__ __volatile__("pause");
#elif defined(__aarch64__)
  /* Use an isb here as we've found it's much closer in duration to
   * the x86 pause instruction vs. yield which is a nop and thus the
   * loop count is lower and the interconnect gets a lot more traffic
   * from loading the ticket above. */
  __asm__ __volatile__("isb");
#endif
}

class CycleClock {
 public:
  static uint64_t Now() {
#if defined(__x86_64__)
    uint64_t low, high;
    __asm__ volatile("rdtsc" : "=a"(low), "=d"(high));
    return (high << 32) | low;
#elif defined(__aarch64__)
    uint64_t val;
    __asm__ volatile("mrs %0, cntvct_el0" : "=r"(val));
    return val;
#else
#error "Unsupported architecture"
#endif
  }

  // number of cycles per millisecond.
  static uint64_t FrequencyUsec();
};

}  // namespace detail
}  // namespace fb2
}  // namespace util