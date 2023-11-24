// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/detail/utils.h"

#include <time.h>

#include "base/logging.h"

namespace util {
namespace fb2 {

namespace detail {
namespace {

#if defined(__x86_64__)

inline uint64_t clock_nanos() {
  struct timespec ts;
  clock_gettime(CLOCK_MONOTONIC, &ts);
  return (uint64_t)ts.tv_sec * 1000000000 + ts.tv_nsec;
}

constexpr uint64_t kDelayNs = 1000000; // 1ms

// Returns the number of cycles per millisecond.
uint64_t do_sample() {
  asm volatile ("" : : : "memory");   // compiler fence to prevent reordering
  uint64_t start = clock_nanos();
  uint64_t tscbefore = CycleClock::Now();
  uint64_t now;
  do {
    now = clock_nanos();
  } while (now < start + kDelayNs);

  uint64_t tscafter = CycleClock::Now();
  return (tscafter - tscbefore) * 1000000u / (now - start);
}

// Returns the number of cycles per microsecond.
static uint64_t tsc_from_cal() {
  constexpr unsigned kSamples = 74;
  uint64_t samples[kSamples];

  for (size_t s = 0; s < kSamples; s++) {
    samples[s] = do_sample();
  }

  uint64_t sum = 0;
  // discard the first half of the samples.
  for (unsigned s = kSamples / 2; s < kSamples; s++) {
    sum += samples[s];
  }

  uint64_t avg = sum / (kSamples / 2);
  // round to the nearest multiple of 1000.
  return (avg + 500) / 1000;
}
#endif
}  // namespace

uint64_t CycleClock::FrequencyUsec() {
  uint64_t res;
#if defined(__x86_64__)
  res = tsc_from_cal();
#elif defined(__aarch64__)
  __asm__ volatile("mrs %0, cntfrq_el0" : "=r"(res));
  res /= 1000000;
#endif
  return res;
}

}  // namespace detail
}  // namespace fb2
}  // namespace util