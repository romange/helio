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

  // Translates usec to cpu cycles.
  static uint64_t FromUsec(uint64_t usec) {
    return (usec * frequency_) / 1000'000ULL;
  }

  // Converts cycles to microseconds.
  static uint64_t ToUsec(uint64_t cycles) {
    return (cycles * 1000'000ULL) / frequency_;
  }

 private:
  static uint64_t frequency_;
};

// The following class aggregates cycle measurements over a short period of time.
// If periods change, the timer is reset automatically otherwise it continues to accumulate cycles.
class RealTimeAggreagator {
 public:
  void Add(uint64_t start, uint64_t now);

  // Returns usec measured in the last 1ms period.
  unsigned Usec1ms() const {
    uint64_t usec = CycleClock::ToUsec(cycles_1ms_);
    return usec > 1000u ? 1000u : usec;
  }

  unsigned Usec10ms() const {
    uint64_t usec = CycleClock::ToUsec(cycles_1ms_);
    return usec > 10000u ? 10000u : usec;
  }

 private:
  uint32_t measurement_start_1ms_ = 0;
  uint32_t measurement_start_10ms_ = 0;

  uint32_t cycles_1ms_ = 0, cycles_10ms_ = 0;
};

class CpuTimeGuard {
 public:
  explicit CpuTimeGuard(RealTimeAggreagator* val) : val_(val) {
    start_ = CycleClock::Now();
  }

  ~CpuTimeGuard() {
    uint64_t now = CycleClock::Now();
    val_->Add(start_, now);
  }

 private:
  RealTimeAggreagator* val_;
  uint64_t start_;
};

}  // namespace base
