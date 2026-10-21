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

  // Translates usec to cpu cycles. Saturates when the result does not fit into 64 bits.
  static uint64_t FromUsec(uint64_t usec) {
    unsigned __int128 cycles = (static_cast<unsigned __int128>(usec) * usec_to_cycles_) >> 32;
    return cycles > UINT64_MAX ? UINT64_MAX : cycles;
  }

  // Converts cycles to microseconds with a fixed-point multiply: no intermediate overflow for
  // any 64-bit input, never below the exact value and at most 1 + cycles / 2^60 usec above it.
  static uint64_t ToUsec(uint64_t cycles) {
    return (static_cast<unsigned __int128>(cycles) * cycles_to_usec_) >> 60;
  }

 private:
  static uint64_t frequency_;
  static uint64_t cycles_to_usec_;  // ceil(1e6 * 2^60 / frequency_)
  static uint64_t usec_to_cycles_;  // ceil(frequency_ * 2^32 / 1e6)
};

// The following class aggregates cycle measurements over a short period of time.
// If periods change, the timer is reset automatically otherwise it continues to accumulate cycles.
class CycleBucketAggregator {
 public:
  void Add(uint64_t start, uint64_t now);

  // Returns usec accumulated in the current 1ms period.
  unsigned Usec1ms() const {
    return Usec1ms(CycleClock::Now());
  }

  // Returns usec accumulated in the 1ms period containing now.
  unsigned Usec1ms(uint64_t now) const {
    if (measurement_start_1ms_ != Milliseconds(now)) {
      return 0;
    }

    uint64_t usec = CycleClock::ToUsec(cycles_1ms_);
    return usec > 1000u ? 1000u : usec;
  }

  // Returns usec accumulated in the current 10ms period.
  unsigned Usec10ms() const {
    return Usec10ms(CycleClock::Now());
  }

  // Returns usec accumulated in the 10ms period containing now.
  unsigned Usec10ms(uint64_t now) const {
    const uint64_t current_bucket_start = (Milliseconds(now) / 10) * 10;
    if (measurement_start_10ms_ != current_bucket_start) {
      return 0;
    }

    uint64_t usec = CycleClock::ToUsec(cycles_10ms_);
    return usec > 10000u ? 10000u : usec;
  }

  // Returns true if any of the measurements in the current period crossed the ratio threshold.
  bool IsOverloaded(float cpu_ratio) const {
    const uint64_t now = CycleClock::Now();
    return Usec1ms(now) >= 1000u * cpu_ratio || Usec10ms(now) >= 10000u * cpu_ratio;
  }

 private:
  static uint64_t Milliseconds(uint64_t cycles) {
    return CycleClock::ToUsec(cycles) / 1000;
  }

  uint64_t measurement_start_1ms_ = 0;
  uint64_t measurement_start_10ms_ = 0;

  uint32_t cycles_1ms_ = 0, cycles_10ms_ = 0;
};

using RealTimeAggregator = CycleBucketAggregator;

class ScopedCycleTimer {
 public:
  explicit ScopedCycleTimer(CycleBucketAggregator* val) : val_(val) {
    start_ = CycleClock::Now();
  }

  ScopedCycleTimer(const ScopedCycleTimer&) = delete;
  ScopedCycleTimer& operator=(const ScopedCycleTimer&) = delete;
  ScopedCycleTimer(ScopedCycleTimer&&) = delete;
  ScopedCycleTimer& operator=(ScopedCycleTimer&&) = delete;

  ~ScopedCycleTimer() {
    uint64_t now = CycleClock::Now();
    val_->Add(start_, now);
  }

 private:
  CycleBucketAggregator* val_;
  uint64_t start_;
};

using CpuTimeGuard = ScopedCycleTimer;

}  // namespace base
