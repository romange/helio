// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <array>
#include <cstdint>
#include <ctime>
#include <memory>
#include <utility>

#include "util/proactor_pool.h"

namespace util {

// Default clock policy: returns the current slot index at one-second granularity.
// A "slot" is the ring-buffer bucket unit, so with this clock a SlidingCounter<NUM>
// covers a NUM-second window with per-second buckets (the historical behavior).
struct SecondsClock {
  uint64_t operator()() const {
    return static_cast<uint64_t>(time(nullptr));
  }
};

namespace detail {

class SlidingCounterBase {
 protected:
  void InitInternal(ProactorPool* pp);
  void CheckInit() const;
  unsigned ProactorThreadIndex() const;

  ProactorPool* pp_ = nullptr;
};

}  // namespace detail

/**
 * @brief Sliding window data structure that can aggregate moving statistics.
 *        It's implemented using a ring-buffer with size specified at compile time.
 *
 * The Clock policy maps wall time to a monotonically increasing slot index; a new slot
 * begins each time the clock's returned value increments. The window spans the last NUM
 * slots. A running total is cached so Sum()/SumTail() are O(1) and never overflow T.
 *
 * @tparam NUM   number of ring-buffer buckets (window length in slots).
 * @tparam T     counter value type.
 * @tparam Clock policy with `uint64_t operator()() const` returning the current slot index.
 */
template <unsigned NUM, typename T = int32_t, typename Clock = SecondsClock>
class SlidingCounter {
  static_assert(NUM > 1, "Invalid window size");

  mutable std::array<T, NUM> count_;

 public:
  SlidingCounter() {
    Reset();
  }

  explicit SlidingCounter(Clock clock) : clock_(std::move(clock)) {
    Reset();
  }

  Clock& clock() {
    return clock_;
  }
  const Clock& clock() const {
    return clock_;
  }

  void Inc() {
    IncBy(1);
  }

  void IncBy(T delta) {
    uint32_t bin = MoveTsIfNeeded();
    count_[bin] += delta;
    total_ += delta;
  }

  // Sums over bins not including the last bin that is currently being filled.
  T SumTail() const {
    uint32_t head = MoveTsIfNeeded();
    return total_ - count_[head];
  }

  T Sum() const {
    MoveTsIfNeeded();
    return total_;
  }

  void Reset() {
    count_.fill(0);
    total_ = 0;
  }

 private:
  // Advances to the current slot, clearing any bins that rolled out of the window and
  // updating the cached total accordingly. Returns the current (head) bin index.
  // Const semantics even though it mutates the mutable ring state.
  uint32_t MoveTsIfNeeded() const;

  Clock clock_;
  mutable T total_ = 0;
  mutable uint64_t last_ts_ = 0;
};

// Requires proactor_pool initialize all the proactors.
template <unsigned NUM> class SlidingCounterDist : protected detail::SlidingCounterBase {
  using Counter = SlidingCounter<NUM>;

 public:
  enum { WIN_SIZE = NUM };

  SlidingCounterDist() = default;

  void Init(ProactorPool* pp) {
    InitInternal(pp);
    sc_thread_map_.reset(new Counter[pp_->size()]);
  }

  void Shutdown() {
    sc_thread_map_.reset();
    pp_ = nullptr;
  }

  void Inc() {
    sc_thread_map_[ProactorThreadIndex()].Inc();
  }

  uint32_t Sum() const {
    CheckInit();

    std::atomic_uint32_t res{0};
    pp_->AwaitBrief([&](unsigned i, auto*) {
      res.fetch_add(sc_thread_map_[i].Sum(), std::memory_order_relaxed);
    });

    return res.load(std::memory_order_acquire);
  }

  uint32_t SumTail() const {
    CheckInit();

    std::atomic_uint32_t res{0};
    pp_->AwaitBrief([&](unsigned i, auto*) {
      res.fetch_add(sc_thread_map_[i].SumTail(), std::memory_order_relaxed);
    });

    return res.load(std::memory_order_acquire);
  }

 private:
  std::unique_ptr<Counter[]> sc_thread_map_;
};

/*********************************************
 Implementation section.
**********************************************/

template <unsigned NUM, typename T, typename Clock>
uint32_t SlidingCounter<NUM, T, Clock>::MoveTsIfNeeded() const {
  uint64_t current = clock_();

  // Clock did not advance (or stepped backward, e.g. NTP): keep the window as is and
  // attribute to the current head bin. Never move last_ts_ backward or clear.
  if (current > last_ts_) {
    if (current - last_ts_ >= NUM) {
      // More than a full window elapsed: everything is stale.
      count_.fill(0);
      total_ = 0;
    } else {
      // Clear only the bins that rolled out of the window, keeping the total consistent.
      for (uint64_t i = last_ts_ + 1; i <= current; ++i)
        total_ -= std::exchange(count_[i % NUM], T{0});
    }
    last_ts_ = current;
  }

  return static_cast<uint32_t>(last_ts_ % NUM);
}

}  // namespace util
