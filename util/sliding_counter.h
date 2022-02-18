// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <array>
#include <cstdint>
#include <memory>
#include <numeric>

#include "util/proactor_pool.h"

namespace util {

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
 *        It's implmented using ring-buffer with size specified at compile time.
 *
 * @tparam NUM
 */
template <unsigned NUM, typename T = int32_t>
class SlidingCounter {
  static_assert(NUM > 1, "Invalid window size");

  mutable std::array<T, NUM> count_;

 public:
  SlidingCounter() {
    Reset();
  }

  void Inc() {
    IncBy(1);
  }

  void IncBy(int32_t delta) {
    int32_t bin = MoveTsIfNeeded();
    count_[bin] += delta;
  }

  // Sums over bins not including the last bin that is currently being filled.
  T SumTail() const;

  T Sum() const {
    MoveTsIfNeeded();
    return std::accumulate(count_.begin(), count_.end(), 0);
  }

  void Reset() {
    count_.fill(0);
  }

 private:
  // Returns the bin corresponding to the current timestamp. Has second precision.
  // updates last_ts_ according to the current timestamp and returns the latest bin.
  // has const semantics even though it updates mutable last_ts_.
  uint32_t MoveTsIfNeeded() const;

  mutable uint32_t last_ts_ = 0;
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
    pp_->Await([&](unsigned i, auto*) {
      res.fetch_add(sc_thread_map_[i].Sum(), std::memory_order_relaxed);
    });

    return res.load(std::memory_order_release);
  }

  uint32_t SumTail() const {
    CheckInit();

    std::atomic_uint32_t res{0};
    pp_->Await([&](unsigned i, auto*) {
      res.fetch_add(sc_thread_map_[i].SumTail(), std::memory_order_relaxed);
    });

    return res.load(std::memory_order_release);
  }

 private:
  std::unique_ptr<Counter[]> sc_thread_map_;
};

/*********************************************
 Implementation section.
**********************************************/

template <unsigned NUM, typename T> auto SlidingCounter<NUM, T>::SumTail() const -> T {
  int32_t start = MoveTsIfNeeded() + 1;  // the tail is one after head.

  T sum = 0;
  for (unsigned i = 0; i < NUM - 1; ++i) {
    sum += count_[(start + i) % NUM];
  }
  return sum;
}

template <unsigned NUM, typename T>
uint32_t SlidingCounter<NUM, T>::MoveTsIfNeeded() const {
  uint32_t current_sec = time(NULL);
  if (last_ts_ + NUM <= current_sec) {
    count_.fill(0);
  } else {
    // Reset delta upto current_time including.
    for (uint32_t i = last_ts_ + 1; i <= current_sec; ++i) {
      count_[i % NUM] = 0;
    }
  }
  last_ts_ = current_sec;

  return current_sec % NUM;
}

}  // namespace util
