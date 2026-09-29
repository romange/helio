// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/cycle_clock.h"

#include <gtest/gtest.h>

namespace base {

TEST(RealTimeAggregatorTest, Usec10msCoversWholeWindow) {
  const uint64_t cycles_ms = CycleClock::FromUsec(1000);

  // Start of a 10ms window.
  const uint64_t start = 10000 * cycles_ms;

  // 500usec of work in each of the first two 1ms periods of the same 10ms window.
  RealTimeAggregator agg;
  agg.Add(start, start + cycles_ms / 2);
  agg.Add(start + cycles_ms, start + cycles_ms + cycles_ms / 2);

  EXPECT_NEAR(agg.Usec1ms(), 500, 1);
  EXPECT_NEAR(agg.Usec10ms(), 1000, 1);
}

}  // namespace base
