// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/io_buf.h"

#include <cstring>

#include <gtest/gtest.h>

#include "base/gtest.h"

namespace base {

// Must match the hardcoded compaction remainder threshold in IoBuf::ConsumeInput (io_buf.cc).
constexpr size_t kCompactThreshold = 512;

// Fill the buffer's append region with a repeating byte value.
static void FillAppend(IoBuf* buf, uint8_t val) {
  auto appendbuf = buf->AppendBuffer();
  memset(appendbuf.data(), val, appendbuf.size());
  buf->CommitWrite(appendbuf.size());
}

// Tests the opportunistic compaction condition: offs_ > size_/2 && remainder < kCompactThreshold.
// Verifies that ConsumeInput triggers opportunistic compaction when the remaining input
// is below kCompactThreshold (defined in io_buf.cc) and consumed bytes exceed remaining.
// The buffer is NOT at full capacity so only the opportunistic condition fires.
TEST(IoBuf, AutoCompactSmallRemainder) {
  // Use capacity larger than the written data so size_ != capacity_.
  constexpr size_t kCap = kCompactThreshold * 2;
  IoBuf buf(kCap);

  // Write exactly kCompactThreshold bytes, leaving half the buffer free.
  auto appendbuf = buf.AppendBuffer();
  memset(appendbuf.data(), 0xAA, kCompactThreshold);
  buf.CommitWrite(kCompactThreshold);

  ASSERT_EQ(buf.InputLen(), kCompactThreshold);
  ASSERT_EQ(buf.Capacity(), kCap);
  ASSERT_EQ(buf.AppendLen(), kCap - kCompactThreshold);  // NOT at full capacity

  constexpr size_t kRemainder = 100;
  static_assert(kCompactThreshold > kRemainder,
                "kCompactThreshold must be greater than kRemainder");

  // Consume leaving kRemainder bytes (< kCompactThreshold). Since size_ != capacity_,
  // only the opportunistic condition (offs_ > size_/2 && remainder < kCompactThreshold) fires.
  buf.ConsumeInput(kCompactThreshold - kRemainder);
  EXPECT_EQ(buf.InputLen(), kRemainder);
  EXPECT_EQ(buf.AppendLen(), kCap - kRemainder);  // offs_ reset to 0, space reclaimed
  for (uint8_t b : buf.InputBuffer()) {
    EXPECT_EQ(b, 0xAA);
  }
}

// Regression test for a deadlock triggered by pipeline=100 with data-size=2048:
// the buffer reached max capacity (65536) with 782 bytes of partial command remaining.
// Since 782 > kCompactThreshold, ConsumeInput's opportunistic compaction did not fire,
// AppendLen stayed 0, and the connection fiber could never read more data.
// The fix: the caller detects AppendLen==0 at max capacity and calls Compact() explicitly.
TEST(IoBuf, CompactReclaimsSpaceOnFullBuffer) {
  constexpr size_t kCap = 65536;
  constexpr size_t kRemainder = kCompactThreshold + 270;  // 782, from production deadlock
  constexpr size_t kConsumed = kCap - kRemainder;

  IoBuf buf(kCap);
  FillAppend(&buf, 0xBB);

  ASSERT_EQ(buf.Capacity(), kCap);
  ASSERT_EQ(buf.InputLen(), kCap);
  ASSERT_EQ(buf.AppendLen(), 0u);

  buf.ConsumeInput(kConsumed);

  // ConsumeInput alone does NOT compact: 782 > kCompactThreshold, opportunistic condition fails.
  ASSERT_EQ(buf.AppendLen(), 0u);  // deadlock-prone state

  // Caller must explicitly compact to reclaim consumed space.
  buf.Compact();
  EXPECT_EQ(buf.InputLen(), kRemainder);
  EXPECT_EQ(buf.AppendLen(), kConsumed);
  for (uint8_t b : buf.InputBuffer()) {
    EXPECT_EQ(b, 0xBB);
  }
}

}  // namespace base
