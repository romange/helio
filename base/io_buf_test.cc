// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/io_buf.h"

#include <cstring>

#include <gtest/gtest.h>

#include "base/gtest.h"

namespace base {

// Must match kCompactThreshold inside IoBuf::ConsumeInput (io_buf.cc).
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

  // Consume leaving 100 bytes (< kCompactThreshold). Since size_ != capacity_,
  // only the opportunistic condition (offs_ > size_/2 && remainder < kCompactThreshold) fires.
  buf.ConsumeInput(kCompactThreshold - 100);
  EXPECT_EQ(buf.InputLen(), 100u);
  EXPECT_GE(buf.AppendLen(), kCap - 100);  // offs_ reset to 0, space reclaimed
}

// Tests the full-buffer compaction condition: size_ == capacity_.
// Verifies that ConsumeInput compacts even when remaining input exceeds kCompactThreshold,
// as long as the buffer is completely full (size_ == capacity_).
// This is a regression test for a deadlock triggered by pipeline=100 with data-size=2048:
// the buffer reached max capacity with 782 bytes of partial command remaining. Since
// 782 > kCompactThreshold, the opportunistic compaction did not fire, AppendLen stayed 0,
// and the connection fiber could never read more data.
TEST(IoBuf, ConsumeFromFullBufferAlwaysCompacts) {
  constexpr size_t kCap = 65536;
  // Consume only 1 byte so offs_=1. This makes the opportunistic condition impossible:
  // offs_(1) > size_/2 (32768) is always false, regardless of kCompactThreshold.
  // Only the size_ == capacity_ condition can trigger compaction here.
  constexpr size_t kConsumed = 1;
  constexpr size_t kRemainder = kCap - kConsumed;

  IoBuf buf(kCap);
  FillAppend(&buf, 0xBB);

  ASSERT_EQ(buf.Capacity(), kCap);
  ASSERT_EQ(buf.InputLen(), kCap);
  ASSERT_EQ(buf.AppendLen(), 0u);

  // This is the exact call sequence that caused the deadlock before the fix.
  buf.ConsumeInput(kConsumed);

  // After the fix: ConsumeInput detects size_ == capacity_ and compacts.
  EXPECT_EQ(buf.InputLen(), kRemainder);
  EXPECT_EQ(buf.AppendLen(), kConsumed);
}

}  // namespace base
