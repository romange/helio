// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/io_buf.h"

#include <cstring>
#include <utility>

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
  const uint64_t generation = buf.generation();

  constexpr size_t kRemainder = 100;
  static_assert(kCompactThreshold > kRemainder,
                "kCompactThreshold must be greater than kRemainder");

  // Consume leaving kRemainder bytes (< kCompactThreshold). Since size_ != capacity_,
  // only the opportunistic condition (offs_ > size_/2 && remainder < kCompactThreshold) fires.
  buf.ConsumeInput(kCompactThreshold - kRemainder);
  EXPECT_EQ(buf.InputLen(), kRemainder);
  EXPECT_EQ(buf.AppendLen(), kCap - kRemainder);  // offs_ reset to 0, space reclaimed
  EXPECT_EQ(buf.generation(), generation);
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
  const uint64_t generation = buf.generation();
  buf.Compact();
  EXPECT_EQ(buf.InputLen(), kRemainder);
  EXPECT_EQ(buf.AppendLen(), kConsumed);
  EXPECT_EQ(buf.generation(), generation);
  for (uint8_t b : buf.InputBuffer()) {
    EXPECT_EQ(b, 0xBB);
  }
}

// Verifies shrinking preserves unread input and replaces the raw buffer memory.
TEST(IoBuf, ShrinkPreservesUnreadInput) {
  IoBuf buf(1024);
  FillAppend(&buf, 0xCC);
  buf.ConsumeInput(600);

  const uint64_t generation = buf.generation();
  ASSERT_TRUE(buf.ShrinkTo(512));
  EXPECT_EQ(buf.Capacity(), 512u);
  EXPECT_EQ(buf.InputLen(), 424u);
  EXPECT_GT(buf.generation(), generation);
  for (uint8_t byte : buf.InputBuffer()) {
    EXPECT_EQ(byte, 0xCC);
  }
}

// Verifies shrinking validates the rounded capacity rather than the requested target.
TEST(IoBuf, ShrinkRoundsUpToFitUnreadInput) {
  IoBuf buf(1024);
  FillAppend(&buf, 0xCC);
  buf.ConsumeInput(700);

  ASSERT_TRUE(buf.ShrinkTo(257));
  EXPECT_EQ(buf.Capacity(), 512u);
  EXPECT_EQ(buf.InputLen(), 324u);
  for (uint8_t byte : buf.InputBuffer()) {
    EXPECT_EQ(byte, 0xCC);
  }
}

// Verifies shrinking an empty buffer updates capacity and raw buffer memory.
TEST(IoBuf, ShrinkEmptyBuffer) {
  IoBuf buf(1024);

  const uint64_t generation = buf.generation();
  ASSERT_TRUE(buf.ShrinkTo(512));
  EXPECT_EQ(buf.Capacity(), 512u);
  EXPECT_EQ(buf.InputLen(), 0u);
  EXPECT_EQ(buf.AppendLen(), 512u);
  EXPECT_GT(buf.generation(), generation);
}

// Verifies a zero shrink target selects the minimum one-byte capacity.
TEST(IoBuf, ShrinkZeroTargetUsesMinimumCapacity) {
  IoBuf buf(1024);

  ASSERT_TRUE(buf.ShrinkTo(0));
  EXPECT_EQ(buf.Capacity(), 1u);
  EXPECT_EQ(buf.InputLen(), 0u);
  EXPECT_EQ(buf.AppendLen(), 1u);
}

// Verifies an equal-capacity reserve compacts unread input.
TEST(IoBuf, ReserveEqualCapacityCompactsUnreadInput) {
  IoBuf buf(1024);
  FillAppend(&buf, 0xCC);
  buf.ConsumeInput(400);

  const uint64_t generation = buf.generation();
  buf.Reserve(1024);

  EXPECT_EQ(buf.Capacity(), 1024u);
  EXPECT_EQ(buf.InputLen(), 624u);
  EXPECT_EQ(buf.AppendLen(), 400u);
  EXPECT_GT(buf.generation(), generation);
  for (uint8_t byte : buf.InputBuffer()) {
    EXPECT_EQ(byte, 0xCC);
  }
}

// Verifies growing the buffer preserves unread input and replaces raw buffer memory.
TEST(IoBuf, ReserveGrowingPreservesUnreadInput) {
  IoBuf buf(512);
  FillAppend(&buf, 0xCC);
  buf.ConsumeInput(100);

  const uint64_t generation = buf.generation();
  buf.Reserve(1024);

  EXPECT_EQ(buf.Capacity(), 1024u);
  EXPECT_EQ(buf.InputLen(), 412u);
  EXPECT_EQ(buf.AppendLen(), 612u);
  EXPECT_GT(buf.generation(), generation);
  for (uint8_t byte : buf.InputBuffer()) {
    EXPECT_EQ(byte, 0xCC);
  }
}

// Verifies a smaller reserve request does not replace raw buffer memory.
TEST(IoBuf, ReserveSmallerCapacityDoesNothing) {
  IoBuf buf(1024);
  FillAppend(&buf, 0xCC);
  buf.ConsumeInput(400);

  const uint64_t generation = buf.generation();
  buf.Reserve(512);

  EXPECT_EQ(buf.Capacity(), 1024u);
  EXPECT_EQ(buf.InputLen(), 624u);
  EXPECT_EQ(buf.AppendLen(), 0u);
  EXPECT_EQ(buf.generation(), generation);
}

// Verifies EnsureCapacity does not replace raw buffer memory when append space fits.
TEST(IoBuf, EnsureCapacityDoesNothingWhenAppendSpaceFits) {
  IoBuf buf(1024);
  auto appendbuf = buf.AppendBuffer();
  memset(appendbuf.data(), 0xCC, 600);
  buf.CommitWrite(600);
  buf.ConsumeInput(100);

  const uint64_t generation = buf.generation();
  buf.EnsureCapacity(buf.AppendLen());

  EXPECT_EQ(buf.Capacity(), 1024u);
  EXPECT_EQ(buf.InputLen(), 500u);
  EXPECT_EQ(buf.AppendLen(), 424u);
  EXPECT_EQ(buf.generation(), generation);
}

// Verifies shrinking rejects targets that cannot reduce capacity or hold unread input.
TEST(IoBuf, ShrinkRejectsNonReducingOrTooSmallTarget) {
  IoBuf buf(1024);
  FillAppend(&buf, 0xDD);
  const uint64_t generation = buf.generation();

  EXPECT_FALSE(buf.ShrinkTo(1024));
  EXPECT_FALSE(buf.ShrinkTo(512));
  EXPECT_EQ(buf.Capacity(), 1024u);
  EXPECT_EQ(buf.InputLen(), 1024u);
  EXPECT_EQ(buf.generation(), generation);

  IoBuf empty_buf(1024);
  const uint64_t empty_generation = empty_buf.generation();
  EXPECT_FALSE(empty_buf.ShrinkTo(768));
  EXPECT_EQ(empty_buf.Capacity(), 1024u);
  EXPECT_EQ(empty_buf.generation(), empty_generation);
}

// Verifies moving a buffer preserves the generation associated with its raw memory.
TEST(IoBuf, MovePreservesGeneration) {
  IoBuf source(512);
  FillAppend(&source, 0xEE);
  source.Reserve(1024);

  const uint64_t generation = source.generation();
  IoBuf destination(std::move(source));

  EXPECT_EQ(destination.Capacity(), 1024u);
  EXPECT_EQ(destination.InputLen(), 512u);
  EXPECT_EQ(destination.generation(), generation);
  for (uint8_t byte : destination.InputBuffer()) {
    EXPECT_EQ(byte, 0xEE);
  }
}

}  // namespace base
