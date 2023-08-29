// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/ring_buffer.h"

#include <gtest/gtest.h>

#include "base/gtest.h"

TEST(ring_buffer, simple) {
  base::RingBuffer<int> buf(4);

  EXPECT_EQ(buf.capacity(), 4);

  for (size_t i = 0; i < 23; i++)
    buf.EmplaceOrOverride(i);

  EXPECT_EQ(buf.size(), 4);
  EXPECT_FALSE(buf.empty());
  EXPECT_TRUE(buf.Full());

  EXPECT_EQ(buf[0], 19);
  EXPECT_EQ(buf[1], 20);
  EXPECT_EQ(buf[2], 21);
  EXPECT_EQ(buf[3], 22);

  buf.ConsumeHead(2);
  EXPECT_EQ(buf.size(), 2);
  EXPECT_FALSE(buf.empty());
  EXPECT_FALSE(buf.Full());
  EXPECT_EQ(buf[0], 21);
  EXPECT_EQ(buf[1], 22);

  int res;
  EXPECT_TRUE(buf.TryDeque(res));
  EXPECT_EQ(res, 21);
  EXPECT_TRUE(buf.TryDeque(res));
  EXPECT_EQ(res, 22);
  EXPECT_FALSE(buf.TryDeque(res));

  EXPECT_EQ(buf.size(), 0);
  EXPECT_TRUE(buf.empty());
  EXPECT_FALSE(buf.Full());
}

TEST(ring_buffer, TryEmplace) {
  base::RingBuffer<int> buf(4);

  EXPECT_TRUE(buf.TryEmplace(4));
  for (size_t i = 0; i < 4; i++)
    buf.EmplaceOrOverride(i);
  EXPECT_FALSE(buf.TryEmplace(4));
}

TEST(ring_buffer, GetTail) {
  base::RingBuffer<int> buf(4);

  EXPECT_TRUE(buf.GetTail());
  EXPECT_EQ(buf.size(), 1);

  for (size_t i = 0; i < 4; i++)
    buf.EmplaceOrOverride(i);

  EXPECT_FALSE(buf.GetTail());
  EXPECT_TRUE(buf.GetTail(/*override=*/true));
}
