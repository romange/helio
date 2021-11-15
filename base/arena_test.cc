// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "base/arena.h"

#include <sys/mman.h>
#include <random>

#include "base/gtest.h"
#include "base/logging.h"

namespace base {

class ArenaTest { };

TEST(ArenaTest, Empty) {
  Arena arena;
}

TEST(ArenaTest, Simple) {
  std::default_random_engine rand(301);
  std::vector<std::pair<size_t, char*> > allocated;
  Arena arena;
  const int N = 100000;
  size_t bytes = 0;
  for (int i = 0; i < N; i++) {
    size_t s;
    if (i % (N / 10) == 0) {
      s = i;
    } else {
      s = (rand() % 4000 == 1) ? rand() % 6000 :
          (rand() % 10 == 1) ? rand() % 100 : rand() % 20;
    }
    if (s == 0) {
      // Our arena disallows size 0 allocations.
      s = 1;
    }
    char* r;
    if (rand() % 10 == 0) {
      r = arena.AllocateAligned(s);
    } else {
      r = arena.Allocate(s);
    }

    for (size_t b = 0; b < s; b++) {
      // Fill the "i"th allocation with a known bit pattern
      r[b] = i % 256;
    }
    bytes += s;
    allocated.push_back(std::make_pair(s, r));
    ASSERT_GE(arena.MemoryUsage(), bytes);
    if (i > N/10) {
      ASSERT_LE(arena.MemoryUsage(), bytes * 1.10);
    }
  }
  for (size_t i = 0; i < allocated.size(); i++) {
    size_t num_bytes = allocated[i].first;
    const char* p = allocated[i].second;

    for (size_t b = 0; b < num_bytes; b++) {
      // Check the "i"th allocation for the known bit pattern
      ASSERT_EQ(int(p[b]) & 0xff, i % 256);
    }
  }
}

}  // namespace base
