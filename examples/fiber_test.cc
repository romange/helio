// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "base/gtest.h"
#include "base/logging.h"
#include "examples/fiber.h"

namespace example {

class FiberTest : public testing::Test {
 public:
};

TEST_F(FiberTest, Basic) {
  int run = 0;
  Fiber fb1("test1", [&] {++run;});
  Fiber fb2("test2", [&] {++run;});
  fb1.Join();
  fb2.Join();

  EXPECT_EQ(2, run);
}

}  // namespace example
