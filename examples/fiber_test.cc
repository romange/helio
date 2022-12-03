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
  // Fiber fb([] {});
  // fb.Join();
}

}  // namespace example
