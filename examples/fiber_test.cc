// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "examples/fiber.h"

#include <condition_variable>
#include <mutex>
#include <thread>

#include "base/gtest.h"
#include "base/logging.h"

using namespace std;

namespace example {

class FiberTest : public testing::Test {
 public:
};

TEST_F(FiberTest, Basic) {
  int run = 0;
  Fiber fb1("test1", [&] { ++run; });
  Fiber fb2("test2", [&] { ++run; });
  fb1.Join();
  fb2.Join();

  EXPECT_EQ(2, run);
}

TEST_F(FiberTest, Remote) {
  Fiber fb1;
  mutex mu;
  condition_variable cnd;
  bool set = false;
  std::thread t1([&] {
    fb1 = Fiber("test1", [] {
      LOG(INFO) << "test1 run";
    });

    {
      unique_lock lk(mu);
      set = true;
      cnd.notify_one();
      LOG(INFO) << "set signaled";
    }
    this_thread::sleep_for(10ms);
  });

  unique_lock lk(mu);
  cnd.wait(lk, [&] { return set; });
  LOG(INFO) << "set = true";
  fb1.Join();
  LOG(INFO) << "fb1 joined";
  t1.join();
}

}  // namespace example
