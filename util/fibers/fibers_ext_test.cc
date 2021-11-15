// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <absl/time/clock.h>
#include "base/gtest.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/simple_channel.h"
#include "util/uring/uring_pool.h"
#include "util/epoll/ev_pool.h"
#include "util/uring/uring_fiber_algo.h"

using namespace boost;
using namespace std;
using namespace chrono_literals;

namespace util {
using namespace uring;

namespace fibers_ext {

class FibersTest : public testing::Test {
 protected:
  void SetUp() final {
  }

  void TearDown() final {
  }

  static void SetUpTestCase() {
  }
};

TEST_F(FibersTest, EventCount) {
  EventCount ec;
  bool signal = false;
  bool fb_exit = false;

  fibers::fiber fb(fibers::launch::dispatch, [&] {
    ec.await([&] { return signal; });
    fb_exit = true;
  });
  ec.notify();
  this_fiber::yield();
  EXPECT_FALSE(fb_exit);

  signal = true;
  ec.notify();
  fb.join();
}

TEST_F(FibersTest, EventCountTimeout) {
  EventCount ec;
  std::chrono::steady_clock::time_point tp = std::chrono::steady_clock::now() + 5ms;
  EXPECT_EQ(std::cv_status::timeout, ec.await_until([] { return false;}, tp));
}

TEST_F(FibersTest, SpuriousNotify) {
  EventCount ec;

  ASSERT_FALSE(ec.notify());
  ASSERT_FALSE(ec.notifyAll());

  int val = 0;

  auto check_positive = [&val]() -> bool { return val > 0; };
  std::thread t1([check_positive, &ec]() { ec.await(check_positive); });

  while (!ec.notify())
    usleep(1000);
  val = 1;
  ASSERT_TRUE(ec.notify());
  t1.join();

  ASSERT_FALSE(ec.notify());
}

TEST_F(FibersTest, FQTP) {
  FiberQueueThreadPool pool(1, 2);

  for (unsigned i = 0; i < 10000; ++i) {
    ASSERT_EQ(i, pool.Await([=] {
      sched_yield();
      return i;
    }));
  }
}

TEST_F(FibersTest, FiberQueue) {
  epoll::EvPool pool{1};
  pool.Run();

  ProactorBase* proactor = pool.GetNextProactor();
  FiberQueue fq{32};

  auto fiber = proactor->LaunchFiber([&] {
    fq.Run();
  });

  constexpr unsigned kIters = 10000;
  size_t delay = 0;
  size_t invocations = 0;
  for (unsigned i = 0; i < kIters; ++i) {
    auto start = absl::Now();

    fq.Add([&, start] {
      ASSERT_TRUE(proactor->IsProactorThread());
      auto dur = absl::Now() - start;
      delay += absl::ToInt64Microseconds(dur);
      ++invocations;
    });
  }
  fq.Shutdown();
  fiber.join();

  EXPECT_EQ(kIters, invocations);
  EXPECT_LT(delay / kIters, 2000);  //
  EXPECT_GT(delay, 0);              //
}

TEST_F(FibersTest, SimpleChannelDone) {
  SimpleChannel<std::function<void()>> s(2);
  std::thread t([&] {
    while (true) {
      std::function<void()> f;
      if (!s.Pop(f))
        break;
      f();
    }
  });

  for (unsigned i = 0; i < 100; ++i) {
    Done done;
    s.Push([done]() mutable { done.Notify(); });
    done.Wait();
  }
  s.StartClosing();
  t.join();
}

typedef testing::Types<base::mpmc_bounded_queue<int>, folly::ProducerConsumerQueue<int>>
    QueueImplementations;

template <typename Q>
class ChannelTest : public ::testing::Test {
 public:
};
TYPED_TEST_SUITE(ChannelTest, QueueImplementations);

TYPED_TEST(ChannelTest, SimpleChannel) {
  using Channel = SimpleChannel<int, TypeParam>;
  Channel channel(16);
  ASSERT_TRUE(channel.TryPush(2));
  channel.Push(4);

  int val = 0;
  ASSERT_TRUE(channel.Pop(val));
  EXPECT_EQ(2, val);
  ASSERT_TRUE(channel.Pop(val));
  EXPECT_EQ(4, val);

  fibers::fiber fb(fibers::launch::post, [&] { EXPECT_TRUE(channel.Pop(val)); });
  channel.Push(7);
  fb.join();
  EXPECT_EQ(7, val);

  fb = fibers::fiber(fibers::launch::post, [&] { EXPECT_FALSE(channel.Pop(val)); });
  channel.StartClosing();
  fb.join();
}

}  // namespace fibers_ext
}  // namespace util
