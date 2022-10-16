// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/time/clock.h>
#include <gmock/gmock.h>

#include <boost/context/continuation.hpp>

#include "base/gtest.h"
#include "base/logging.h"
#include "util/epoll/epoll_pool.h"
#include "util/fibers/fiberqueue_threadpool.h"
#include "util/fibers/simple_channel.h"
#include "util/uring/uring_fiber_algo.h"
#include "util/uring/uring_pool.h"

using namespace boost;
using namespace std;
using namespace chrono_literals;

namespace util {
using namespace uring;
namespace ctx = boost::context;

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
  EXPECT_EQ(std::cv_status::timeout, ec.await_until([] { return false; }, tp));
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
  epoll::EpollPool pool{1};
  pool.Run();

  ProactorBase* proactor = pool.GetNextProactor();
  FiberQueue fq{32};

  auto fiber = proactor->LaunchFiber([&] { fq.Run(); });

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

TEST_F(FibersTest, BoostContext) {
  int a = -1;

  // Switches immediately and passes the current context as sink to the lambda.
  ctx::continuation source = ctx::callcc([&a](ctx::continuation&& sink) {
    a = 0;
    int b = 1;
    for (;;) {
      // switches to sink. sink immediately looses its state but it returns the new one
      // if it switches back here.
      sink = sink.resume();
      int next = a + b;
      a = b;
      b = next;
    }
    return std::move(sink);
  });

  int current = 0;
  int next = 1;
  for (int j = 0; j < 10; ++j) {
    EXPECT_EQ(a, current);
    int tmp = next;
    next += current;
    current = tmp;
    source = std::move(source).resume();
  }

  vector<string> steps;
  ctx::continuation c1 = ctx::callcc([&steps](ctx::continuation&& c) {
    steps.push_back("c1.start");  // here c is the next address right after callcc call.

    // interrupt point A.
    c = c.resume_with([&](ctx::continuation&& c) {
      steps.push_back("rw1");
      return std::move(c);
    });

    // after resume_with call c is the address of testing function at point (B).
    steps.push_back("c1.end");
    return std::move(c);  // continues with c.
  });

  // here c1 is the address after (A) or before "c1.end" line.
  c1 = c1.resume_with([&](ctx::continuation&& c) {
    // here c has the address of c1 lambda before "c1.end" line.
    // in other words, c equals to c1.
    steps.push_back("rw2");
    return std::move(c);  // continue with c1.
  });

  // point B.

  ASSERT_THAT(steps, testing::ElementsAre("c1.start", "rw1", "rw2", "c1.end"));
  ASSERT_FALSE(c1);
}

TEST_F(FibersTest, BoostContextResumeWith) {
  ctx::continuation c1, c2;

  auto cb1 = [&](ctx::continuation&& c) {
    LOG(INFO) << "c1.start";
    c = c.resume();
    LOG(INFO) << "c1.end";

    // c should have been address of (A) but we returned an empty continuation.
    CHECK(!c);

    // instead we kept the address in c2.
    return std::move(c2);
  };

  c1 = ctx::callcc(std::move(cb1));  // runs cb1 until resume() point.

  // c1 represents cb1 above at c1.end line.
  // before switching back to cb1, we run the lambda below on top of c1 stack.
  c1 = c1.resume_with([&](ctx::continuation&& c) {
    LOG(INFO) << "rw1";

    // c and then c2 point to the address right after resume_with call in c1 stack.
    // i.e. to point A.
    c2 = std::move(c);

    return ctx::continuation{};
  });

  // point A.
  ASSERT_FALSE(c1);
  LOG(INFO) << "end";
}

typedef testing::Types<base::mpmc_bounded_queue<int>, folly::ProducerConsumerQueue<int>>
    QueueImplementations;

template <typename Q> class ChannelTest : public ::testing::Test { public: };
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
