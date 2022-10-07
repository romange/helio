// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/epoll/proactor.h"

#include <fcntl.h>
#include <gmock/gmock.h>
#include <netinet/in.h>
#include <sys/socket.h>

#include "absl/time/clock.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "base/varz_value.h"
#include "util/epoll/epoll_fiber_scheduler.h"
#include "util/fibers/fibers_ext.h"

using namespace boost;
using namespace std;
using base::VarzValue;
using testing::ElementsAre;
using testing::Pair;

namespace util {
namespace epoll {

class EpollProactorTest : public testing::Test {
 protected:
  void SetUp() override {
    ev_cntrl_ = std::make_unique<EpollProactor>();
    ev_cntrl_thread_ = thread{[this] {
      ev_cntrl_->Init();
      ev_cntrl_->Run(); }};
  }

  void TearDown() override {
    ev_cntrl_->Stop();
    ev_cntrl_thread_.join();
    ev_cntrl_.reset();
  }

  static void SetUpTestCase() {
    testing::FLAGS_gtest_death_test_style = "threadsafe";
  }

  std::unique_ptr<EpollProactor> ev_cntrl_;
  std::thread ev_cntrl_thread_;
};

TEST_F(EpollProactorTest, AsyncCall) {
  for (unsigned i = 0; i < 1000; ++i) {
    ev_cntrl_->DispatchBrief([] {});
  }
  usleep(5000);
}

TEST_F(EpollProactorTest, Await) {
  thread_local int val = 5;

  ev_cntrl_->AwaitBrief([] { val = 15; });
  EXPECT_EQ(5, val);

  int j = ev_cntrl_->AwaitBrief([] { return val; });
  EXPECT_EQ(15, j);
}

TEST_F(EpollProactorTest, Sleep) {
  ev_cntrl_->Await([] {
    LOG(INFO) << "Before Sleep";
    this_fiber::sleep_for(20ms);
    LOG(INFO) << "After Sleep";
  });
}

TEST_F(EpollProactorTest, DispatchTest) {
  fibers::condition_variable cnd1, cnd2;
  fibers::mutex mu;
  int state = 0;

  LOG(INFO) << "LaunchFiber";
  auto fb = ev_cntrl_->LaunchFiber([&] {
    this_fiber::properties<FiberProps>().set_name("jessie");

    std::unique_lock<fibers::mutex> g(mu);
    state = 1;
    LOG(INFO) << "state 1";

    cnd2.notify_one();
    cnd1.wait(g, [&] { return state == 2; });
    LOG(INFO) << "End";
  });

  {
    std::unique_lock<fibers::mutex> g(mu);
    cnd2.wait(g, [&] { return state == 1; });
    state = 2;
    LOG(INFO) << "state 2";
    cnd1.notify_one();
  }
  LOG(INFO) << "BeforeJoin";
  fb.join();
}

TEST_F(EpollProactorTest, Periodic) {
  unsigned count = 0;
  auto cb = [&] {
    VLOG(1) << "Tick " << count;
    ++count;
  };

  uint32_t id = ev_cntrl_->AwaitBrief([&] { return ev_cntrl_->AddPeriodic(1, cb); });

  usleep(20000);
  ev_cntrl_->Await([&] { return ev_cntrl_->CancelPeriodic(id); });
  unsigned num = count;
  ASSERT_TRUE(count >= 15 && count <= 25) << count;
  usleep(20000);
  EXPECT_EQ(num, count);
}

TEST_F(EpollProactorTest, SleepMany) {
  constexpr size_t kNumIters = 600;

  auto cb = []() {
    this_fiber::properties<FiberProps>().set_name("sleeper");
    for (unsigned i = 0; i < kNumIters; ++i) {
      auto sleep = chrono::steady_clock::now();
      sleep += 15us;

      DVLOG(1) << "sleep_until " << sleep.time_since_epoch().count();
      this_fiber::sleep_until(sleep);
    }
    LOG(INFO) << "Sleeper finished ";
  };
  ev_cntrl_->Await(cb);
  LOG(INFO) << "AwaitSleep finished ";
}

void BM_AsyncCall(benchmark::State& state) {
  EpollProactor proactor;
  std::thread t([&] { proactor.Run(); });

  while (state.KeepRunning()) {
    proactor.DispatchBrief([] {});
  }
  proactor.Stop();
  t.join();
}
BENCHMARK(BM_AsyncCall);

void BM_AwaitCall(benchmark::State& state) {
  EpollProactor proactor;
  std::thread t([&] { proactor.Run(); });

  while (state.KeepRunning()) {
    proactor.AwaitBrief([] {});
  }
  proactor.Stop();
  t.join();
}
BENCHMARK(BM_AwaitCall);

}  // namespace epoll
}  // namespace util
