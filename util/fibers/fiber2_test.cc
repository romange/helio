// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/fiber2.h"

#include <condition_variable>
#include <mutex>
#include <thread>

#include "base/gtest.h"
#include "base/logging.h"
#include "util/fibers/synchronization.h"
#include "util/fibers/uring_proactor.h"

using namespace std;

namespace util {
namespace fb2 {

constexpr uint32_t kRingDepth = 16;

class FiberTest : public testing::Test {
 public:
};

class ProactorTest : public testing::Test {
 protected:
  void SetUp() final {
    proactor_ = std::make_unique<UringProactor>();
    proactor_thread_ = thread{[this] {
      proactor_->SetIndex(0);
      proactor_->Init(kRingDepth);
      proactor_->Run();
    }};
  }

  void TearDown() final {
    proactor_->Stop();
    proactor_thread_.join();
    proactor_.reset();
  }

  static void SetUpTestCase() {
    testing::FLAGS_gtest_death_test_style = "threadsafe";
  }

  using IoResult = UringProactor::IoResult;

  std::unique_ptr<UringProactor> proactor_;
  std::thread proactor_thread_;
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
    fb1 = Fiber("test1", [] { LOG(INFO) << "test1 run"; });

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

TEST_F(FiberTest, Dispatch) {
  int val1 = 0, val2 = 0;

  Fiber fb2;

  Fiber fb1(Launch::dispatch, "test1", [&] {
    val1 = 1;
    fb2 = Fiber(Launch::dispatch, "test2", [&] { val2 = 1; });
    val1 = 2;
  });
  EXPECT_EQ(1, val1);
  EXPECT_EQ(1, val2);

  fb1.Join();
  EXPECT_EQ(2, val1);

  fb2.Join();
}

TEST_F(FiberTest, EventCount) {
  EventCount ec;
  bool signal = false;
  bool fb_exit = false;

  Fiber fb(Launch::dispatch, "fb", [&] {
    ec.await([&] { return signal; });
    fb_exit = true;
  });
  ec.notify();
  ThisFiber::Yield();

  EXPECT_FALSE(fb_exit);

  signal = true;
  ec.notify();
  fb.Join();
}

TEST_F(ProactorTest, AsyncCall) {
  ASSERT_FALSE(UringProactor::IsProactorThread());
  ASSERT_EQ(-1, UringProactor::GetIndex());

  for (unsigned i = 0; i < ProactorBase::kTaskQueueLen * 2; ++i) {
    VLOG(1) << "Dispatch: " << i;
    proactor_->DispatchBrief([i] {
      VLOG(1) << "I: " << i;
    });
  }
  LOG(INFO) << "DispatchBrief done";
  // proactor_->AwaitBrief([] {});
  usleep(5000);
}

TEST_F(ProactorTest, Await) {
  thread_local int val = 5;

  proactor_->AwaitBrief([] { val = 15; });
  EXPECT_EQ(5, val);

  int j = proactor_->AwaitBrief([] { return val; });
  EXPECT_EQ(15, j);
}

TEST_F(ProactorTest, AsyncEvent) {
  Done done;

  auto cb = [done](IoResult, uint32_t, int64_t payload) mutable {
    done.Notify();
    LOG(INFO) << "notify";
  };

  proactor_->DispatchBrief([&] {
    uring::SubmitEntry se = proactor_->GetSubmitEntry(std::move(cb), 1);
    se.sqe()->opcode = IORING_OP_NOP;
    LOG(INFO) << "submit";
  });
  LOG(INFO) << "DispatchBrief";
  done.Wait();
}

TEST_F(ProactorTest, DispatchTest) {
  CondVarAny cnd1, cnd2;
  Mutex mu;
  int state = 0;

  LOG(INFO) << "LaunchFiber";
  auto fb = proactor_->LaunchFiber("jessie", [&] {

    unique_lock g(mu);
    state = 1;
    LOG(INFO) << "state 1";

    cnd2.notify_one();
    cnd1.wait(g, [&] { return state == 2; });
    LOG(INFO) << "End";
  });

  {
    unique_lock g(mu);
    cnd2.wait(g, [&] { return state == 1; });
    state = 2;
    LOG(INFO) << "state 2";
    cnd1.notify_one();
  }
  LOG(INFO) << "BeforeJoin";
  fb.Join();
}

}  // namespace fb2
}  // namespace util
