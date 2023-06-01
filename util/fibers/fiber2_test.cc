// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/fiber2.h"

#include <absl/strings/str_cat.h>

#include <condition_variable>
#include <mutex>
#include <thread>

#include "base/gtest.h"
#include "base/logging.h"
#include "util/fibers/epoll_proactor.h"
#include "util/fibers/future.h"
#include "util/fibers/synchronization.h"

#ifdef __linux__
 #include "util/fibers/uring_proactor.h"
#else
  #include <pthread_np.h>
#endif

using namespace std;
using absl::StrCat;

namespace util {
namespace fb2 {

#ifndef __linux__

int gettid() {
  return pthread_getthreadid_np();
}

#endif

constexpr uint32_t kRingDepth = 16;

class FiberTest : public testing::Test {
 public:
};

struct ProactorThread {
  std::unique_ptr<ProactorBase> proactor;
  std::thread proactor_thread;

  ProactorThread(unsigned index, ProactorBase::Kind kind);

  ~ProactorThread() {
    LOG(INFO) << "Stopping proactor thread";
    proactor->Stop();
    proactor_thread.join();
    proactor.reset();
  }

  ProactorBase* get() {
    return proactor.get();
  }
};

ProactorThread::ProactorThread(unsigned index, ProactorBase::Kind kind) {
  switch (kind) {
    case ProactorBase::Kind::EPOLL:
      proactor.reset(new EpollProactor);
      break;
    case ProactorBase::Kind::IOURING:
#ifdef __linux__
      proactor.reset(new UringProactor);
#else
      LOG(FATAL) << "IOUring is not supported on this platform";
#endif
      break;

  }

  proactor_thread = thread{[=] {
    proactor->SetIndex(index);
    switch (kind) {
      case ProactorBase::Kind::EPOLL:
        static_cast<EpollProactor*>(proactor.get())->Init();
        break;
      case ProactorBase::Kind::IOURING:
#ifdef __linux__
        static_cast<UringProactor*>(proactor.get())->Init(kRingDepth);
#endif
        break;
    }

    proactor->Run();
  }};
}

class ProactorTest : public testing::TestWithParam<string_view> {

 protected:
  static unique_ptr<ProactorThread> CreateProactorThread() {
    string_view param = GetParam();
    if (param == "epoll") {
      return make_unique<ProactorThread>(0, ProactorBase::EPOLL);
    }
    if (param == "uring") {
      return make_unique<ProactorThread>(0, ProactorBase::IOURING);
    }

    LOG(FATAL) << "Unknown param: " << param;
    return nullptr;
  }

  void SetUp() final {
    proactor_th_ = CreateProactorThread();
  }

  void TearDown() final {
    proactor_th_.reset();
  }

  static void SetUpTestCase() {
    testing::FLAGS_gtest_death_test_style = "threadsafe";
  }

  ProactorBase* proactor() {
    return proactor_th_->get();
  }

  using IoResult = int;

  std::unique_ptr<ProactorThread> proactor_th_;
};

INSTANTIATE_TEST_SUITE_P(Engines, ProactorTest,
                         testing::Values("epoll"
#ifdef __linux__
                         ,"uring"
#endif
                         ));

struct SlistMember {
  detail::FI_ListHook hook;
};

using TestSlist = boost::intrusive::slist<
    SlistMember,
    boost::intrusive::member_hook<SlistMember, detail::FI_ListHook, &SlistMember::hook>,
    boost::intrusive::constant_time_size<false>, boost::intrusive::cache_last<true>>;

TEST_F(FiberTest, SList) {
  TestSlist queue;
  SlistMember m1;
  TestSlist::iterator it = TestSlist::s_iterator_to(m1);
  ASSERT_FALSE(m1.hook.is_linked());
  // queue.erase(it); <- infinite loop since the item is not there.
  queue.push_front(m1);
  ASSERT_TRUE(m1.hook.is_linked());
  queue.erase(it);
}

TEST_F(FiberTest, Basic) {
  int run = 0;
  Fiber fb1("test1", [&] { ++run; });
  Fiber fb2("test2", [&] { ++run; });
  fb1.Join();
  fb2.Join();

  EXPECT_EQ(2, run);

  Fiber fb3(
      "test3", [](int i) {}, 1);
  fb3.Join();
}

TEST_F(FiberTest, Stack) {
  Fiber fb1, fb2;
  {
    uint64_t val1 = 42;
    uint64_t arg1 = 43;
    auto cb1 = [val1](uint64_t arg) {
      ASSERT_EQ(val1, 42);
      ASSERT_EQ(arg, 43);
    };

    fb1 = Fiber(Launch::post, "test1", cb1, arg1);
  }

  {
    uint64_t val = 142;
    auto cb1 = [val](uint64_t arg) {
      EXPECT_EQ(val, 142);
      EXPECT_EQ(arg, 143);
    };

    fb2 = Fiber(Launch::post, "test2", cb1, 143);
  }
  fb1.Join();
  fb2.Join();

  // Test with moveable only arguments.
  unique_ptr<int> pass(new int(42));
  Fiber(
      "test3", [](unique_ptr<int> p) { EXPECT_EQ(42, *p); }, move(pass))
      .Detach();
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

  ASSERT_FALSE(detail::FiberActive()->wait_hook.is_linked());
  EXPECT_EQ(std::cv_status::timeout,
            ec.await_until([] { return false; }, chrono::steady_clock::now() + 10ms));

  ASSERT_FALSE(detail::FiberActive()->sleep_hook.is_linked());
  signal = false;
  Fiber(Launch::post, "fb2", [&] {
    signal = true;
    ec.notify();
  }).Detach();
  auto next = chrono::steady_clock::now() + 1s;
  LOG(INFO) << "timeout at " << next.time_since_epoch().count();

  EXPECT_EQ(std::cv_status::no_timeout, ec.await_until([&] { return signal; }, next));
  signal = false;
  Fiber fb3(Launch::post, "fb3", [&] {
    signal = true;
    ThisFiber::SleepFor(2ms);
    ec.notify();
  });
  next = chrono::steady_clock::now();
  EXPECT_EQ(std::cv_status::timeout, ec.await_until([&] { return signal; }, next));
  fb3.Join();
}

TEST_F(FiberTest, Future) {
  Promise<int> p1;
  Future<int> f1 = p1.get_future();

  Fiber fb("fb3", [f1 = move(f1)]() mutable { EXPECT_EQ(42, f1.get()); });
  p1.set_value(42);

  fb.Join();
}

#ifdef __linux__
TEST_F(FiberTest, AsyncEvent) {
  Done done;

  auto cb = [done](auto*, UringProactor::IoResult, uint32_t) mutable {
    done.Notify();
    LOG(INFO) << "notify";
  };

  ProactorThread pth(0, ProactorBase::IOURING);
  pth.get()->DispatchBrief(
      [up = reinterpret_cast<UringProactor*>(pth.proactor.get()), cb = move(cb)] {
        SubmitEntry se = up->GetSubmitEntry(std::move(cb));
        se.sqe()->opcode = IORING_OP_NOP;
        LOG(INFO) << "submit";
      });

  LOG(INFO) << "DispatchBrief";
  done.Wait();
}
#endif

TEST_F(FiberTest, Notify) {
  EventCount ec;
  Fiber fb1([&] {
    for (unsigned i = 0; i < 1000; ++i) {
      ec.await_until([] { return false;}, chrono::steady_clock::now() + 10us);
    }
  });

  bool done{false};
  Fiber fb2([&] {
    while (!done) {
      ec.notify();
      ThisFiber::SleepFor(10us);
    }
  });

  fb1.Join();
  LOG(INFO) << "fb1 joined";
  done = true;
  fb2.Join();
}

TEST_F(FiberTest, AtomicGuard) {
  FiberAtomicGuard guard;
#ifndef NDEBUG
  EXPECT_DEATH(ThisFiber::Yield(), "Preempting inside");
#endif
}

TEST_P(ProactorTest, AsyncCall) {
  ASSERT_FALSE(ProactorBase::IsProactorThread());
  ASSERT_EQ(-1, ProactorBase::GetIndex());

  for (unsigned i = 0; i < ProactorBase::kTaskQueueLen * 2; ++i) {
    VLOG(1) << "Dispatch: " << i;
    proactor()->DispatchBrief([i] { VLOG(1) << "I: " << i; });
  }
  LOG(INFO) << "DispatchBrief done";
  proactor()->AwaitBrief([] {});

  usleep(2000);
  EventCount ec;
  bool signal = false;
  proactor()->DispatchBrief([&] {
    signal = true;
    ec.notify();
  });

  auto next = chrono::steady_clock::now() + 1s;
  EXPECT_EQ(std::cv_status::no_timeout, ec.await_until([&] { return signal; }, next));
}

TEST_P(ProactorTest, Await) {
  thread_local int val = 5;

  proactor()->AwaitBrief([] { val = 15; });
  EXPECT_EQ(5, val);

  int j = proactor()->AwaitBrief([] { return val; });
  EXPECT_EQ(15, j);
}

TEST_P(ProactorTest, DispatchTest) {
  CondVarAny cnd1, cnd2;
  Mutex mu;
  int state = 0;

  LOG(INFO) << "LaunchFiber";
  auto fb = proactor()->LaunchFiber("jessie", [&] {
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

TEST_P(ProactorTest, Sleep) {
  proactor()->Await([] {
    LOG(INFO) << "Before Sleep";
    ThisFiber::SleepFor(20ms);
    LOG(INFO) << "After Sleep";
  });
}

TEST_P(ProactorTest, MultiParking) {
  constexpr unsigned kNumFibers = 64;
  constexpr unsigned kNumThreads = 32;

  EventCount ec;
  unique_ptr<ProactorThread> ths[kNumThreads];
  atomic_uint num_started{0};
  Fiber fbs[kNumThreads][kNumFibers];

  for (unsigned i = 0; i < kNumThreads; ++i) {
    ths[i] = CreateProactorThread();
  }

  for (unsigned i = 0; i < kNumThreads; ++i) {
    for (unsigned j = 0; j < kNumFibers; ++j) {
      fbs[i][j] = ths[i]->proactor->LaunchFiber(StrCat("test", i, "/", j), [&] {
        num_started.fetch_add(1, std::memory_order_relaxed);
        ec.notify();

        VLOG(1) << "After ec.notify()";
        for (unsigned iter = 0; iter < 10; ++iter) {
          for (unsigned k = 0; k < kNumThreads; ++k) {
            ths[k]->proactor->AwaitBrief([] { return true; });
          }
        }
      });
    }
  }

  ec.await([&] { return num_started == kNumThreads * kNumFibers; });
  LOG(INFO) << "After the first await";

  for (auto& fb_arr : fbs) {
    for (auto& fb : fb_arr)
      fb.Join();
  }

  LOG(INFO) << "After fiber join";
  for (auto& th : ths)
    th.reset();
}

TEST_P(ProactorTest, Migrate) {
  ProactorThread pth(0, proactor()->GetKind());

  pid_t dest_tid = pth.get()->AwaitBrief([&] { return gettid(); });

  Fiber fb = proactor()->LaunchFiber([&] {
    // Originally I used pthread_self(). However it is declared as 'attribute ((const))'
    // thus it allows compiler to eliminate subsequent calls to this function.
    // Therefore I use syscall variant to get fresh values.
    pid_t tid1 = gettid();
    LOG(INFO) << "Source pid " << tid1 << ", dest pid " << dest_tid;
    proactor()->Migrate(pth.get());
    LOG(INFO) << "After migrate";
    ASSERT_EQ(dest_tid, gettid());
  });
  fb.Join();
}

TEST_P(ProactorTest, NotifyRemote) {
  EventCount ec;
  Done done;
  proactor()->Await([&] {
    for (unsigned i = 0; i < 1000; ++i) {
      ec.await_until([] { return false;}, chrono::steady_clock::now() + 10us);
    }
    done.Notify();
  });

  bool keep_run{true};
  Fiber fb2([&] {
    while (keep_run) {
      ec.notify();
      ThisFiber::SleepFor(1us);
    }
  });

  done.Wait();
  keep_run = false;
  fb2.Join();
}

TEST_P(ProactorTest, BriefDontBlock) {
  Done done;


  proactor()->AwaitBrief([&] {
#ifndef NDEBUG
  EXPECT_DEATH(done.WaitFor(1ms), "Should not preempt");
#endif
  });
}

}  // namespace fb2
}  // namespace util
