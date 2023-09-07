// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/fibers.h"

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
#include <sys/syscall.h>

// older linux systems do not expose this system call so we wrap it in our own function.
int my_gettid() {
  return syscall(SYS_gettid);
}

#endif

#if defined(__FreeBSD__)
#include <pthread_np.h>
#endif

using namespace std;
using absl::StrCat;
using namespace testing;

namespace util {
namespace fb2 {

#if defined(__FreeBSD__)

int my_gettid() {
  return pthread_getthreadid_np();
}

#endif

#if defined(__APPLE__)

unsigned my_gettid() {
 uint64_t tid;
 pthread_threadid_np(NULL, &tid);
 return tid;
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

  proactor_thread = thread{[this, kind, index] {
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
    const TestInfo* const test_info = UnitTest::GetInstance()->current_test_info();
    LOG(INFO) << "Starting " << test_info->name();
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
                                         ,
                                         "uring"
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
  uint64_t epoch = FiberSwitchEpoch();

  Fiber fb1("test1", [&] { ++run; });
  Fiber fb2("test2", [&] { ++run; });
  EXPECT_EQ(epoch, FiberSwitchEpoch());

  fb1.Join();
  fb2.Join();

  EXPECT_EQ(2, run);
  EXPECT_LT(epoch, FiberSwitchEpoch());

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
      "test3", [](unique_ptr<int> p) { EXPECT_EQ(42, *p); }, std::move(pass))
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
  LOG(INFO) << "timeout at " << next.time_since_epoch().count();
  EXPECT_EQ(std::cv_status::timeout, ec.await_until([&] { return signal; }, next));
  fb3.Join();
}

TEST_F(FiberTest, EventCountMT) {
  EventCount ec1, ec2;
  atomic_uint32_t cnt{0}, gate{0};
  constexpr unsigned kNumIters = 1000;

  array<thread, 5> threads;
  for (auto& th : threads) {
    th = thread([&] {
      Fiber fb2("producer", [&] {
        for (unsigned i = 0; i < kNumIters; ++i) {
          ec2.await([&] { return gate.load(memory_order_relaxed) == i; });
          ThisFiber::SleepFor(1us);
          if (cnt.fetch_add(1, memory_order_relaxed) == threads.size() - 1)
            ec1.notify();
        }
      });
      fb2.Join();
    });
  }

  unsigned preempts = 0;

  Fiber fb(Launch::dispatch, "fb1", [&] {
    for (unsigned i = 0; i < kNumIters; ++i) {
      gate.store(i, memory_order_release);
      ec2.notifyAll();

      preempts += ec1.await([&] { return cnt.load(memory_order_relaxed) == threads.size(); });
      cnt.store(0, memory_order_relaxed);
    }
  });

  fb.Join();
  for (auto& th : threads) {
    th.join();
  }
  LOG(INFO) << "Preempts: " << preempts;
}

TEST_F(FiberTest, Future) {
  Promise<int> p1;
  Future<int> f1 = p1.get_future();

  Fiber fb("fb3", [f1 = std::move(f1)]() mutable { EXPECT_EQ(42, f1.get()); });
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
      [up = reinterpret_cast<UringProactor*>(pth.proactor.get()), cb = std::move(cb)] {
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
      ec.await_until([] { return false; }, chrono::steady_clock::now() + 10us);
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

TEST_F(FiberTest, CleanExit) {
  ASSERT_EXIT(
      {
        thread th([] { Fiber([] { exit(42); }).Join(); });
        th.join();
      },
      ::testing::ExitedWithCode(42), "");
}

// EXPECT_DEATH does not work well with freebsd, also it does not work well with gtest_repeat.
#if 0
TEST_F(FiberTest, AtomicGuard) {
  FiberAtomicGuard guard;
#ifndef NDEBUG
  EXPECT_DEATH(ThisFiber::Yield(), "Preempting inside");
#endif
}
#endif

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

constexpr unsigned kNumFibers = 64;
constexpr unsigned kNumThreads = 16;

TEST_P(ProactorTest, MultiParking) {
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

        string_view name = ThisFiber::GetName();
        VLOG(1) << "After ec.notify() " << name;

        for (unsigned iter = 0; iter < 10; ++iter) {
          for (unsigned k = 0; k < kNumThreads; ++k) {
            DVLOG(2) << name << " " << pthread_self() << " -> " << iter << "/" << k;
            ths[k]->proactor->AwaitBrief([=] {});
          }
        }

        VLOG(1) << "After loop " << name;
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

  pid_t dest_tid = pth.get()->AwaitBrief([&] { return my_gettid(); });

  Fiber fb = proactor()->LaunchFiber([&] {
    // Originally I used pthread_self(). However it is declared as 'attribute ((const))'
    // thus it allows compiler to eliminate subsequent calls to this function.
    // Therefore I use syscall variant to get fresh values.
    pid_t tid1 = my_gettid();
    LOG(INFO) << "Source pid " << tid1 << ", dest pid " << dest_tid;
    proactor()->Migrate(pth.get());
    LOG(INFO) << "After migrate";
    ASSERT_EQ(dest_tid, my_gettid());
  });
  fb.Join();
}

TEST_P(ProactorTest, NotifyRemote) {
  EventCount ec;
  Done done;
  proactor()->Await([&] {
    for (unsigned i = 0; i < 1000; ++i) {
      ec.await_until([] { return false; }, chrono::steady_clock::now() + 10us);
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

TEST_P(ProactorTest, Timeout) {
  EventCount ec;

  auto consumer_fb = proactor()->LaunchFiber("consumer", [&] {
    for (unsigned i = 0; i < 1000; ++i) {
      ec.await_until([] { return false; }, chrono::steady_clock::now() + 10us);
    }
    LOG(INFO) << "Consumer done";
  });

  Fiber producer_fb("producer", [&] {
    for (unsigned i = 0; i < 1000; ++i) {
      ec.notify();
      ThisFiber::SleepFor(1us);
    }
    LOG(INFO) << "Producer done";
  });

  LOG(INFO) << "Before join ";
  consumer_fb.Join();
  producer_fb.Join();
}

TEST_P(ProactorTest, Mutex) {
  unique_ptr<ProactorThread> ths[kNumThreads];
  Fiber fbs[kNumThreads];

  for (unsigned i = 0; i < kNumThreads; ++i) {
    ths[i] = CreateProactorThread();
  }

  Mutex mu;

  for (unsigned i = 0; i < kNumThreads; ++i) {
    fbs[i] = ths[i]->get()->LaunchFiber([&] { lock_guard lk(mu); });
  }

  for (auto& fb : fbs)
    fb.Join();

  for (auto& th : ths)
    th.reset();
}

TEST_P(ProactorTest, DragonflyBug1591) {
  auto sock = std::unique_ptr<FiberSocketBase>(proactor()->CreateSocket());
  auto sock2 = std::unique_ptr<FiberSocketBase>(proactor()->CreateSocket());

  int next_step = 0;
  auto start_step = [&next_step](int step) {
    while (next_step < step) {
      ThisFiber::Yield();
    }
    LOG(INFO) << "step " << step;
  };
  auto end_step = [&next_step]() { next_step++; };

  Mutex m1, m2;
  auto fb_server = proactor()->LaunchFiber("server", [&] {
    start_step(0);
    auto ec = sock->Listen(0, /*backlog=*/10);
    ASSERT_FALSE(ec) << ec.message();
    end_step();

    start_step(2);
    auto a_res = sock->Accept();
    a_res.value()->SetProactor(proactor());
    ASSERT_TRUE(a_res.has_value()) << a_res.error().message();
    m1.lock();
    end_step();

    start_step(4);
    for (size_t i = 0; i < 100; i++) {
      ec = a_res.value()->Write(io::Buffer("TRIGGERS A READ IN THE OTHER SOCKET"sv));
      ThisFiber::SleepFor(1ms);
    }

    a_res.value()->Close();
    sock->Close();

    m1.unlock();
    end_step();
  });

  auto fb_client = proactor()->LaunchFiber("client", [&] {
    start_step(1);
    auto localhost = boost::asio::ip::make_address("127.0.0.1");
    auto ep = FiberSocketBase::endpoint_type{localhost, sock->LocalEndpoint().port()};
    auto ec = sock2->Connect(ep);
    end_step();

    start_step(3);
    ASSERT_FALSE(ec) << ec.message();
    uint8_t buf[128];
    // This triggers the dangling read_context_ bug on timeout.
    sock2->set_timeout(1);
    auto res = sock2->Recv(buf);
    ASSERT_FALSE(res.has_value()) << "Receive should fail on timeout";
    end_step();

    // Now try to acquire the lock. Since it was acquired by the other fiber in step #2,
    // this will stall. Make sure that we don't get spurios wakeups because of the socket.
    m1.lock();
    m1.unlock();
    sock2->Close();
  });

  fb_client.Join();
  fb_server.Join();
}

TEST_P(ProactorTest, dump_fiber_stacks) {
  fb2::detail::FiberInterface::PrintAllFiberStackTraces();
}

}  // namespace fb2
}  // namespace util
