// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/proactor.h"

#include <fcntl.h>
#include <gmock/gmock.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/stat.h>

#include "absl/time/clock.h"
#include "base/gtest.h"
#include "base/logging.h"
#include "util/accept_server.h"
#include "util/fibers/fibers_ext.h"
#include "util/sliding_counter.h"
#include "util/uring/uring_fiber_algo.h"
#include "util/uring/uring_file.h"
#include "util/uring/uring_pool.h"
#include "util/varz.h"

using namespace boost;
using namespace std;
using base::VarzValue;
using testing::ElementsAre;
using testing::Pair;

namespace util {
namespace uring {

constexpr uint32_t kRingDepth = 16;

class ProactorTest : public testing::Test {
 protected:
  void SetUp() final {
    proactor_ = std::make_unique<Proactor>();
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

  using IoResult = Proactor::IoResult;

  std::unique_ptr<Proactor> proactor_;
  std::thread proactor_thread_;
};

TEST_F(ProactorTest, AsyncCall) {
  for (unsigned i = 0; i < 10000; ++i) {
    proactor_->DispatchBrief([] {});
  }
  usleep(5000);
}

TEST_F(ProactorTest, Await) {
  thread_local int val = 5;

  proactor_->AwaitBrief([] { val = 15; });
  EXPECT_EQ(5, val);

  int j = proactor_->AwaitBrief([] { return val; });
  EXPECT_EQ(15, j);
}

TEST_F(ProactorTest, Sleep) {
  proactor_->Await([] {
    LOG(INFO) << "Before Sleep";
    this_fiber::sleep_for(20ms);
    LOG(INFO) << "After Sleep";
  });
}

TEST_F(ProactorTest, SqeOverflow) {
  size_t unique_id = 0;
  char buf[128];

  int fd = open(gflags::GetArgv0(), O_RDONLY | O_CLOEXEC);
  CHECK_GT(fd, 0);

  constexpr size_t kMaxPending = kRingDepth * 100;
  fibers_ext::BlockingCounter bc(kMaxPending);
  auto cb = [&bc](IoResult, uint32_t, int64_t payload) { bc.Dec(); };

  proactor_->Dispatch([&]() mutable {
    for (unsigned i = 0; i < kMaxPending; ++i) {
      SubmitEntry se = proactor_->GetSubmitEntry(cb, unique_id++);
      se.PrepRead(fd, buf, sizeof(buf), 0);
      VLOG(1) << i;
    }
  });

  bc.Wait();  // We wait on all callbacks before closing FD that is being handled by IO loop.

  close(fd);
}

TEST_F(ProactorTest, SqPoll) {
  io_uring_params params;
  memset(&params, 0, sizeof(params));

  // IORING_SETUP_SQPOLL require CAP_SYS_ADMIN permissions (root). In addition, every fd used
  // with this ring should be registered via io_uring_register syscall.
  params.flags |= IORING_SETUP_SQPOLL;
  io_uring ring;

  int res = io_uring_queue_init_params(16, &ring, &params);
  if (res != 0) {
    int err = errno;
    LOG(INFO) << "IORING_SETUP_SQPOLL not supported " << err;
    return;
  }

  io_uring_sqe* sqe = io_uring_get_sqe(&ring);
  io_uring_prep_nop(sqe);
  sqe->user_data = 42;
  int num_submitted = io_uring_submit(&ring);
  ASSERT_EQ(1, num_submitted);
  io_uring_cqe* cqe;

  ASSERT_EQ(0, io_uring_wait_cqe(&ring, &cqe));
  EXPECT_EQ(0, cqe->res);
  EXPECT_EQ(42, cqe->user_data);
  io_uring_cq_advance(&ring, 1);

  int fds[2];
  for (int i = 0; i < 2; ++i) {
    fds[i] = socket(AF_INET, SOCK_DGRAM | SOCK_CLOEXEC, 0);
    ASSERT_GT(fds[i], 0);
  }
  int srv_fd = fds[0];
  int clientfd = fds[1];

  ASSERT_EQ(0, io_uring_register_files(&ring, fds, 2));

  struct sockaddr_in serv_addr;
  memset(&serv_addr, 0, sizeof(serv_addr));
  serv_addr.sin_family = AF_INET;
  serv_addr.sin_port = htons(10200);
  serv_addr.sin_addr.s_addr = htonl(INADDR_ANY);

  res = bind(srv_fd, (sockaddr*)&serv_addr, sizeof(serv_addr));
  ASSERT_EQ(0, res) << strerror(errno) << " " << errno;

  // Issue recv command
  sqe = io_uring_get_sqe(&ring);
  char buf[100];
  io_uring_prep_recv(sqe, 0, buf, sizeof(buf), 0);
  sqe->user_data = 43;
  sqe->flags |= IOSQE_FIXED_FILE;
  num_submitted = io_uring_submit(&ring);
  ASSERT_EQ(1, num_submitted);

  // Issue connect command. Cpnnect/accept are not supported by uring/sqpoll.
  res = connect(clientfd, (sockaddr*)&serv_addr, sizeof(serv_addr));
  ASSERT_EQ(0, res);
  write(clientfd, buf, 1);

  ASSERT_EQ(0, io_uring_wait_cqe_nr(&ring, &cqe, 1));
  ASSERT_EQ(43, cqe[0].user_data);
  EXPECT_EQ(1, cqe->res);  // Got 1 byte

  io_uring_queue_exit(&ring);
  close(srv_fd);
}

TEST_F(ProactorTest, AsyncEvent) {
  fibers_ext::Done done;

  auto cb = [done](IoResult, uint32_t, int64_t payload) mutable { done.Notify(); };

  proactor_->DispatchBrief([&] {
    SubmitEntry se = proactor_->GetSubmitEntry(std::move(cb), 1);
    se.sqe()->opcode = IORING_OP_NOP;
  });
  done.Wait();
}

TEST_F(ProactorTest, Migrate) {
  unique_ptr<Proactor> dest = std::make_unique<Proactor>();
  pid_t dest_tid = 0;

  mutex mu;
  condition_variable cv;
  thread dest_thread = thread{[&] {
    dest->SetIndex(1);
    dest->Init(kRingDepth);
    dest_tid = gettid();
    LOG(INFO) << "Running dest thread " << dest_tid;
    mu.lock();
    cv.notify_all();
    mu.unlock();
    dest->Run();
  }};

  unique_lock lk(mu);
  cv.wait(lk, [&] { return dest_tid != 0; });

  fibers::fiber fb = proactor_->LaunchFiber([&] {
    // Originally I used pthread_self(). However it is declared as 'attribute ((const))'
    // thus it allows compiler to eliminate subsequent calls to this function.
    // Therefore I use syscall variant to get fresh values.
    pid_t tid1 = gettid();
    LOG(INFO) << "Source pid " << tid1 << ", dest pid " << dest_tid;
    proactor_->Migrate(dest.get());
    LOG(INFO) << "After migrate";
    ASSERT_EQ(dest_tid, gettid());
  });
  fb.join();
  dest->Stop();
  dest_thread.join();
}

TEST_F(ProactorTest, Pool) {
  std::atomic_int val{0};
  UringPool pool{16, 2};
  pool.Run();

  pool.AwaitFiberOnAll([&](auto*) { val += 1; });
  EXPECT_EQ(2, val);

  EXPECT_EQ(-1, ProactorBase::GetIndex());
  int id = pool[0].AwaitBrief([] { return ProactorBase::GetIndex(); });
  EXPECT_EQ(0, id);

  pool.Stop();
}

TEST_F(ProactorTest, DispatchTest) {
  fibers::condition_variable cnd1, cnd2;
  fibers::mutex mu;
  int state = 0;

  LOG(INFO) << "LaunchFiber";
  auto fb = proactor_->LaunchFiber([&] {
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

TEST_F(ProactorTest, SlidingCounter) {
  SlidingCounter<10> sc;
  proactor_->AwaitBrief([&] { sc.Inc(); });
  uint32_t cnt = proactor_->AwaitBrief([&] { return sc.Sum(); });
  EXPECT_EQ(1, cnt);

  UringPool pool{16, 2};
  pool.Run();
  SlidingCounterDist<4> sc2;
  sc2.Init(&pool);
  pool.Await([&](auto*) { sc2.Inc(); });
  cnt = sc2.Sum();
  EXPECT_EQ(2, cnt);
}

TEST_F(ProactorTest, Varz) {
  VarzQps qps("test1");
  ASSERT_DEATH(qps.Inc(), "");  // Messes up log-file softlink.

  UringPool pool{16, 2};
  pool.Run();

  qps.Init(&pool);

  vector<pair<string, uint32_t>> vals;
  auto cb = [&vals](const char* str, VarzValue&& v) { vals.emplace_back(str, v.num); };
  pool[0].Await([&] { qps.Iterate(cb); });

  EXPECT_THAT(vals, ElementsAre(Pair("test1", 0)));
}

TEST_F(ProactorTest, Periodic) {
  unsigned count = 0;
  auto cb = [&] { ++count; };

  uint32_t id = proactor_->AwaitBrief([&] { return proactor_->AddPeriodic(1, cb); });
  usleep(20000);
  proactor_->Await([&] { return proactor_->CancelPeriodic(id); });
  unsigned num = count;
  ASSERT_TRUE(count >= 15 && count <= 25) << count;
  usleep(20000);
  EXPECT_EQ(num, count);
}

TEST_F(ProactorTest, TimedWait) {
  fibers_ext::EventCount ec;
  fibers::mutex mu;
  fibers::condition_variable cv;
  bool res = false;
  auto fb1 = proactor_->LaunchFiber([&] {
    std::unique_lock<fibers::mutex> lk(mu);
    cv.wait(lk, [&] { return res; });
  });

  auto fb2 = proactor_->LaunchFiber([&] {
    this_fiber::properties<FiberProps>().set_name("waiter");
    this_fiber::sleep_for(1ms);

    chrono::steady_clock::time_point tp = chrono::steady_clock::now() + 1ms;
    cv_status status = ec.await_until([] { return false; }, tp);
    LOG(INFO) << "await_until " << int(status);
  });

  this_fiber::sleep_for(1ms);
  mu.lock();
  res = true;
  cv.notify_one();
  mu.unlock();

  fb1.join();
  fb2.join();
}

TEST_F(ProactorTest, File) {
  string path = base::GetTestTempPath("foo.txt");
  LOG(INFO) << "writing to path " << path;

  io::Result<io::WriteFile*> res = proactor_->Await([&] { return OpenWrite(path); });
  ASSERT_TRUE(res) << res.error().message();

  unique_ptr<io::WriteFile> file(res.value());
  string str(1u << 18, 'a');
  error_code ec = proactor_->Await([&] { return file->Write(str); });
  ASSERT_FALSE(ec) << ec;

  str.assign(1u << 18, 'b');
  ec = proactor_->Await([&] { return file->Write(str); });
  ec = proactor_->Await([&] { return file->Close(); });
  struct statx sbuf;
  ASSERT_EQ(0, statx(0, path.c_str(), AT_STATX_SYNC_AS_STAT, STATX_SIZE, &sbuf));
  ASSERT_EQ(1 << 19, sbuf.stx_size);
}

#if 0
TEST_F(ProactorTest, Splice) {
  string path = base::GetTestTempPath("input.txt");
  constexpr auto kFlags = O_CLOEXEC | O_CREAT | O_TRUNC | O_RDWR;
  unique_ptr<LinuxFile> from, to;
  string to_path = base::GetTestTempPath("output.txt");

  int pipefd[2];
  ASSERT_EQ(0, pipe(pipefd));
  int res = fcntl(pipefd[0], F_GETPIPE_SZ);
  ASSERT_GT(res, 4096);

  proactor_->Await([&] {
    auto res = uring::OpenLinux(path, kFlags, 0600);
    ASSERT_TRUE(res);
    from = std::move(res).value();
    error_code ec = from->Write(io::Buffer(string(4096, 'b')), 0, 0);
    ASSERT_FALSE(ec);

    res = uring::OpenLinux(to_path, kFlags, 0600);
    ASSERT_TRUE(res);
    to = std::move(res).value();
  });

  FiberCall::IoResult io_res = proactor_->Await([&] {
    uring::SubmitEntry se1 = proactor_->GetSubmitEntry(nullptr, 0);
    se1.PrepSplice(from->fd(), 0, pipefd[1], -1, 4096, SPLICE_F_MOVE);
    se1.sqe()->flags |= IOSQE_IO_LINK;

    FiberCall fc(proactor_.get());
    fc->PrepSplice(pipefd[0], -1, to->fd(), -1, 4096, SPLICE_F_MOVE);
    return fc.Get();
  });
  ASSERT_EQ(4096, io_res);
  proactor_->Await([&] {
    from.reset();
    to.reset();
  });
}
#endif

void BM_AsyncCall(benchmark::State& state) {
  Proactor proactor;
  std::thread t([&] {
    proactor.Init(128);
    proactor.Run();
  });

  while (state.KeepRunning()) {
    proactor.DispatchBrief([] {});
  }
  proactor.Stop();
  t.join();
}
BENCHMARK(BM_AsyncCall);

void BM_AwaitCall(benchmark::State& state) {
  Proactor proactor;
  std::thread t([&] {
    proactor.Init(128);
    proactor.Run();
  });

  while (state.KeepRunning()) {
    proactor.AwaitBrief([] {});
  }
  proactor.Stop();
  t.join();
}
BENCHMARK(BM_AwaitCall);

}  // namespace uring
}  // namespace util
