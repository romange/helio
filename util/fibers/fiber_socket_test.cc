// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <thread>

#include "base/gtest.h"
#include "base/logging.h"
#include "util/fiber_socket_base.h"
#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"

#ifdef __linux__
#include "util/fibers/uring_proactor.h"
#endif
#include "util/fibers/epoll_proactor.h"

namespace util {
namespace fb2 {

constexpr uint32_t kRingDepth = 8;

#ifdef __linux__
void InitProactor(ProactorBase* p) {
  if (p->GetKind() == ProactorBase::IOURING) {
    static_cast<UringProactor*>(p)->Init(kRingDepth);
  } else {
    static_cast<EpollProactor*>(p)->Init();
  }
}
#else
void InitProactor(ProactorBase* p) {
  static_cast<EpollProactor*>(p)->Init();
}
#endif

using namespace std;

class FiberSocketTest : public testing::TestWithParam<string_view> {
 protected:
  void SetUp() final;

  void TearDown() final;

  static void SetUpTestCase() {
    testing::FLAGS_gtest_death_test_style = "threadsafe";
  }

  using IoResult = int;

  unique_ptr<ProactorBase> proactor_;
  thread proactor_thread_;
  unique_ptr<FiberSocketBase> listen_socket_;
  unique_ptr<FiberSocketBase> conn_socket_;

  uint16_t listen_port_ = 0;
  Fiber accept_fb_;
  std::error_code accept_ec_;
  FiberSocketBase::endpoint_type listen_ep_;
};

INSTANTIATE_TEST_SUITE_P(Engines, FiberSocketTest,
                         testing::Values("epoll"
#ifdef __linux__
                                         ,
                                         "uring"
#endif
                                         ));

void FiberSocketTest::SetUp() {
#if __linux__
  bool use_uring = GetParam() == "uring";
  ProactorBase* proactor = nullptr;
  if (use_uring)
    proactor = new UringProactor;
  else
    proactor = new EpollProactor;
#else
  ProactorBase* proactor = new EpollProactor;
#endif

  atomic_bool init_done{false};

  proactor_thread_ = thread{[proactor, &init_done] {
    InitProactor(proactor);
    init_done.store(true, memory_order_release);
    proactor->Run();
  }};

  // hack to wait until proactor thread crosses init.
  while (!init_done.load()) {
    usleep(1000);
  }
  proactor_.reset(proactor);

  listen_socket_.reset(proactor_->CreateSocket());
  auto ec = listen_socket_->Listen(0, 0);
  CHECK(!ec);
  listen_port_ = listen_socket_->LocalEndpoint().port();

  auto address = boost::asio::ip::make_address("127.0.0.1");
  listen_ep_ = FiberSocketBase::endpoint_type{address, listen_port_};

  accept_fb_ = proactor_->LaunchFiber("AcceptFb", [this] {
    auto accept_res = listen_socket_->Accept();
    VLOG_IF(1, !accept_res) << "Accept res: " << accept_res.error();

    if (accept_res) {
      VLOG(1) << "Accepted connection " << *accept_res;
      FiberSocketBase* sock = *accept_res;
      conn_socket_.reset(sock);
      conn_socket_->SetProactor(proactor_.get());
    } else {
      accept_ec_ = accept_res.error();
    }
  });
}

void FiberSocketTest::TearDown() {
  VLOG(1) << "TearDown";

  proactor_->Await([&] {
    listen_socket_->Shutdown(SHUT_RDWR);
    if (conn_socket_)
      (void)conn_socket_->Close();
  });

  accept_fb_.JoinIfNeeded();

  // We close here because we need to wake up listening socket.
  proactor_->Await([&] { (void)listen_socket_->Close(); });

  proactor_->Stop();
  proactor_thread_.join();
  proactor_.reset();
}

TEST_P(FiberSocketTest, Basic) {
  unique_ptr<FiberSocketBase> sock(proactor_->CreateSocket());

  LOG(INFO) << "before wait ";
  proactor_->Await([&] {
    ThisFiber::SetName("ConnectFb");

    LOG(INFO) << "Connecting to " << listen_ep_;
    error_code ec = sock->Connect(listen_ep_);
    accept_fb_.Join();
    VLOG(1) << "After join";
    ASSERT_FALSE(ec) << ec.message();
    ASSERT_FALSE(accept_ec_);
    uint8_t buf[16];
    VLOG(1) << "Before writesome";
    auto res = sock->WriteSome(io::Bytes(buf));
    EXPECT_EQ(16, res.value_or(0));
    VLOG(1) << "closing client sock " << sock->native_handle();
    (void)sock->Close();
  });
}

TEST_P(FiberSocketTest, Timeout) {
#ifdef __APPLE__
    GTEST_SKIP() << "Skipped FiberSocketTest.Timeout test on MacOS";
    return;
#endif
  constexpr unsigned kNumSocks = 2;

  unique_ptr<FiberSocketBase> sock[kNumSocks];
  for (size_t i = 0; i < kNumSocks; ++i) {
    sock[i].reset(proactor_->CreateSocket());
    sock[i]->set_timeout(5);  // we set timeout that won't supposed to trigger.
  }

  proactor_->Await([&] {
    for (size_t i = 0; i < kNumSocks; ++i) {
      error_code ec = sock[i]->Connect(listen_ep_);
      EXPECT_FALSE(ec);
      ThisFiber::SleepFor(5ms);
    }
  });
  accept_fb_.Join();
  ASSERT_FALSE(accept_ec_);

  LOG(INFO) << "creating timedout socket";
  unique_ptr<FiberSocketBase> tm_sock(proactor_->CreateSocket());
  tm_sock->set_timeout(5);

  error_code tm_ec = proactor_->Await([&] { return tm_sock->Connect(listen_ep_); });

  // sock[0] was accepted and then its peer was deleted.
  // therefore, we read from sock[1] that was opportunistically accepted with the ack from peer.
  uint8_t buf[16];
  io::Result<size_t> read_res = proactor_->Await([&] {
    auto res = sock[1]->Recv(buf, 0);
    (void)tm_sock->Close();
    for (size_t i = 0; i < kNumSocks; ++i) {
      (void)sock[i]->Close();
    }
    return res;
  });

  // In freebsd, we get connection_aborted (EV_EOF) immediately instead of timeout.
  ASSERT_TRUE(tm_ec == errc::operation_canceled || tm_ec == errc::connection_aborted ||
              tm_ec == errc::connection_reset)
      << tm_ec;
  EXPECT_EQ(read_res.error(), errc::operation_canceled);
}

TEST_P(FiberSocketTest, Poll) {
  unique_ptr<FiberSocketBase> sock(proactor_->CreateSocket());
  struct linger ling;
  ling.l_onoff = 1;
  ling.l_linger = 0;
  LOG(INFO) << "Before connect";

  proactor_->Await([&] {
    error_code ec = sock->Connect(listen_ep_);
    EXPECT_FALSE(ec);

    // We enforce RST event on server socket by setting linger option with timeout=0.
    // This way, client socket won't send any FIN notifications and will just disappear.
    // See https://stackoverflow.com/a/13088864/2280111
    CHECK_EQ(0, setsockopt(sock->native_handle(), SOL_SOCKET, SO_LINGER, &ling, sizeof(ling)));
  });
  accept_fb_.Join();

  auto poll_cb = [](uint32_t mask) {
    LOG(INFO) << "Res: " << mask;
    // POLLRDHUP is linux specific
    #ifdef __linux__
    EXPECT_TRUE((POLLRDHUP) & mask);
    #endif

    EXPECT_TRUE((POLLHUP) & mask);
    EXPECT_TRUE(POLLERR & mask);
  };

  proactor_->Await([&] { conn_socket_->PollEvent(POLLHUP | POLLERR, poll_cb); });
  LOG(INFO) << "Before close";
  proactor_->Await([&] {
    auto ec = sock->Close();
    (void)ec;
  });
  usleep(100);
}

TEST_P(FiberSocketTest, AsyncWrite) {
  unique_ptr<FiberSocketBase> sock;
  Done done;
  proactor_->Dispatch([&] {
    sock.reset(proactor_->CreateSocket());
    error_code ec = sock->Connect(listen_ep_);
    EXPECT_FALSE(ec);

    static char buf[] = "HELLO";
    sock->AsyncWrite(io::Buffer(buf), [done](error_code ec) mutable {
      EXPECT_FALSE(ec) << ec.message();
      done.Notify();
    });
  });
  done.Wait();

  proactor_->Await([&] { (void)sock->Close(); });
}

TEST_P(FiberSocketTest, UDS) {
  string path = base::GetTestTempPath("sock.uds");

  unique_ptr<FiberSocketBase> sock;
  proactor_->Await([&] {
    sock.reset(proactor_->CreateSocket());
    error_code ec = sock->Create(AF_UNIX);
    EXPECT_FALSE(ec);
    LOG(INFO) << "Socket created " << sock->native_handle();
    mode_t permissions = 0777;
    ec = sock->ListenUDS(path.c_str(), permissions, 1);
    EXPECT_FALSE(ec) << ec.message();

    // Get file permissions
    struct stat file_stat;
    if (stat(path.c_str(), &file_stat) == -1) {
      ASSERT_TRUE(false) << "Unable to stat file";
    }
    mode_t file_permissions = file_stat.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO);
    EXPECT_EQ(file_permissions, permissions);

    LOG(INFO) << "Socket Listening";
    unlink(path.c_str());
    (void)sock->Close();
  });

  LOG(INFO) << "Finished";
}

}  // namespace fb2
}  // namespace util
