// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <gmock/gmock.h>

#include <thread>

#include "base/gtest.h"
#include "base/logging.h"
#include "util/fiber_socket_base.h"
#include "util/fibers/fibers.h"
#include "util/fibers/synchronization.h"

#ifdef __linux__
#include "util/fibers/uring_proactor.h"
#include "util/fibers/uring_socket.h"
#endif
#include "util/fibers/epoll_proactor.h"

namespace util {
namespace fb2 {

constexpr uint32_t kRingDepth = 8;
using namespace testing;

#ifdef __linux__
void InitProactor(ProactorBase* p) {
  if (p->GetKind() == ProactorBase::IOURING) {
    static_cast<UringProactor*>(p)->Init(0, kRingDepth);
  } else {
    static_cast<EpollProactor*>(p)->Init(0);
  }
}
#else
void InitProactor(ProactorBase* p) {
  static_cast<EpollProactor*>(p)->Init(0);
}
#endif

using namespace std;

// Struct to combine proactor type and IP version parameters
struct TestParams {
  string_view proactor_type;
  bool use_ipv6;

  TestParams(string_view type, bool ipv6) : proactor_type(type), use_ipv6(ipv6) {
  }

  string ToString() const {
    string ip_ver = use_ipv6 ? "IPv6" : "IPv4";
    return string(proactor_type) + "_" + ip_ver;
  }
};

class FiberSocketTest : public testing::TestWithParam<TestParams> {
 protected:
  void SetUp() final;

  void TearDown() final;

  static void SetUpTestCase() {
    testing::FLAGS_gtest_death_test_style = "threadsafe";
  }

  // Return the proactor type parameter
  string_view GetProactorType() const {
    return GetParam().proactor_type;
  }

  // Return whether to use IPv6
  bool UseIPv6() const {
    return GetParam().use_ipv6;
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
  uint32_t conn_sock_err_mask_ = 0;
};

INSTANTIATE_TEST_SUITE_P(Engines, FiberSocketTest,
                         testing::Values(TestParams("epoll", false)  // epoll with IPv4
                                         ,
                                         TestParams("epoll", true)  // epoll with IPv6
#ifdef __linux__
                                         ,
                                         TestParams("uring", false)  // uring with IPv4
                                         ,
                                         TestParams("uring", true)  // uring with IPv6
#endif
                                         ),
                         [](const auto& info) { return info.param.ToString(); });

void FiberSocketTest::SetUp() {
#if __linux__
  bool use_uring = GetProactorType() == "uring";
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

  proactor_.reset(proactor);

  error_code ec = proactor_->AwaitBrief([&] {
    listen_socket_.reset(proactor_->CreateSocket());

    // For IPv6, we need to explicitly create an IPv6 socket
    if (UseIPv6()) {
      auto create_ec = listen_socket_->Create(AF_INET6);
      if (create_ec) {
        LOG(ERROR) << "Failed to create IPv6 socket: " << create_ec.message();
        return create_ec;
      }
    }

    return listen_socket_->Listen(0, 0);
  });

  CHECK(!ec);
  listen_port_ = listen_socket_->LocalEndpoint().port();
  DCHECK_GT(listen_port_, 0);

  // Use IPv4 or IPv6 address based on the parameter
  boost::asio::ip::address address;
  if (UseIPv6()) {
    address = boost::asio::ip::make_address("::1");  // IPv6 loopback
  } else {
    address = boost::asio::ip::make_address("127.0.0.1");  // IPv4 loopback
  }
  listen_ep_ = FiberSocketBase::endpoint_type{address, listen_port_};

  accept_fb_ = proactor_->LaunchFiber("AcceptFb", [this] {
    auto accept_res = listen_socket_->Accept();
    VLOG_IF(1, !accept_res) << "Accept res: " << accept_res.error();

    if (accept_res) {
      VLOG(1) << "Accepted connection " << *accept_res;
      FiberSocketBase* sock = *accept_res;
      conn_socket_.reset(sock);
      conn_socket_->SetProactor(proactor_.get());
      conn_socket_->RegisterOnErrorCb([this](uint32_t mask) {
        LOG(INFO) << "Error mask: " << mask;
        conn_sock_err_mask_ = mask;
      });
    } else {
      accept_ec_ = accept_res.error();
    }
  });
}

void FiberSocketTest::TearDown() {
  VLOG(1) << "TearDown";

  proactor_->Await([&] {
    std::ignore = listen_socket_->Shutdown(SHUT_RDWR);
    if (conn_socket_) {
      conn_socket_->CancelOnErrorCb();
      std::ignore = conn_socket_->Close();
    }
  });

  accept_fb_.JoinIfNeeded();

  // We close here because we need to wake up listening socket.
  proactor_->Await([&] { std::ignore = listen_socket_->Close(); });

  proactor_->Stop();
  proactor_thread_.join();
  proactor_.reset();
}

TEST_P(FiberSocketTest, Basic) {
  unique_ptr<FiberSocketBase> sock(proactor_->CreateSocket());

  LOG(INFO) << "Running Basic test for " << (UseIPv6() ? "IPv6" : "IPv4") << " with "
            << GetProactorType() << " proactor";
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
    std::ignore = sock->Close();
  });
}

TEST_P(FiberSocketTest, Bug363) {
  proactor_->Await([&] {
    ThisFiber::SetName("ConnectFb");
    unique_ptr<FiberSocketBase> sock(proactor_->CreateSocket());
    sock->set_timeout(3);

    std::ignore = sock->Connect(listen_ep_);
    accept_fb_.Join();

    // Explode the socket with Writes until it blocks. At this point the socket
    // should timeout;
    constexpr size_t kBufSize = 1024 * 100;
    std::unique_ptr<uint8_t[]> buf(new uint8_t[kBufSize]);
    memset(buf.get(), 0, kBufSize);
    for (int i = 0; i < 50; ++i) {
      auto res = sock->WriteSome(io::Bytes(buf.get(), kBufSize));
      if (!res) {
        EXPECT_EQ(res.error().value(), ECANCELED);
      }
    }

    std::ignore = sock->Close();
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
    std::ignore = tm_sock->Close();
    for (size_t i = 0; i < kNumSocks; ++i) {
      std::ignore = sock[i]->Close();
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

  LOG(INFO) << "Before close";
  proactor_->Await([&] { std::ignore = sock->Close(); });
  usleep(1000);

  // POLLRDHUP is linux specific
#ifdef __linux__
  EXPECT_TRUE(POLLRDHUP & conn_sock_err_mask_) << conn_sock_err_mask_;
#endif

  // POLLERR does not appear on macos.
  EXPECT_TRUE((POLLHUP | POLLERR) & conn_sock_err_mask_) << conn_sock_err_mask_;
}

TEST_P(FiberSocketTest, PollCancel) {
  unique_ptr<FiberSocketBase> sock(proactor_->CreateSocket());
  proactor_->Await([&] {
    error_code ec = sock->Connect(listen_ep_);
    EXPECT_FALSE(ec);
  });
  accept_fb_.Join();
  proactor_->Await([&] {
    conn_socket_->CancelOnErrorCb();
    std::ignore = sock->Close();
  });
  usleep(100);

  // Should not be updated due to cancellation.
  EXPECT_EQ(0, conn_sock_err_mask_);
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

  proactor_->Await([&] { std::ignore = sock->Close(); });
}

TEST_P(FiberSocketTest, UDS) {
  string path = base::GetTestTempPath("sock.uds");
  unlink(path.c_str());

  unique_ptr<FiberSocketBase> sock;
  proactor_->Await([&] {
    sock.reset(proactor_->CreateSocket());
    EXPECT_FALSE(sock->Create(AF_UNIX));
    LOG(INFO) << "Socket created " << sock->native_handle();
    mode_t permissions = 0777;

    auto ec = sock->ListenUDS(path.c_str(), permissions, 1);
    EXPECT_FALSE(ec) << ec.message();

    // Get file permissions
    struct stat file_stat;
    if (stat(path.c_str(), &file_stat) == -1) {
      ASSERT_TRUE(false) << "Unable to stat file";
    }
    mode_t file_permissions = file_stat.st_mode & (S_IRWXU | S_IRWXG | S_IRWXO);
    EXPECT_EQ(file_permissions, permissions);

    LOG(INFO) << "Socket Listening";
  });

  bool got_connection = false;
  auto uds_accept = proactor_->LaunchFiber("AcceptFb", [&sock, &got_connection] {
    auto accept_res = sock->Accept();
    EXPECT_TRUE(accept_res) << accept_res.error().message();
    auto linux_sock = dynamic_cast<LinuxSocketBase*>(*accept_res);
    EXPECT_NE(linux_sock, nullptr);
    EXPECT_TRUE(linux_sock->IsUDS());
    got_connection = true;
  });

  proactor_->Await([&] {
    sockaddr_un addr;
    addr.sun_family = AF_UNIX;

    unique_ptr<FiberSocketBase> client_sock{proactor_->CreateSocket()};
    EXPECT_FALSE(client_sock->Create(AF_UNIX));

    auto client_path = (path + "-client");
    unlink(client_path.c_str());

    strcpy(addr.sun_path, client_path.c_str());
    auto ec = client_sock->Bind((struct sockaddr*)&addr, sizeof(addr));
    EXPECT_FALSE(ec) << ec.message();

    // socket->Connect()'s interface is limited to tcp connections, so use manual connect
    strcpy(addr.sun_path, path.c_str());
    int res = connect(client_sock->native_handle(), (struct sockaddr*)&addr, sizeof(addr));
    EXPECT_EQ(res, 0) << error_code{res, system_category()}.message();

    ec = client_sock->Close();
    EXPECT_FALSE(ec) << ec.message();
  });

  uds_accept.JoinIfNeeded();
  EXPECT_TRUE(got_connection);

  proactor_->Await([&] { std::ignore = sock->Close(); });

  LOG(INFO) << "Finished";
}

TEST_P(FiberSocketTest, RecvProvided) {
  bool use_uring = GetProactorType() == "uring";
  if (!use_uring) {
    GTEST_SKIP() << "RecvProvided is supported only on uring";
    return;
  }
#ifdef __linux__
  constexpr unsigned kBufLen = 40;
  if (use_uring) {
    UringProactor* up = static_cast<UringProactor*>(proactor_.get());
    up->Await([up] { up->RegisterBufferRing(1, 4, kBufLen); });
  }
#endif

  unique_ptr<FiberSocketBase> sock;
  error_code ec;
  uint8_t buf[128];
  memset(buf, 'x', sizeof(buf));

  proactor_->Await([&] {
    sock.reset(proactor_->CreateSocket());
    ec = sock->Connect(listen_ep_);
    ASSERT_FALSE(ec);
    ec = sock->Write(io::Bytes(buf));
    ASSERT_FALSE(ec);
#ifdef __linux__
    if (use_uring) {
      reinterpret_cast<UringSocket*>(conn_socket_.get())->set_bufring_id(1);
    }
#endif
  });

  FiberSocketBase::ProvidedBuffer pbuf[8];

  unsigned res = proactor_->Await([&] {
    res = conn_socket_->RecvProvided(8, pbuf);
#ifdef __linux__
    if (use_uring) {
      bool has_more = static_cast<UringSocket*>(conn_socket_.get())->HasRecvData();
      EXPECT_TRUE(has_more);
    }
#endif
    return res;
  });

  // iouring returns a single buffer.
  ASSERT_TRUE(res > 0 && res < 8);
  proactor_->Await([&] { std::ignore = sock->Close(); });

  size_t total_size = 0;
  for (unsigned i = 0; i < res; ++i) {
    ASSERT_GT(pbuf[i].res_len, 0);
    total_size += pbuf[i].res_len;
  }

  ASSERT_LE(total_size, sizeof(buf));

  proactor_->Await([&] {
    for (unsigned i = 0; i < res; ++i) {
      conn_socket_->ReturnProvided(pbuf[i]);
    }
  });
}

#ifdef __linux__
TEST_P(FiberSocketTest, RecvMultiShot) {
  bool use_uring = GetProactorType() == "uring";
  if (!use_uring) {
    GTEST_SKIP() << "RecvMultiShot is supported only on uring";
    return;
  }

  constexpr unsigned kBufLen = 40;
  constexpr unsigned kGid = 1;
  UringProactor* up = static_cast<UringProactor*>(proactor_.get());
  up->Await([up] { up->RegisterBufferRing(kGid, 4, kBufLen); });

  unique_ptr<FiberSocketBase> sock;
  error_code ec;
  array<uint8_t, 120> buf;
  buf.fill('x');

  proactor_->Await([&] {
    sock.reset(proactor_->CreateSocket());
    ec = sock->Connect(listen_ep_);
    ASSERT_FALSE(ec);
    ec = sock->Write(io::Bytes(buf));
    ASSERT_FALSE(ec);
  });

  proactor_->Await([&] {
    static_cast<UringSocket*>(conn_socket_.get())->set_bufring_id(1);
    static_cast<UringSocket*>(conn_socket_.get())->EnableRecvMultishot();
  });

  size_t total_size = 0;
  FiberSocketBase::ProvidedBuffer pbuf[8];
  unsigned res = proactor_->Await([&] { return conn_socket_->RecvProvided(8, pbuf); });
  ASSERT_TRUE(res > 0 && res < 8);
  for (unsigned i = 0; i < res; ++i) {
    ASSERT_GT(pbuf[i].res_len, 0);
    total_size += pbuf[i].res_len;
    uint16_t bid = pbuf[i].buf_id;
    auto compare = [](const uint8_t* b, unsigned len) {
      for (unsigned i = 0; i < len; ++i) {
        ASSERT_EQ('x', b[i]);
      }
    };

    while (unsigned(pbuf[i].res_len) > kBufLen) {
      uint8_t* b = up->GetBufRingPtr(kGid, bid);
      compare(b, kBufLen);
      pbuf[i].res_len -= kBufLen;
      ++pbuf[i].buf_pos;
      bid = up->GetBufIdByPos(kGid, pbuf[i].buf_pos);
    }
    compare(up->GetBufRingPtr(kGid, bid), pbuf[i].res_len);
  }

  proactor_->Await([&] {
    for (unsigned i = 0; i < res; ++i) {
      conn_socket_->ReturnProvided(pbuf[i]);
    }
  });

  EXPECT_EQ(total_size, buf.size());

  proactor_->Await([&] { std::ignore = sock->Close(); });
  res = proactor_->Await([&] { return conn_socket_->RecvProvided(8, pbuf); });

  ASSERT_EQ(res, 1);
  ASSERT_EQ(pbuf[0].res_len, -ECONNABORTED);
}

TEST_P(FiberSocketTest, MultiShotNobuf) {
  bool use_uring = GetProactorType() == "uring";
  if (!use_uring) {
    GTEST_SKIP() << "RecvMultiShot is supported only on uring";
    return;
  }

  UringProactor* up = static_cast<UringProactor*>(proactor_.get());
  up->Await([up] { up->RegisterBufferRing(2, 4, 4); });

  unique_ptr<FiberSocketBase> sock;
  error_code ec;
  uint8_t buf[16];
  memset(buf, 'x', sizeof(buf));

  proactor_->Await([&] {
    sock.reset(proactor_->CreateSocket());
    ec = sock->Connect(listen_ep_);
    ASSERT_FALSE(ec);
    ec = sock->Write(io::Bytes(buf));
    ASSERT_FALSE(ec);
  });

  proactor_->Await([&] {
    static_cast<UringSocket*>(conn_socket_.get())->set_bufring_id(0);  // invalid bufring.
    static_cast<UringSocket*>(conn_socket_.get())->EnableRecvMultishot();
  });

  unsigned res;
  FiberSocketBase::ProvidedBuffer pbuf[8];
  proactor_->Await([&] { res = conn_socket_->RecvProvided(8, pbuf); });
  ASSERT_EQ(res, 1);

  // with 6.8 kernel we get ENOBUFS, with newer kernels we get ENOENT.
  ASSERT_THAT(pbuf[0].res_len, AnyOf(-ENOBUFS, -ENOENT));
  proactor_->Await([&] { std::ignore = sock->Close(); });
}

TEST_P(FiberSocketTest, NotEmpty) {
  bool use_uring = GetProactorType() == "uring";
  bool has_poll_first = false;

  if (use_uring) {
    has_poll_first = static_cast<UringProactor*>(proactor_.get())->HasPollFirst();
  }

  if (!has_poll_first) {
    GTEST_SKIP() << "NotEmpty test is supported only on uring with poll first";
    return;
  }

  unique_ptr<FiberSocketBase> sock;
  error_code ec;
  proactor_->Await([&] {
    sock.reset(proactor_->CreateSocket());
    ec = sock->Connect(listen_ep_);
  });
  ASSERT_FALSE(ec);
  constexpr size_t kBufSize = 8192;
  unique_ptr<uint8_t[]> buf(new uint8_t[kBufSize]);

  proactor_->Await([&] {
    ec = sock->Write(io::Bytes(buf.get(), kBufSize));
    io::Result<size_t> res = conn_socket_->Recv(io::MutableBytes(buf.get(), 16));
    ASSERT_EQ(16, res.value_or(0));
  });

  ASSERT_FALSE(ec);

  UringSocket* uring_sock = static_cast<UringSocket*>(conn_socket_.get());
  EXPECT_TRUE(uring_sock->HasRecvData());  // we have more pending data to read.

  proactor_->Await([&] { std::ignore = sock->Close(); });
}

TEST_P(FiberSocketTest, OpenMany) {
  bool use_uring = GetProactorType() == "uring";
  if (!use_uring) {
    GTEST_SKIP() << "OpenMany requires iouring";
    return;
  }

  proactor_->Await([&] {
    for (unsigned i = 0; i < 10000; ++i) {
      UringProactor* up = static_cast<UringProactor*>(proactor_.get());
      UringSocket sock(up);
      auto ec = sock.Create(AF_INET);
      ASSERT_FALSE(ec);
      ec = sock.Close();
      ASSERT_FALSE(ec);
      usleep(100);
    }
  });
}

TEST_P(FiberSocketTest, SendProvided) {
  bool use_uring = GetProactorType() == "uring";
  if (!use_uring) {
    GTEST_SKIP() << "SendProvided requires iouring";
    return;
  }

  constexpr unsigned kBufGid = 3;
  UringProactor* up = static_cast<UringProactor*>(proactor_.get());
  up->Await([up] { up->RegisterBufferRing(kBufGid, 4 /*nentries*/, 7 /*esize*/); });

  unique_ptr<FiberSocketBase> sock;
  error_code ec;
  proactor_->Await([&] {
    sock.reset(proactor_->CreateSocket());
    ec = sock->Connect(listen_ep_);
  });
  ASSERT_FALSE(ec);

  UringSocket* us = static_cast<UringSocket*>(sock.get());
  // With kernel 6.8 it produces error EOPNOTSUPP on the following call.
  proactor_->Await([&] {
    us->SendProvided(kBufGid, [](io::Result<size_t> res) {
      if (res) {
        LOG(INFO) << "SendProvided res: " << *res;
      } else {
        LOG(ERROR) << "SendProvided error: " << res.error();
      }
    });
  });
  proactor_->Await([&] { std::ignore = sock->Close(); });
}

#endif

TEST_P(FiberSocketTest, OnRecvHook) {
  unique_ptr<FiberSocketBase> sock;
  Done done;
  atomic_int recv_count = 0;

  // Create a socket and register OnRecv hook.
  proactor_->Await([&] {
    sock.reset(proactor_->CreateSocket());
    error_code ec = sock->Connect(listen_ep_);
    EXPECT_FALSE(ec);

    sock->RegisterOnRecv([&](const FiberSocketBase::RecvNotification& notif) {
      ++recv_count;
    });
  });

  // From another socket write some data to trigger the OnRecv hook.
  proactor_->Await([&] {
    uint8_t buf[16];
    memset(buf, 'y', sizeof(buf));
    error_code ec = conn_socket_->Write(io::Bytes(buf));
    EXPECT_FALSE(ec);
  });

  // Make sure the OnRecv hook was called.
  ThisFiber::SleepFor(10ms);
  EXPECT_EQ(recv_count, 1);
  proactor_->Await([&] { std::ignore = sock->Close(); });
}

TEST_P(FiberSocketTest, ShutdownWhileReading) {
  unique_ptr<FiberSocketBase> sock;
  Done d;
  Done pre_read;

  proactor_->Dispatch([&] {
    sock.reset(proactor_->CreateSocket());
    EXPECT_FALSE(sock->Connect(listen_ep_));
    accept_fb_.Join();
    std::array<char, 1> a;
    const io::MutableBytes buffer{reinterpret_cast<unsigned char*>(a.data()), a.size()};

    pre_read.Notify();
    auto ec = sock->ReadAtLeast(buffer, 1);

    EXPECT_FALSE(ec.has_value());

    const auto actual_err = ec.error();
    // TODO: can there be same categories for the two proactors
    const auto expected =
        std::error_code{ECONNABORTED, GetProactorType() == "epoll" ? std::generic_category()
                                                                   : std::system_category()};
    EXPECT_EQ(actual_err.value(), expected.value());
    EXPECT_EQ(actual_err.category(), expected.category());

    d.Notify();
  });

  // Make sure the socket is within RecvMsg
  pre_read.Wait();
  EXPECT_FALSE(d.WaitFor(100ms));

  proactor_->Await([&] {
    EXPECT_FALSE(sock->Shutdown(SHUT_RDWR));
    EXPECT_FALSE(sock->Close());
  });

  EXPECT_TRUE(d.WaitFor(100ms));
}

}  // namespace fb2
}  // namespace util
