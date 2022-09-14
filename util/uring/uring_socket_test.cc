// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/uring_socket.h"

#include "base/gtest.h"
#include "base/logging.h"

namespace util {
namespace uring {

constexpr uint32_t kRingDepth = 8;
using namespace std;
namespace fibers = boost::fibers;

class UringSocketTest : public testing::Test {
 protected:
  void SetUp() final;

  void TearDown() final;

  static void SetUpTestCase() {
    testing::FLAGS_gtest_death_test_style = "threadsafe";
  }

  using IoResult = Proactor::IoResult;

  unique_ptr<Proactor> proactor_;
  thread proactor_thread_;
  unique_ptr<LinuxSocketBase> listen_socket_;
  unique_ptr<LinuxSocketBase> conn_socket_;

  uint16_t listen_port_ = 0;
  fibers::fiber accept_fb_;
  std::error_code accept_ec_;
  FiberSocketBase::endpoint_type listen_ep_;
};

void UringSocketTest::SetUp() {
  proactor_ = std::make_unique<Proactor>();
  proactor_thread_ = thread{[this] {
    proactor_->Init(kRingDepth);
    proactor_->Run();
  }};
  listen_socket_.reset(proactor_->CreateSocket());
  auto ec = listen_socket_->Listen(0, 0);
  CHECK(!ec);
  listen_port_ = listen_socket_->LocalEndpoint().port();

  auto address = boost::asio::ip::make_address("127.0.0.1");
  listen_ep_ = FiberSocketBase::endpoint_type{address, listen_port_};

  accept_fb_ = proactor_->LaunchFiber([this] {
    auto accept_res = listen_socket_->Accept();
    if (accept_res) {
      FiberSocketBase* sock = *accept_res;
      conn_socket_.reset(static_cast<LinuxSocketBase*>(sock));
      conn_socket_->SetProactor(proactor_.get());
    } else {
      accept_ec_ = accept_res.error();
    }
  });
}

void UringSocketTest::TearDown() {
  proactor_->Await([&] {
    listen_socket_->Shutdown(SHUT_RDWR);
  });

  listen_socket_.reset();
  conn_socket_.reset();
  if (accept_fb_.joinable()) {
    accept_fb_.join();
  }
  proactor_->Stop();
  proactor_thread_.join();
  proactor_.reset();
}

TEST_F(UringSocketTest, Basic) {
  unique_ptr<LinuxSocketBase> sock(proactor_->CreateSocket());

  proactor_->Await([&] {
    error_code ec = sock->Connect(listen_ep_);
    EXPECT_FALSE(ec);
    accept_fb_.join();

    ASSERT_FALSE(accept_ec_);
    uint8_t buf[16];
    auto res = sock->WriteSome(io::Bytes(buf));
    EXPECT_EQ(16, res.value_or(0));
  });
}

TEST_F(UringSocketTest, Timeout) {
  unique_ptr<LinuxSocketBase> sock[2];
  for (size_t i = 0; i < 2; ++i) {
    sock[i].reset(proactor_->CreateSocket());
    sock[i]->set_timeout(5);  // we set timeout that won't supposed to trigger.
  }

  proactor_->Await([&] {
    for (size_t i = 0; i < 2; ++i) {
      error_code ec = sock[i]->Connect(listen_ep_);
      EXPECT_FALSE(ec);
    }
  });
  accept_fb_.join();
  ASSERT_FALSE(accept_ec_);

  unique_ptr<LinuxSocketBase> tm_sock(proactor_->CreateSocket());
  tm_sock->set_timeout(5);

  error_code tm_ec = proactor_->Await([&] { return tm_sock->Connect(listen_ep_); });
  EXPECT_EQ(tm_ec, errc::operation_canceled);

  // sock[0] was accepted and then its peer was deleted.
  // therefore, we read from sock[1] that was opportunistically accepted with the ack from peer.
  uint8_t buf[16];
  io::Result<size_t> read_res = proactor_->Await([&] { return sock[1]->Recv(buf); });
  EXPECT_EQ(read_res.error(), errc::operation_canceled);
}

TEST_F(UringSocketTest, Poll) {
  unique_ptr<LinuxSocketBase> sock(proactor_->CreateSocket());
  struct linger ling;
  ling.l_onoff = 1;
  ling.l_linger = 0;

  proactor_->Await([&] {
    error_code ec = sock->Connect(listen_ep_);
    EXPECT_FALSE(ec);

    // We enforce RST event on server socket by setting linger option with timeout=0.
    // This way, client socket won't send any FIN notifications and will just disappear.
    // See https://stackoverflow.com/a/13088864/2280111
    CHECK_EQ(0, setsockopt(sock->native_handle(), SOL_SOCKET, SO_LINGER, &ling, sizeof(ling)));
  });
  accept_fb_.join();

  auto poll_cb = [](uint32_t mask) {
    LOG(INFO) << "Res: " << mask;
    EXPECT_TRUE((POLLRDHUP | POLLHUP) & mask);
    EXPECT_TRUE(POLLERR & mask);
  };

  proactor_->Await([&] {
    UringSocket* us = static_cast<UringSocket*>(this->conn_socket_.get());
    us->PollEvent(POLLHUP | POLLERR, poll_cb);
  });

  proactor_->Await([&] {
    auto ec = sock->Close();
    (void)ec;
  });
  usleep(100);
}

TEST_F(UringSocketTest, AsyncWrite) {
  unique_ptr<LinuxSocketBase> sock;
  fibers_ext::Done done;
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
}

TEST_F(UringSocketTest, UDS) {
  string path = base::GetTestTempPath("sock.uds");

  unique_ptr<LinuxSocketBase> sock;
  proactor_->Await([&] {
    sock.reset(proactor_->CreateSocket());
    error_code ec = sock->Create(AF_UNIX);
    EXPECT_FALSE(ec);
    ec = sock->ListenUDS(path.c_str(), 1);
    EXPECT_FALSE(ec);
  });
}

}  // namespace uring
}  // namespace util
