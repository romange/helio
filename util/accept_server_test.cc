// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/accept_server.h"

#include <thread>

#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>

#include "base/gtest.h"
#include "base/logging.h"
#include "util/asio_stream_adapter.h"
#include "util/fibers/pool.h"
#include "util/listener_interface.h"

namespace util {

using namespace std;
namespace h2 = boost::beast::http;
using fb2::Pool;

#ifdef __linux__
#define USE_URING 1
#else
#define USE_URING 0
#endif

class TestConnection : public Connection {
 public:
  TestConnection(ProactorPool* pp) : pp_(pp) {
  }

  unsigned migrations = 0;

 protected:
  void HandleRequests() final;

 private:
  ProactorPool* pp_;
};

void TestConnection::HandleRequests() {
  ThisFiber::SetName("ServerConnection");

  char buf[128];
  boost::system::error_code ec;

  AsioStreamAdapter<FiberSocketBase> asa(*socket_);

  while (true) {
    size_t res = asa.read_some(boost::asio::buffer(buf), ec);
    if (ec == std::errc::connection_aborted)
      break;

    CHECK(!ec) << ec << "/" << ec.message();
    string_view sv(buf, res);
    if (sv == "migrate") {
      ++migrations;
      ProactorBase* other = (pp_->at(0) == socket_->proactor()) ? pp_->at(1) : pp_->at(0);
      CHECK(socket_->proactor() != other);
      listener()->Migrate(this, other);
      CHECK(socket_->proactor() == other);
    }
    asa.write_some(boost::asio::buffer(buf, res), ec);

    if (FiberSocketBase::IsConnClosed(ec))
      break;

    CHECK(!ec);
  }
  VLOG(1) << "TestConnection exit";
}

const char* kMaxConnectionsError = "max connections received";

class TestListener : public ListenerInterface {
 public:
  virtual Connection* NewConnection(ProactorBase* context) final {
    return new TestConnection(pool());
  }

  virtual void OnMaxConnectionsReached(FiberSocketBase* sock) override {
    sock->Write(io::Buffer(kMaxConnectionsError));
  }
};

class AcceptServerTest : public testing::Test {
 protected:
  void SetUp() override;

  void TearDown() override {
    client_sock_->proactor()->Await([&] { client_sock_->Close(); });
    as_->Stop(true);
    watchdog_done_.Notify();
    watchdog_fiber_.Join();
    pp_->Stop();
  }

  static void SetUpTestCase() {
  }

  FiberSocketBase::endpoint_type ep_;

  std::unique_ptr<ProactorPool> pp_;
  std::unique_ptr<AcceptServer> as_;
  std::unique_ptr<FiberSocketBase> client_sock_;
  TestListener* listener_;
  fb2::Fiber watchdog_fiber_;
  fb2::Done watchdog_done_;
};

void AcceptServerTest::SetUp() {
  const uint16_t kPort = 1234;
#if USE_URING
  ProactorPool* up = Pool::IOUring(16, 2);
#else
  ProactorPool* up = Pool::Epoll(2);
#endif
  pp_.reset(up);
  pp_->Run();

  as_.reset(new AcceptServer{up});
  listener_ = new TestListener();
  auto ec = as_->AddListener("localhost", kPort, listener_);
  CHECK(!ec) << ec;
  as_->Run();

  ProactorBase* pb = pp_->GetNextProactor();
  client_sock_.reset(pb->CreateSocket());
  auto address = boost::asio::ip::make_address("127.0.0.1");
  ep_ = FiberSocketBase::endpoint_type{address, kPort};

  pb->Await([&] {
    ThisFiber::SetName("ClientConnect");
    FiberSocketBase::error_code ec = client_sock_->Connect(ep_);
    CHECK(!ec) << ec.message();
  });

  watchdog_fiber_ = pp_->GetNextProactor()->LaunchFiber([this] {
    ThisFiber::SetName("Watchdog");

    if (!watchdog_done_.WaitFor(10s)) {
      LOG(ERROR) << "Deadlock detected!!!!";

      fb2::Mutex m;
      pp_->AwaitFiberOnAll([&m](unsigned index, ProactorBase* base) {
        std::unique_lock lk(m);
        LOG(ERROR) << "Proactor ------------------------" << index << " ---------------:\n";
        fb2::detail::FiberInterface::PrintAllFiberStackTraces();
        LOG(ERROR) << "Proactor ------------------------" << index << " end---------------\n";
      });
      LOG(FATAL) << "Deadlock detected!!!!";
    }
  });
}

void RunClient(FiberSocketBase* fs) {
  LOG(INFO) << ": Ping-client started";
  AsioStreamAdapter<> asa(*fs);

  ASSERT_TRUE(fs->IsOpen());

  h2::request<h2::string_body> req(h2::verb::get, "/", 11);
  req.body().assign("foo");
  req.prepare_payload();
  h2::write(asa, req);
  uint8_t buf[128];
  fs->Recv(io::MutableBytes(buf));
  LOG(INFO) << ": echo-client stopped";
}

TEST_F(AcceptServerTest, Basic) {
  client_sock_->proactor()->Await([&] { RunClient(client_sock_.get()); });
}

TEST_F(AcceptServerTest, Break) {
  usleep(1000);
}

TEST_F(AcceptServerTest, ConnectionsLimit) {
  listener_->SetMaxClients((1 << 16) - 1);
  ASSERT_EQ(listener_->GetMaxClients(), (1 << 16) - 1);
  listener_->SetMaxClients(1);
  ASSERT_EQ(listener_->GetMaxClients(), 1);
  this_thread::sleep_for(10ms);

#if 0  // this test does not work with direct-fd on kernels < 6.4
  std::thread t1([this] {
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    ASSERT_GE(fd, 0);

    const int val = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &val, sizeof(val));

    struct sockaddr_in addr;
    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_port = htons(this->ep.port());
    addr.sin_addr.s_addr = INADDR_ANY;

    int res = connect(fd, (struct sockaddr*)&addr, sizeof(addr));
    ASSERT_EQ(res, 0);

    char buf[128];
    res = recv(fd, buf, sizeof(buf), 0);
    ASSERT_EQ(24, res);  // error message
    ASSERT_EQ(0, strcmp(buf, kMaxConnectionsError));
    res = send(fd, buf, sizeof(buf), MSG_NOSIGNAL);
    ASSERT_EQ(sizeof(buf), res);
    LOG(INFO) << "before receive";
    res = recv(fd, buf, sizeof(buf), MSG_NOSIGNAL);  // is stuck on < 6.4 kernels
    LOG(INFO) << "after receive";
    ASSERT_EQ(-1, res);
    close(fd);
  });
  t1.join();
#endif
  listener_->SetMaxClients((1 << 16) - 1);
  ASSERT_EQ(listener_->GetMaxClients(), (1 << 16) - 1);
}

TEST_F(AcceptServerTest, UDS) {
#ifdef __APPLE__
  GTEST_SKIP() << "Skipped AcceptServerTest.UDS test on MacOS";
  return;
#endif
  AcceptServer as{pp_.get(), false};
  const char kSockPath[] = "/tmp/uds.sock";
  unlink(kSockPath);
  auto ec = as.AddUDSListener(kSockPath, 0700, new TestListener);
  ASSERT_FALSE(ec) << ec;
  as.Run();
  as.Stop(true);
}

TEST_F(AcceptServerTest, Migrate) {
  // Make sure the connection is active.
  client_sock_->proactor()->Await([&] { RunClient(client_sock_.get()); });

  // Find the server connection.
  vector<pair<unsigned, Connection*>> conns;
  listener_->TraverseConnections([&](unsigned tid, Connection* c) { conns.emplace_back(tid, c); });
  ASSERT_EQ(1, conns.size());
  TestConnection* conn = (TestConnection*)conns[0].second;
  ASSERT_EQ(0, conn->migrations);

  uint8_t buf[32];

  LOG(INFO) << "Before migration loop";

  client_sock_->proactor()->Await([&] {
    ThisFiber::SetName("Client");
    for (unsigned i = 0; i < 200; ++i) {
      auto ec = client_sock_->Write(io::Buffer("migrate"));
      ASSERT_TRUE(!ec);
      VLOG(1) << "Write done " << i;
      auto res = client_sock_->Read(io::MutableBytes(buf, 7));
      ASSERT_TRUE(res && *res == 7);
      VLOG(1) << "Read done " << i;
    }
  });
  ASSERT_EQ(200, conn->migrations);
}

TEST_F(AcceptServerTest, Shutdown) {
  listener_->SetMaxClients(1 << 16);
  auto* proactor = client_sock_->proactor();


  proactor->Await([proactor, this] {
#ifdef __linux__
  constexpr auto kHupMask = POLLHUP | POLLRDHUP;
#else
  constexpr auto kHupMask = POLLHUP;
#endif

    vector<unique_ptr<FiberSocketBase>> socks;
    constexpr unsigned kNumSocks = 2000;
    unsigned called = 0;
    fb2::CondVarAny cond;

    for (unsigned i = 0; i < kNumSocks; ++i) {
      socks.emplace_back(proactor->CreateSocket());
      error_code ec = socks.back()->Connect(ep_);
      EXPECT_FALSE(ec);

      socks.back()->RegisterOnErrorCb([&](int err) {
        EXPECT_EQ(err, kHupMask);
        ++called;
        cond.notify_one();
      });
    }

    for (unsigned i = 0; i < socks.size(); ++i) {
      std::ignore = socks[i]->Shutdown(SHUT_RDWR);
    }
    fb2::NoOpLock lock;
    ASSERT_TRUE(cond.wait_for(lock, 1s, [&] {return called == kNumSocks;}));

    // Please note that in perfect conditions like with this unit test,
    // we will get ErrorCb trigerred for every socket because both the server and the client
    // exchange FIN packets. With real world scenarios it's not guaranteed that Shutdown
    // will trigger POLLHUP delivery, because it depends on the other size sending FIN as well.
    // See https://man7.org/linux/man-pages/man2/poll.2.html for more details.
    EXPECT_GE(called, socks.size());
    for (unsigned i = 0; i < socks.size(); ++i) {
      std::ignore = socks[i]->Close();
    }
  });
}

}  // namespace util
