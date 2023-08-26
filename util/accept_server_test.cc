// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/accept_server.h"

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
 protected:
  void HandleRequests() final;
};

void TestConnection::HandleRequests() {
  char buf[128];
  boost::system::error_code ec;

  AsioStreamAdapter<FiberSocketBase> asa(*socket_);

  while (true) {
    asa.read_some(boost::asio::buffer(buf), ec);
    if (ec == std::errc::connection_aborted)
      break;

    CHECK(!ec) << ec << "/" << ec.message();

    asa.write_some(boost::asio::buffer(buf), ec);

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
    return new TestConnection;
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
    pp_->Stop();
  }

  static void SetUpTestCase() {
  }

  FiberSocketBase::endpoint_type ep;

  std::unique_ptr<ProactorPool> pp_;
  std::unique_ptr<AcceptServer> as_;
  std::unique_ptr<FiberSocketBase> client_sock_;
  TestListener* listener_;
};

void AcceptServerTest::SetUp() {
  const uint16_t kPort = 1234;
#if USE_URING
  ProactorPool* up = Pool::IOUring(16, 2);
#else
  ProactorPool* up = Pool::Epoll();
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
  ep = FiberSocketBase::endpoint_type{address, kPort};

  pb->Await([&] {
    FiberSocketBase::error_code ec = client_sock_->Connect(ep);
    CHECK(!ec) << ec;
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
  fs->Read(io::MutableBytes(buf));
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

  ProactorBase* pb = pp_->GetNextProactor();
  std::unique_ptr<FiberSocketBase> second_client(pb->CreateSocket());

  pb->Await([&] {
    FiberSocketBase::error_code ec = second_client->Connect(ep);
    CHECK(!ec) << ec;

    // Check that we get the error message from the server.
    uint8_t buf[200] = {0};
    second_client->Recv({buf, 200});
    EXPECT_FALSE(strcmp(reinterpret_cast<const char*>(buf), kMaxConnectionsError)) << buf;

    // The server should close the socket on its side after sending the error message,
    // but it's not trivial to test for this. What we do is to perform two dummy writes
    // and a small sleep. The writes should trigger a RST response from the closed socket
    // and after a RST response further writes should fail.
    second_client->Write(io::Buffer("test"));
    second_client->Write(io::Buffer("test"));
    ThisFiber::SleepFor(2ms);
    EXPECT_TRUE(second_client->Write(io::Buffer("test")));

    second_client->Close();
  });

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

}  // namespace util
