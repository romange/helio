// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/tls/tls_socket.h"

#include <gmock/gmock.h>

#include <algorithm>
#include <thread>

#include "absl/strings/str_cat.h"
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

enum TlsContextRole { SERVER, CLIENT };

SSL_CTX* CreateSslCntx(TlsContextRole role) {
  std::string base_path = TEST_CERT_PATH;
  std::string tls_key_file = absl::StrCat(base_path, "/server-key.pem");
  std::string tls_key_cert = absl::StrCat(base_path, "/server-cert.pem");
  std::string tls_ca_cert_file = absl::StrCat(base_path, "/ca-cert.pem");

  SSL_CTX* ctx;

  if (role == TlsContextRole::SERVER) {
    ctx = SSL_CTX_new(TLS_server_method());
    // TODO init those to build on ci
  } else {
    ctx = SSL_CTX_new(TLS_client_method());
  }
  unsigned mask = SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT;

  bool res = SSL_CTX_use_PrivateKey_file(ctx, tls_key_file.c_str(), SSL_FILETYPE_PEM) != 1;
  EXPECT_FALSE(res);
  res = SSL_CTX_use_certificate_chain_file(ctx, tls_key_cert.c_str()) != 1;
  EXPECT_FALSE(res);
  res = SSL_CTX_load_verify_locations(ctx, tls_ca_cert_file.data(), nullptr) != 1;
  EXPECT_FALSE(res);
  res = 1 == SSL_CTX_set_cipher_list(ctx, "DEFAULT");
  EXPECT_TRUE(res);
  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
  SSL_CTX_set_options(ctx, SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS);
  SSL_CTX_set_verify(ctx, mask, NULL);
  SSL_CTX_set_dh_auto(ctx, 1);
  return ctx;
}

class TlsFiberSocketTest : public testing::TestWithParam<string_view> {
 protected:
  void SetUp() final;
  void TearDown() final;

  static void SetUpTestCase() {
    testing::FLAGS_gtest_death_test_style = "threadsafe";
  }

  using IoResult = int;

  // TODO clean up
  virtual void HandleRequest() {
    tls_socket_ = std::make_unique<tls::TlsSocket>(conn_socket_.release());
    ssl_ctx_ = CreateSslCntx(SERVER);
    tls_socket_->InitSSL(ssl_ctx_);
    tls_socket_->Accept();

    uint8_t buf[16];
    auto res = tls_socket_->Recv(buf);
    EXPECT_TRUE(res.has_value());
    EXPECT_TRUE(res.value() == 16);

    auto write_res = tls_socket_->Write(buf);
    EXPECT_FALSE(write_res);
  }

  unique_ptr<ProactorBase> proactor_;
  thread proactor_thread_;
  unique_ptr<FiberSocketBase> listen_socket_;
  unique_ptr<FiberSocketBase> conn_socket_;
  unique_ptr<tls::TlsSocket> tls_socket_;
  SSL_CTX* ssl_ctx_;

  uint16_t listen_port_ = 0;
  Fiber accept_fb_;
  Fiber conn_fb_;
  std::error_code accept_ec_;
  FiberSocketBase::endpoint_type listen_ep_;
  uint32_t conn_sock_err_mask_ = 0;
};

INSTANTIATE_TEST_SUITE_P(Engines, TlsFiberSocketTest,
                         testing::Values("epoll"
#ifdef __linux__
                                         ,
                                         "uring"
#endif
                                         ),
                         [](const auto& info) { return string(info.param); });

void TlsFiberSocketTest::SetUp() {
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

  proactor_.reset(proactor);

  error_code ec = proactor_->AwaitBrief([&] {
    listen_socket_.reset(proactor_->CreateSocket());
    return listen_socket_->Listen(0, 0);
  });

  CHECK(!ec);
  listen_port_ = listen_socket_->LocalEndpoint().port();
  DCHECK_GT(listen_port_, 0);

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
      conn_socket_->RegisterOnErrorCb([this](uint32_t mask) {
        LOG(INFO) << "Error mask: " << mask;
        conn_sock_err_mask_ = mask;
      });
      conn_fb_ = proactor_->LaunchFiber([this]() { HandleRequest(); });
    } else {
      accept_ec_ = accept_res.error();
    }
  });
}

void TlsFiberSocketTest::TearDown() {
  VLOG(1) << "TearDown";

  proactor_->Await([&] {
    std::ignore = listen_socket_->Shutdown(SHUT_RDWR);
    if (conn_socket_) {
      std::ignore = conn_socket_->Close();
    } else {
      std::ignore = tls_socket_->Close();
    }
  });

  conn_fb_.JoinIfNeeded();
  accept_fb_.JoinIfNeeded();

  // We close here because we need to wake up listening socket.
  proactor_->Await([&] { std::ignore = listen_socket_->Close(); });

  proactor_->Stop();
  proactor_thread_.join();
  proactor_.reset();

  SSL_CTX_free(ssl_ctx_);
}

TEST_P(TlsFiberSocketTest, Basic) {
  unique_ptr tls_sock = std::make_unique<tls::TlsSocket>(proactor_->CreateSocket());
  SSL_CTX* ssl_ctx = CreateSslCntx(CLIENT);
  tls_sock->InitSSL(ssl_ctx);

  LOG(INFO) << "before wait ";
  proactor_->Await([&] {
    ThisFiber::SetName("ConnectFb");

    LOG(INFO) << "Connecting to " << listen_ep_;
    error_code ec = tls_sock->Connect(listen_ep_);
    EXPECT_FALSE(ec);
    {
      uint8_t buf[16];
      std::fill(std::begin(buf), std::end(buf), uint8_t(120));
      VLOG(1) << "Before writesome";

      Done done;
      iovec v{.iov_base = &buf, .iov_len = 16};

      tls_sock->AsyncWriteSome(&v, 1, [done](auto result) mutable {
        EXPECT_TRUE(result.has_value());
        EXPECT_EQ(*result, 16);
        done.Notify();
      });

      done.Wait();
    }
    {
      uint8_t buf[16];
      Done done;
      iovec v{.iov_base = &buf, .iov_len = 16};
      tls_sock->AsyncReadSome(&v, 1, [done](auto result) mutable {
        EXPECT_TRUE(result.has_value());
        EXPECT_EQ(*result, 16);
        done.Notify();
      });

      done.Wait();

      for (uint8_t c : buf) {
        EXPECT_EQ(c, 120);
      }
    }

    VLOG(1) << "closing client sock " << tls_sock->native_handle();
    std::ignore = tls_sock->Close();
    accept_fb_.Join();
    VLOG(1) << "After join";
    ASSERT_FALSE(ec) << ec.message();
    ASSERT_FALSE(accept_ec_);
  });
  SSL_CTX_free(ssl_ctx);
}

}  // namespace fb2
}  // namespace util
