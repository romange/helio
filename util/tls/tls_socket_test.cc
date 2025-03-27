// Copyright 2025, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
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

  CHECK_EQ(1, SSL_CTX_use_PrivateKey_file(ctx, tls_key_file.c_str(), SSL_FILETYPE_PEM));
  CHECK_EQ(1, SSL_CTX_use_certificate_chain_file(ctx, tls_key_cert.c_str()));
  CHECK_EQ(1, SSL_CTX_load_verify_locations(ctx, tls_ca_cert_file.data(), nullptr));
  CHECK_EQ(1, SSL_CTX_set_cipher_list(ctx, "DEFAULT"));

  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);
  SSL_CTX_set_options(ctx, SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS);
  SSL_CTX_set_verify(ctx, mask, NULL);
  SSL_CTX_set_dh_auto(ctx, 1);
  return ctx;
}

class TlsSocketTest : public testing::TestWithParam<string_view> {
 protected:
  void SetUp() final;
  void TearDown() final;

  using IoResult = int;

  unique_ptr<ProactorBase> proactor_;
  thread proactor_thread_;
  unique_ptr<FiberSocketBase> listen_socket_;
  unique_ptr<tls::TlsSocket> server_socket_;
  SSL_CTX* ssl_ctx_;

  Fiber accept_fb_;
  std::error_code accept_ec_;
  FiberSocketBase::endpoint_type listen_ep_;
};

INSTANTIATE_TEST_SUITE_P(Engines, TlsSocketTest,
                         testing::Values("epoll"
#ifdef __linux__
                                         ,
                                         "uring"
#endif
                                         ),
                         [](const auto& info) { return string(info.param); });

void TlsSocketTest::SetUp() {
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

  proactor_thread_ = thread{[proactor] {
    InitProactor(proactor);
    proactor->Run();
  }};

  proactor_.reset(proactor);

  error_code ec = proactor_->AwaitBrief([&] {
    listen_socket_.reset(proactor_->CreateSocket());
    return listen_socket_->Listen(0, 0);
  });

  CHECK(!ec);
  listen_ep_ = listen_socket_->LocalEndpoint();

  accept_fb_ = proactor_->LaunchFiber("AcceptFb", [this] {
    auto accept_res = listen_socket_->Accept();
    CHECK(accept_res) << "Accept error: " << accept_res.error();

    FiberSocketBase* sock = *accept_res;
    VLOG(1) << "Accepted connection " << sock->native_handle();

    sock->SetProactor(proactor_.get());
    sock->RegisterOnErrorCb([this](uint32_t mask) {
      LOG(ERROR) << "Error mask: " << mask;
    });
    server_socket_ = std::make_unique<tls::TlsSocket>(sock);
    ssl_ctx_ = CreateSslCntx(SERVER);
    server_socket_->InitSSL(ssl_ctx_);
    auto tls_accept = server_socket_->Accept();
    CHECK(accept_res) << "Tls Accept error: " << accept_res.error();
  });
}

void TlsSocketTest::TearDown() {
  VLOG(1) << "TearDown";

  proactor_->Await([&] {
    std::ignore = listen_socket_->Shutdown(SHUT_RDWR);
    if (server_socket_) {
      std::ignore = server_socket_->Close();
    }
  });

  accept_fb_.JoinIfNeeded();

  proactor_->Await([&] { std::ignore = listen_socket_->Close(); });

  proactor_->Stop();
  proactor_thread_.join();
  proactor_.reset();

  SSL_CTX_free(ssl_ctx_);
}

TEST_P(TlsSocketTest, ShortWrite) {
  unique_ptr<tls::TlsSocket> client_sock;
  {
    SSL_CTX* ssl_ctx = CreateSslCntx(CLIENT);

    proactor_->Await([&] {
      client_sock.reset(new tls::TlsSocket(proactor_->CreateSocket()));
      client_sock->InitSSL(ssl_ctx);
    });
    SSL_CTX_free(ssl_ctx);
  }

  error_code ec = proactor_->Await([&] {
    LOG(INFO) << "Connecting to " << listen_ep_;
    return client_sock->Connect(listen_ep_);
  });
  ASSERT_FALSE(ec) << ec.message();

  auto client_fb = proactor_->LaunchFiber([&] {
    uint8_t buf[256];
    iovec iov{buf, sizeof(buf)};

    client_sock->ReadSome(&iov, 1);
  });

  // Server side.
  auto server_read_fb = proactor_->LaunchFiber([&] {
    // This read actually causes the fiber to flush pending writes and preempt on iouring.
    uint8_t buf[256];
    iovec iov;
    iov.iov_base = buf;
    iov.iov_len = sizeof(buf);
    server_socket_->ReadSome(&iov, 1);
  });

  auto write_res = proactor_->Await([&] {
    ThisFiber::Yield();
    uint8_t buf[16] = {0};

    VLOG(1) << "Writing to client";
    return server_socket_->Write(buf);
  });

  ASSERT_FALSE(write_res) << write_res;
  LOG(INFO) << "Finished";
  client_fb.Join();
  proactor_->Await([&] { std::ignore = client_sock->Close(); });
  server_read_fb.Join();
}

}  // namespace fb2
}  // namespace util
