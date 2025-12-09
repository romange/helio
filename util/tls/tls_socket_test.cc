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
  std::unique_ptr<tls::TlsSocket> CreateClientSocket();

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
    sock->RegisterOnErrorCb([](uint32_t mask) { LOG(ERROR) << "Error mask: " << mask; });
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
      server_socket_->CancelOnErrorCb();
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

std::unique_ptr<tls::TlsSocket> TlsSocketTest::CreateClientSocket() {
  std::unique_ptr<tls::TlsSocket> client_sock;
  SSL_CTX* ssl_ctx = CreateSslCntx(CLIENT);
  proactor_->Await([&] {
    client_sock.reset(new tls::TlsSocket(proactor_->CreateSocket()));
    client_sock->InitSSL(ssl_ctx);
  });
  SSL_CTX_free(ssl_ctx);
  error_code ec = proactor_->Await([&] { return client_sock->Connect(listen_ep_); });
  if (ec) {
    ADD_FAILURE() << "Connect failed: " << ec.message();
    return nullptr;
  }
  return client_sock;
}

// Tests that a short write does not deadlock: both client and server block on ReadSome, but
// a timeout ensures the test completes even if no data is received.
TEST_P(TlsSocketTest, ShortWrite) {
  auto client_sock = CreateClientSocket();
  ASSERT_TRUE(client_sock);

  // Set a timeout to avoid deadlock on blocking ReadSome calls.
  constexpr uint32_t kTimeoutMs = 100;
  proactor_->Await([&] {
    client_sock->set_timeout(kTimeoutMs);
    server_socket_->set_timeout(kTimeoutMs);
  });

  auto client_fb = proactor_->LaunchFiber([&] {
    uint8_t buf[256];
    iovec iov{buf, sizeof(buf)};
    client_sock->ReadSome(&iov, 1);
  });

  // Server side.
  auto server_read_fb = proactor_->LaunchFiber([&] {
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

// Validates non-blocking TrySend and TryRecv for basic client-server TLS communication.
// Ensures data sent by client is received correctly by server using polling TryRecv.
TEST_P(TlsSocketTest, TryReadWrite) {
  auto client_sock{CreateClientSocket()};
  ASSERT_TRUE(client_sock);

  const std::string kData{"Hello, TrySend!"};

  // Client sends using TrySend
  auto client_fb = proactor_->LaunchFiber([&] {
    io::Bytes data(reinterpret_cast<const uint8_t*>(kData.data()), kData.size());
    auto res = client_sock->TrySend(data);
    ASSERT_TRUE(res);
    ASSERT_EQ(*res, kData.size());
  });

  // Server receives using TryRecv
  proactor_->Await([&] {
    uint8_t buf[1024];
    io::MutableBytes mb(buf, sizeof(buf));

    io::Result<size_t> res = nonstd::expected_lite::make_unexpected(
        make_error_code(std::errc::resource_unavailable_try_again));

    // Poll until we get data
    constexpr int kMaxPollAttempts = 300;
    for (int attempts_count{}; attempts_count < kMaxPollAttempts; ++attempts_count) {
      res = server_socket_->TryRecv(mb);
      if (res)
        break;
      if (res.error() != std::errc::resource_unavailable_try_again &&
          res.error() != std::errc::operation_would_block) {
        break;  // Real error
      }
      ThisFiber::SleepFor(10ms);
    }

    ASSERT_TRUE(res) << "Server TryRecv failed: " << (res ? "OK" : res.error().message());
    ASSERT_EQ(*res, kData.size());
    ASSERT_EQ(std::string((char*)buf, *res), kData);
  });

  client_fb.Join();
  proactor_->Await([&] { std::ignore = client_sock->Close(); });
}

// Validates TrySend with iovec (vectorized send) and TryRecv with partial reads.
// Ensures server can accumulate partial reads to reconstruct the full message.
TEST_P(TlsSocketTest, TrySendVector) {
  auto client_sock = CreateClientSocket();
  ASSERT_TRUE(client_sock);
  const std::string kPart1{"Hello, "};
  const std::string kPart2{"Vector!"};
  const std::string kFull{kPart1 + kPart2};

  // Client sends using TrySend with iovec
  auto client_fb = proactor_->LaunchFiber([&] {
    iovec v[2] = {{.iov_base = (void*)kPart1.data(), .iov_len = kPart1.size()},
                  {.iov_base = (void*)kPart2.data(), .iov_len = kPart2.size()}};

    auto res{client_sock->TrySend(v, 2)};
    ASSERT_TRUE(res);
    ASSERT_EQ(*res, kFull.size());
  });

  // Server receives using TryRecv, accumulate partial reads
  proactor_->Await([&] {
    uint8_t buf[1024];
    size_t total{};
    std::string result;
    constexpr int kMaxPollAttempts = 100;
    for (int attempts_count{}; attempts_count < kMaxPollAttempts; ++attempts_count) {
      io::MutableBytes mb(buf + total, sizeof(buf) - total);
      auto res = server_socket_->TryRecv(mb);
      if (res && (*res > 0)) {
        result.append((char*)(buf + total), *res);
        total += *res;
        if (total >= kFull.size())
          break;
      } else if (res && (*res == 0)) {
        break;  // EOF
      } else if (res.error() != std::errc::resource_unavailable_try_again &&
                 res.error() != std::errc::operation_would_block) {
        break;  // Real error
      }
      ThisFiber::SleepFor(10ms);
    }
    ASSERT_EQ(result, kFull);
  });

  client_fb.Join();
  proactor_->Await([&] { std::ignore = client_sock->Close(); });
}

// Validates event-driven read using RegisterOnRecv notification.
// Ensures callback only signals a fiber, which then safely performs TryRecv after send completes.
TEST_P(TlsSocketTest, RegisterOnRecv) {
  auto client_sock = CreateClientSocket();
  ASSERT_TRUE(client_sock);
  const std::string send_data{"Async Recv Data in RegisterOnRecv test case"};
  std::string received_data;
  Done data_ready;

  // Register the callback to notify a waiting fiber when data arrives.
  proactor_->DispatchBrief([&] {
    server_socket_->RegisterOnRecv(
        [&](const util::FiberSocketBase::RecvNotification&) { data_ready.Notify(); });
  });

  // Launch sender fiber to send data that will trigger the RegisterOnRecv callback.
  auto client_fb = proactor_->LaunchFiber([&] {
    io::Bytes data(reinterpret_cast<const uint8_t*>(send_data.data()), send_data.size());
    auto res = client_sock->TrySend(data);
    ASSERT_TRUE(res);
  });

  // Launch a fiber to perform the actual TryRecv and store the result.
  auto recv_fb = proactor_->LaunchFiber([&] {
    data_ready.Wait();
    uint8_t buf[1024];
    auto res = server_socket_->TryRecv(io::MutableBytes(buf, sizeof(buf)));
    if (res && (*res > 0)) {
      received_data.assign(reinterpret_cast<const char*>(buf), *res);
    }
  });

  client_fb.Join();
  recv_fb.Join();
  EXPECT_EQ(received_data, send_data);
  proactor_->Await([&] { server_socket_->ResetOnRecvHook(); });
  proactor_->Await([&] { std::ignore = client_sock->Close(); });
}

class AsyncTlsSocketTest : public testing::TestWithParam<string_view> {
 protected:
  void SetUp() final;
  void TearDown() final;

  static void SetUpTestCase() {
    testing::FLAGS_gtest_death_test_style = "threadsafe";
  }

  using IoResult = int;

  virtual void HandleRequest() {
    const size_t kBufSize = 1 << 20;
    unique_ptr<uint8_t[]> buf(new uint8_t[kBufSize]);

    auto res = tls_socket_->Recv(io::MutableBytes{buf.get(), kBufSize});
    EXPECT_TRUE(res.has_value());

    auto write_res = tls_socket_->Write(io::Bytes{buf.get(), *res});
    EXPECT_FALSE(write_res);
  }

  unique_ptr<tls::TlsSocket> CreateClientSock();

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

INSTANTIATE_TEST_SUITE_P(Engines, AsyncTlsSocketTest,
                         testing::Values("epoll"
#ifdef __linux__
                                         ,
                                         "uring"
#endif
                                         ),
                         [](const auto& info) { return string(info.param); });

void AsyncTlsSocketTest::SetUp() {
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
  std::string name("accept");
  Fiber::Opts opts{.name = name, .stack_size = 128 * 1024};

  accept_fb_ = proactor_->LaunchFiber(opts, [this] {
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
      tls_socket_ = std::make_unique<tls::TlsSocket>(conn_socket_.release());
      ssl_ctx_ = CreateSslCntx(SERVER);
      tls_socket_->InitSSL(ssl_ctx_);
      tls_socket_->Accept();
      VLOG(1) << "TLS Accept succeeded";
      conn_fb_ = proactor_->LaunchFiber([this]() { HandleRequest(); });
    } else {
      accept_ec_ = accept_res.error();
    }
  });
}

void AsyncTlsSocketTest::TearDown() {
  VLOG(1) << "TearDown";

  proactor_->Await([&] {
    std::ignore = listen_socket_->Shutdown(SHUT_RDWR);
    CHECK(!conn_socket_);
    tls_socket_->CancelOnErrorCb();
    std::ignore = tls_socket_->Close();
  });

  conn_fb_.JoinIfNeeded();
  accept_fb_.JoinIfNeeded();

  proactor_->Await([&] { std::ignore = listen_socket_->Close(); });

  proactor_->Stop();
  proactor_thread_.join();
  proactor_.reset();

  SSL_CTX_free(ssl_ctx_);
}

unique_ptr<tls::TlsSocket> AsyncTlsSocketTest::CreateClientSock() {
  unique_ptr<tls::TlsSocket> client_sock;
  {
    SSL_CTX* ssl_ctx = CreateSslCntx(CLIENT);

    proactor_->Await([&] {
      client_sock.reset(new tls::TlsSocket(proactor_->CreateSocket()));
      client_sock->InitSSL(ssl_ctx);
    });
    SSL_CTX_free(ssl_ctx);
  }
  return client_sock;
}

TEST_P(AsyncTlsSocketTest, AsyncRW) {
  unique_ptr tls_sock = CreateClientSock();

  proactor_->Await([&] {
    ThisFiber::SetName("ConnectFb");

    error_code ec = tls_sock->Connect(listen_ep_);
    EXPECT_FALSE(ec);
    uint8_t res[16];
    std::fill(std::begin(res), std::end(res), uint8_t(120));
    {
      Done done;
      iovec v{.iov_base = &res, .iov_len = 16};

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
      EXPECT_EQ(memcmp(begin(res), begin(buf), 16), 0);
    }

    VLOG(1) << "closing client sock " << tls_sock->native_handle();
    std::ignore = tls_sock->Close();
    accept_fb_.Join();
    VLOG(1) << "After join";
    ASSERT_FALSE(ec) << ec.message();
    ASSERT_FALSE(accept_ec_);
  });
}

class AsyncTlsSocketNeedWrite : public AsyncTlsSocketTest {
  virtual void HandleRequest() {
    Done done;
    // Without handling NEED_WRITE in AsyncReq::CompleteAsyncReq we would deadlock here
    uint8_t res[256];
    iovec v{.iov_base = &res, .iov_len = 256};
    tls_socket_->__DebugForceNeedWriteOnAsyncRead(&v, 1, [&](auto res) { done.Notify(); });

    done.Wait();
    VLOG(1) << "Read completed";
    done.Reset();

    tls_socket_->__DebugForceNeedWriteOnAsyncWrite(&v, 1, [&](auto res) { done.Notify(); });
    done.Wait();
    VLOG(1) << "Write completed";
  }
};

#if 0
// TODO once we fix epoll AsyncRead from blocking to nonblocking investiage why it fails on mac,
// For now also disable this on mac altogether.
#ifdef __linux__

INSTANTIATE_TEST_SUITE_P(Engines, AsyncTlsSocketNeedWrite, testing::Values("epoll", "uring"),
                         [](const auto& info) { return string(info.param); });

TEST_P(AsyncTlsSocketNeedWrite, AsyncReadNeedWrite) {
  unique_ptr tls_sock = CreateClientSock();

  proactor_->Await([&] {
    error_code ec = tls_sock->Connect(listen_ep_);
    EXPECT_FALSE(ec);
    uint8_t res[256];
    std::fill(std::begin(res), std::end(res), uint8_t(120));
    {
      VLOG(1) << "Before writesome";

      Done done;
      iovec v{.iov_base = &res, .iov_len = 256};

      for(size_t i = 0; i < 2; ++i) {
        tls_sock->AsyncWrite(&v, 1, [&](auto result) mutable {
          EXPECT_FALSE(result);
          done.Notify();
        });

        done.Wait();
        done.Reset();
      }

      VLOG(1) << "Step 2";
      // Write it again to simulate NEED_READ_AND_MAYBE_WRITE in __DebugForceNeedWriteOnAsyncWrite
      tls_sock->AsyncWrite(&v, 1, [&](auto result) mutable {
        EXPECT_FALSE(result);
        done.Notify();
      });

      done.Wait();
      done.Reset();

      VLOG(1) << "Step 3";

      uint8_t buf[256] = {1};
      v.iov_base = &buf;
      v.iov_len = 256;
      tls_sock->AsyncRead(&v, 1, [&](auto result) mutable {
        EXPECT_FALSE(result);
        VLOG(1) << "Read callback";
        done.Notify();
      });

      done.Wait();
      EXPECT_EQ(memcmp(begin(res), begin(buf), 256), 0);
    }
    VLOG(1) << "closing client sock " << tls_sock->native_handle();
    std::ignore = tls_sock->Close();
    accept_fb_.Join();
    VLOG(1) << "After join";
    ASSERT_FALSE(ec) << ec.message();
    ASSERT_FALSE(accept_ec_);
  });
}

#endif
#endif

class AsyncTlsSocketTestPartialRW : public AsyncTlsSocketTest {
 protected:
  virtual void HandleRequest() {
    uint8_t buf[kPayloadSz];
    auto res = tls_socket_->ReadAtLeast(buf, kPayloadSz);
    EXPECT_TRUE(res.has_value());
    EXPECT_TRUE(res.value() == kPayloadSz) << res.value();

    absl::Span<const uint8_t> partial_write(buf, kPayloadSz / 2);
    // We split the write to two small ones.
    auto write_res = tls_socket_->Write(partial_write);
    EXPECT_FALSE(write_res);
    write_res = tls_socket_->Write(partial_write);
    EXPECT_FALSE(write_res);
  }

  static constexpr size_t kPayloadSz = 32768;
};

INSTANTIATE_TEST_SUITE_P(Engines, AsyncTlsSocketTestPartialRW,
                         testing::Values("epoll"
#ifdef __linux__
                                         ,
                                         "uring"
#endif
                                         ),
                         [](const auto& info) { return string(info.param); });

TEST_P(AsyncTlsSocketTestPartialRW, PartialAsyncReadWrite) {
  unique_ptr tls_sock = CreateClientSock();

  std::string name = "main stack";
  Fiber::Opts opts{.name = name, .stack_size = 128 * 1024};
  proactor_->Await(
      [&] {
        ThisFiber::SetName("ConnectFb");

        LOG(INFO) << "Connecting to " << listen_ep_;
        error_code ec = tls_sock->Connect(listen_ep_);
        EXPECT_FALSE(ec);
        uint8_t res[kPayloadSz];
        std::fill(std::begin(res), std::end(res), uint8_t(120));
        {
          VLOG(1) << "Before writesome";

          Done done;
          iovec v{.iov_base = &res, .iov_len = kPayloadSz};

          tls_sock->AsyncWrite(&v, 1, [&](auto result) mutable {
            EXPECT_FALSE(result);
            done.Notify();
          });

          done.Wait();
        }
        {
          uint8_t buf[kPayloadSz];
          Done done;
          iovec v{.iov_base = &buf, .iov_len = kPayloadSz};

          tls_sock->AsyncRead(&v, 1, [&](auto result) mutable {
            EXPECT_FALSE(result);
            done.Notify();
          });

          done.Wait();

          EXPECT_EQ(memcmp(begin(res), begin(buf), kPayloadSz), 0);
        }

        VLOG(1) << "closing client sock " << tls_sock->native_handle();
        std::ignore = tls_sock->Close();
        accept_fb_.Join();
        VLOG(1) << "After join";
        ASSERT_FALSE(ec) << ec.message();
        ASSERT_FALSE(accept_ec_);
      },
      opts);
}

class AsyncTlsSocketRenegotiate : public AsyncTlsSocketTest {
  virtual void HandleRequest() {
    uint8_t buf[kPayloadSz];
    auto res = tls_socket_->ReadAtLeast(buf, kPayloadSz);
    EXPECT_TRUE(res.has_value());
    EXPECT_TRUE(res.value() == kPayloadSz) << res.value();

    absl::Span<const uint8_t> partial_write(buf, kPayloadSz / 2);
    // We split the write to two small ones.
    auto write_res = tls_socket_->Write(partial_write);
    EXPECT_FALSE(write_res);
    write_res = tls_socket_->Write(partial_write);
    EXPECT_FALSE(write_res);
  }

  tls::TlsSocket* Handle() {
    return tls_socket_.get();
  }

 public:
  static constexpr size_t kPayloadSz = 32768;
};

// TODO once we fix epoll AsyncRead from blocking to nonblocking, we should add it here as well
// For now also disable this on mac since there is no iouring on mac
#ifdef __linux__

INSTANTIATE_TEST_SUITE_P(Engines, AsyncTlsSocketRenegotiate, testing::Values("uring"),
                         [](const auto& info) { return string(info.param); });

TEST_P(AsyncTlsSocketRenegotiate, Renegotiate) {
  unique_ptr tls_sock = CreateClientSock();

  std::string name = "main stack";
  Fiber::Opts opts{.name = name, .stack_size = 128 * 1024};

  proactor_->Await(
      [&] {
        ThisFiber::SetName("ConnectFb");

        error_code ec = tls_sock->Connect(listen_ep_);
        EXPECT_FALSE(ec);

        uint8_t send_buf[kPayloadSz];
        uint8_t res[kPayloadSz];
        std::fill(std::begin(send_buf), std::end(send_buf), uint8_t(120));
        {
          Done done_read, done_write;
          iovec send_vec{.iov_base = &send_buf, .iov_len = kPayloadSz};
          iovec read_vec{.iov_base = &res, .iov_len = kPayloadSz};

          // We don't need to call ssl_renegotiate here, the first read will also negotiate the
          // protocol
          tls_sock->AsyncRead(&read_vec, 1, [&](auto result) mutable {
            EXPECT_FALSE(result);
            done_read.Notify();
          });

          // Here AsyncWrite will resume later since write_in_progress bit is set
          tls_sock->AsyncWrite(&send_vec, 1, [&](auto result) mutable {
            EXPECT_FALSE(result);
            done_write.Notify();
          });

          done_write.Wait();
          done_read.Wait();
          EXPECT_EQ(memcmp(begin(res), begin(send_buf), kPayloadSz), 0);
        }

        VLOG(1) << "closing client sock " << tls_sock->native_handle();
        std::ignore = tls_sock->Close();
        accept_fb_.Join();
        VLOG(1) << "After join";
        ASSERT_FALSE(ec) << ec.message();
        ASSERT_FALSE(accept_ec_);
      },
      opts);
}

#endif

}  // namespace fb2
}  // namespace util
