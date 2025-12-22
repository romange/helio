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
#include "util/tls/tls_test_infra.h"

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
  void CreateClientSocket(std::unique_ptr<tls::TlsSocket>& client_sock);

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
// Uses output parameter to allow assertions inside the function..
void TlsSocketTest::CreateClientSocket(std::unique_ptr<tls::TlsSocket>& client_sock) {
  SSL_CTX* ssl_ctx = CreateSslCntx(CLIENT);
  proactor_->Await([&] {
    client_sock.reset(new tls::TlsSocket(proactor_->CreateSocket()));
    client_sock->InitSSL(ssl_ctx);
  });
  SSL_CTX_free(ssl_ctx);
  error_code ec = proactor_->Await([&] { return client_sock->Connect(listen_ep_); });
  ASSERT_FALSE(ec) << "Connect failed: " << ec.message();
  ASSERT_TRUE(client_sock) << "Client socket is null after connect";
}

TEST_P(TlsSocketTest, ShortWrite) {
  std::unique_ptr<tls::TlsSocket> client_sock;
  CreateClientSocket(client_sock);

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

// Validates TryRecv with partial reads after sending a vector.
// Ensures server can accumulate partial reads to reconstruct the full message.
TEST_P(TlsSocketTest, TryRecvVector) {
  std::unique_ptr<tls::TlsSocket> client_sock;
  CreateClientSocket(client_sock);
  const std::string kPart1{"Hello, "};
  const std::string kPart2{"Vector!"};
  const std::string kFull{kPart1 + kPart2};

  // Client sends using TrySend with iovec
  auto client_fb = proactor_->LaunchFiber([&] {
    iovec v[2] = {{.iov_base = (void*)kPart1.data(), .iov_len = kPart1.size()},
                  {.iov_base = (void*)kPart2.data(), .iov_len = kPart2.size()}};
    auto ec = client_sock->Write(v, 2);
    ASSERT_FALSE(ec) << ec.message();
  });

  // Server receives using TryRecv, accumulate partial reads
  proactor_->Await([&] {
    uint8_t buf[1024];
    size_t total_bytes_received{};
    std::string result;
    constexpr int kMaxPollAttempts = 1000;
    int attempts_count{};

    for (attempts_count = 0; attempts_count < kMaxPollAttempts; ++attempts_count) {
      io::MutableBytes current_buf(buf + total_bytes_received, sizeof(buf) - total_bytes_received);
      auto res = server_socket_->TryRecv(current_buf);

      if (res && (*res > 0)) {
        result.append((char*)(buf + total_bytes_received), *res);
        total_bytes_received += *res;

        if (total_bytes_received >= kFull.size()) {
          break;  // Data fully received
        }
      } else if (res && (*res == 0)) {
        break;  // EOF
      } else if (res.error() != std::errc::resource_unavailable_try_again &&
                 res.error() != std::errc::operation_would_block) {
        // Real error occurred (not EAGAIN/EWOULDBLOCK)
        break;
      }

      // We must back off when receiving EAGAIN
      ThisFiber::SleepFor(10ms);
    }

    // Explicitly assert that we didn't time out.
    ASSERT_LT(attempts_count, kMaxPollAttempts)
        << "Test timed out waiting for all data after " << attempts_count << " attempts.";
    // Check the accumulated result against the expected value.
    ASSERT_EQ(result, kFull);
  });

  client_fb.Join();
  proactor_->Await([&] { std::ignore = client_sock->Close(); });
}

// Validates event-driven read using RegisterOnRecv notification.
// Ensures callback only signals a fiber, which then safely performs TryRecv after send completes.
TEST_P(TlsSocketTest, RegisterOnRecv) {
  std::unique_ptr<tls::TlsSocket> client_sock;
  CreateClientSocket(client_sock);
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
    auto ec = client_sock->Write(data);
    ASSERT_FALSE(ec) << ec.message();
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

namespace util::tls {

// Mock Fixture
class MockTlsSocketTest : public testing::TestWithParam<std::string> {
 protected:
  void SetUp() override {
    // Standard Proactor setup (Similar to TlsSocketTest but without Listen/Accept)
#ifdef __linux__
    bool use_uring = GetParam() == "uring";
    if (use_uring) {
      proactor_ = std::make_unique<fb2::UringProactor>();
    } else {
      proactor_ = std::make_unique<fb2::EpollProactor>();
    }
#else
    proactor_ = std::make_unique<fb2::EpollProactor>();
#endif

    proactor_thread_ = std::thread{[this] {
      InitProactor(proactor_.get());
      proactor_->Run();
    }};

    proactor_->Await([&] {
      // Create a dummy underlying socket
      auto* fd = proactor_->CreateSocket();
      sock_ = std::make_unique<TlsSocket>(fd);

      // Inject the Mock
      // We use testing::NiceMock as the most permissive mock wrapper: it silently ignores all
      // unexpected/uninteresting method calls made by TlsSocket (the "noise"). This allows us to
      // selectively and strictly set EXPECT_CALLs only for the core interactions (Read, Write,
      // Handshake) relevant to our unit test logic.
      auto mock_ptr = std::make_unique<testing::NiceMock<MockEngine>>();
      mock_engine_ = mock_ptr.get();
      TestDelegator::SetEngine(sock_.get(), std::move(mock_ptr));
    });
  }
  SSL* GetSsl() {
    return mock_engine_->native_handle();
  }
  void TearDown() override {
    proactor_->Await([&] {
      if (sock_) {
        std::ignore = sock_->Close();
      }
    });

    proactor_->Stop();
    if (proactor_thread_.joinable()) {
      proactor_thread_.join();
    }
  }

  std::unique_ptr<fb2::ProactorBase> proactor_;
  std::thread proactor_thread_;

  // The object under test
  std::unique_ptr<TlsSocket> sock_;

  // Pointer to the mock (owned by sock_)
  MockEngine* mock_engine_{};
};

// TryRecvErrorTest Scenario Configuration
//
// This struct defines the inputs and expectations for a single unit test case
// of TlsSocket::TryRecv. It models the interaction between three components:
//   1. The TlsSocket's internal state (e.g., write conflict flags).
//   2. The TLS Engine's requirements (e.g., NEED_WRITE, decrypted data).
//   3. The Underlying Socket's behavior (e.g., EAGAIN, partial write, EOF).
struct TryRecvScenario {
  std::string test_name;

  // Initial Conditions
  uint32_t initial_state{};  // e.g., TlsSocket::WRITE_IN_PROGRESS

  // Engine Behavior
  Engine::OpResult engine_read_ret;  // What engine_->Read() returns
  size_t engine_output_pending{};    // What engine_->OutputPending() returns

  // Underlying Socket Behavior (Optional)
  // If set, EXPECT_CALL will be set on next_sock_->TrySend()
  std::optional<io::Result<size_t>> sock_send_ret;

  // If set, EXPECT_CALL will be set on next_sock_->TryRecv()
  std::optional<io::Result<size_t>> sock_recv_ret;

  // Expected Outcome
  std::error_code expected_error;  // Expected error from TlsSocket::TryRecv
  size_t expected_bytes{};         // Expected success bytes (usually 0 for errors)
};

// Pretty printer for GTest to show readable test names
std::ostream& operator<<(std::ostream& os, const TryRecvScenario& p) {
  return os << p.test_name;
}

INSTANTIATE_TEST_SUITE_P(MockEngines, MockTlsSocketTest,
                         testing::Values("epoll"
#ifdef __linux__
                                         ,
                                         "uring"
#endif
                                         ),
                         [](const auto& info) { return std::string(info.param); });

// basic mock Test to see MockTlsSocketTest is functional
TEST_P(MockTlsSocketTest, BasicWrite) {
  proactor_->Await([&] {
    uint8_t buf[] = "hello";

    // Expect (Register) the MockEngine to receive a Write call and return 5
    // For those new to gmock, here we only expect, actual enforcement is done after the socket
    // write.
    EXPECT_CALL(*mock_engine_, Write(testing::_)).WillOnce(testing::Return(Engine::OpResult{5}));

    auto res = sock_->Write(io::Bytes(buf, 5));
    ASSERT_FALSE(res);  // assert no error
  });
}

// Another basic mock to ensure strict ordering works as well for MockTlsSocketTest (using
// InSequence)
TEST_P(MockTlsSocketTest, HandshakeThenWrite) {
  proactor_->Await([&] {
    testing::InSequence s;
    uint8_t buf[] = "app data";

    // Expectation 0: If VLOG(1) is on, Accept() calls native_handle().
    //    We make sure it returns the valid internal SSL object we created.
    EXPECT_CALL(*mock_engine_, native_handle()).WillRepeatedly(testing::Return(GetSsl()));

    // Expectation 1: Handshake return success
    EXPECT_CALL(*mock_engine_, Handshake(testing::_))
        .WillOnce(testing::Return(Engine::OpResult{1}));  // Return success (no error code)

    // Expectation 2: Write returns full buffer size
    EXPECT_CALL(*mock_engine_, Write(testing::_))
        .WillOnce(testing::Return(Engine::OpResult{sizeof(buf)}));

    // Execution / Validation
    ASSERT_TRUE(sock_->Accept());  // This calls engine_->Handshake(),
    ASSERT_FALSE(sock_->Write(io::Bytes(buf, sizeof(buf))));
  });
}

class TryRecvErrorTest : public testing::TestWithParam<TryRecvScenario> {
 protected:
  void SetUp() override {
    proactor_ = std::make_unique<fb2::EpollProactor>();
    proactor_thread_ = std::thread{[this] {
      fb2::InitProactor(proactor_.get());
      proactor_->Run();
    }};

    proactor_->Await([&] {
      // 1. Create Mock Underlying Socket
      auto mock_sock = std::make_unique<testing::NiceMock<MockFiberSocket>>();
      mock_next_sock_ = mock_sock.get();

      // 2. Create TlsSocket wrapping the mock
      sock_ = std::make_unique<TlsSocket>(std::move(mock_sock));

      // 3. Inject Mock Engine
      auto mock_eng = std::make_unique<testing::NiceMock<MockEngine>>();
      mock_engine_ = mock_eng.get();
      TestDelegator::SetEngine(sock_.get(), std::move(mock_eng));
    });
  }

  void TearDown() override {
    proactor_->Await([&] {
      // State check: ensure we didn't leak internal flags in error paths
      // (Optional sanity check, though some errors might legitimately leave flags)
    });

    proactor_->Stop();
    if (proactor_thread_.joinable()) {
      proactor_thread_.join();
    }
  }

  std::unique_ptr<fb2::ProactorBase> proactor_;
  std::thread proactor_thread_;
  std::unique_ptr<TlsSocket> sock_;
  MockEngine* mock_engine_ = nullptr;
  MockFiberSocket* mock_next_sock_ = nullptr;
};

// VerifyEdgeCases: Parameterized White-Box Test
//
// This test validates the robustness of TryRecv() by simulating various non-happy
// path scenarios (defined in TryRecvScenario). It uses mocks to:
// 1. Inject internal state (e.g., WRITE_IN_PROGRESS).
// 2. Control the TLS Engine (e.g., force NEED_WRITE or NEED_READ).
// 3. Control the underlying socket (e.g., simulate EAGAIN, partial writes, EOF).
//
// It ensures that TryRecv handles complex state transitions, race conditions,
// and network errors correctly without crashing or deadlocking.
TEST_P(TryRecvErrorTest, VerifyEdgeCases) {
  const auto& p = GetParam();

  proactor_->Await([&] {
    // 1. Apply Initial State
    if (p.initial_state != 0) {
      TestDelegator::SetState(sock_.get(), p.initial_state);
    }

    // 2. Setup Engine Read Expectations
    EXPECT_CALL(*mock_engine_, Read(testing::_, testing::_))
        .WillRepeatedly(testing::Return(p.engine_read_ret));

    EXPECT_CALL(*mock_engine_, OutputPending())
        .WillRepeatedly(testing::Return(p.engine_output_pending));

    // 3. Handle Engine Output (if needed)
    std::vector<uint8_t> dummy_out_buf;
    if (p.engine_output_pending > 0) {
      // Ensure the buffer returned by Peek has the expected size.
      // This prevents the infinite loop in TryRecv where it checks (bytes_sent <
      // output_buf.size()).
      dummy_out_buf.resize(p.engine_output_pending);

      EXPECT_CALL(*mock_engine_, PeekOutputBuf())
          .WillRepeatedly(
              testing::Return(Engine::Buffer(dummy_out_buf.data(), dummy_out_buf.size())));

      if (p.sock_send_ret.has_value() && *p.sock_send_ret) {
        // Only if send succeeds do we expect ConsumeOutputBuf
        EXPECT_CALL(*mock_engine_, ConsumeOutputBuf(testing::_)).WillRepeatedly(testing::Return());
      }
    }

    // 4. Setup Socket Send Expectations
    if (p.sock_send_ret.has_value()) {
      EXPECT_CALL(*mock_next_sock_, TrySend(testing::_))
          .WillRepeatedly(testing::Return(*p.sock_send_ret));
    }

    // 5. Setup Socket Recv Expectations
    if (p.sock_recv_ret.has_value()) {
      EXPECT_CALL(*mock_engine_, PeekInputBuf())
          .WillRepeatedly(testing::Return(Engine::MutableBuffer{}));

      EXPECT_CALL(*mock_next_sock_, TryRecv(testing::_))
          .WillRepeatedly(testing::Return(*p.sock_recv_ret));

      if (*p.sock_recv_ret && **p.sock_recv_ret > 0) {
        EXPECT_CALL(*mock_engine_, CommitInput(**p.sock_recv_ret))
            .WillRepeatedly(testing::Return());
      }
    }

    // 6. Execution
    uint8_t buf[128];
    auto res = sock_->TryRecv(io::MutableBytes(buf, sizeof(buf)));

    // 7. Verification
    if (p.expected_error) {
      ASSERT_FALSE(res) << "Expected error " << p.expected_error.message() << " but got success";
      EXPECT_EQ(res.error(), p.expected_error)
          << "Expected " << p.expected_error.message() << ", got " << res.error().message();
    } else {
      ASSERT_TRUE(res) << "Expected success but got error: " << res.error().message();
      EXPECT_EQ(*res, p.expected_bytes);
    }

    // Cleanup
    TestDelegator::SetState(sock_.get(), 0);
  });
}

// Instantiating the Scenarios for VerifyEdgeCases
INSTANTIATE_TEST_SUITE_P(
    AllErrorPaths, TryRecvErrorTest,
    testing::Values(
        // Case 0: Write Conflict
        // Scenario: A write operation (e.g., handshake) is already in progress on another fiber.
        // The Engine requests a read/write op, but we cannot proceed safely due to concurrency.
        // Expected: Should back off immediately and return EAGAIN (resource_unavailable_try_again).
        TryRecvScenario{
            .test_name = "WriteConflict_ReturnsTryAgain",
            .initial_state = TestDelegator::GetWriteInProgress(),
            .engine_read_ret = Engine::NEED_READ_AND_MAYBE_WRITE,
            .sock_send_ret = std::nullopt,
            .sock_recv_ret = std::nullopt,
            .expected_error = std::make_error_code(std::errc::resource_unavailable_try_again),
        },

        // Case 1: Engine Needs Write -> Socket EAGAIN
        // Scenario: The TLS Engine generated data (e.g. renegotiation) and requests a flush.
        // We attempt to write to the underlying socket, but it returns EAGAIN (socket buffer full).
        // Expected: We must propagate the EAGAIN to the caller so they can retry later.
        TryRecvScenario{
            .test_name = "EngineNeedWrite_SocketEagain",
            .initial_state = 0,
            .engine_read_ret = Engine::NEED_WRITE,
            .engine_output_pending = 100,
            .sock_send_ret = io::Result<size_t>(nonstd::make_unexpected(
                std::make_error_code(std::errc::resource_unavailable_try_again))),
            .sock_recv_ret = std::nullopt,
            .expected_error = std::make_error_code(std::errc::resource_unavailable_try_again),
        },

        // Case 2: Engine Needs Write -> Socket Short Write
        // Scenario: The TLS Engine has 100 bytes pending. The underlying socket only accepts 50.
        // Since we haven't flushed the full TLS record, we cannot proceed to the read phase safely.
        // Expected: Return EAGAIN to the caller to indicate the operation is incomplete.
        TryRecvScenario{
            .test_name = "EngineNeedWrite_ShortWrite",
            .initial_state = 0,
            .engine_read_ret = Engine::NEED_WRITE,
            .engine_output_pending = 100,
            .sock_send_ret = io::Result<size_t>(50),  // Wrote 50 out of 100
            .sock_recv_ret = std::nullopt,
            .expected_error = std::make_error_code(std::errc::resource_unavailable_try_again),
        },

        // Case 3: Engine Needs Read -> Socket EOF (Dirty Shutdown)
        // Scenario: The TLS Engine needs more data to decrypt a record, but the underlying socket
        // returns 0 (EOF). This means the TCP connection closed without sending a TLS
        // 'close_notify'.
        // Expected: This is a protocol error (Dirty EOF). Return 'connection_reset'.
        TryRecvScenario{
            .test_name = "UpstreamSocketEOF_DirtyShutdown",
            .initial_state = 0,
            .engine_read_ret = Engine::NEED_READ_AND_MAYBE_WRITE,
            .engine_output_pending = 0,
            .sock_send_ret = std::nullopt,
            .sock_recv_ret = io::Result<size_t>(0),  // Underlying socket closed
            .expected_error = std::make_error_code(std::errc::connection_reset),
        },

        // Case 4: Engine Needs Read -> Socket Error
        // Scenario: The TLS Engine requests a read, but the underlying socket returns a system
        // error (e.g., RST packet received, network unreachable).
        // Expected: Propagate the underlying system error (connection_reset) to the caller.
        TryRecvScenario{
            .test_name = "UpstreamSocketError_Propagates",
            .initial_state = 0,
            .engine_read_ret = Engine::NEED_READ_AND_MAYBE_WRITE,
            .engine_output_pending = 0,
            .sock_send_ret = std::nullopt,
            .sock_recv_ret = io::Result<size_t>(
                nonstd::make_unexpected(std::make_error_code(std::errc::connection_reset))),
            .expected_error = std::make_error_code(std::errc::connection_reset),
        },

        // Case 5: Abrupt Stream EOF from Engine
        // Scenario: The TLS Engine itself detects a fatal error (e.g. bad MAC, protocol violation)
        // or an abrupt closure and returns EOF_ABRUPT.
        // Expected: Translate this internal fatal error into 'connection_reset'.
        TryRecvScenario {
          .test_name = "EngineEOFStream_ReturnsReset", .initial_state = 0,
          .engine_read_ret = Engine::EOF_ABRUPT, .engine_output_pending = 0,
          .sock_send_ret = std::nullopt, .sock_recv_ret = std::nullopt,
          .expected_error = std::make_error_code(std::errc::connection_reset),
        },

        // Case 6: Graceful Stream EOF from Engine
        // Scenario: The TLS Engine receives a "close_notify" alert.
        // Expected: Return success with 0 bytes to indicate a clean EOF.
        TryRecvScenario {
          .test_name = "EngineEOFGraceful_ReturnsZero",
          .initial_state = 0,
          .engine_read_ret = Engine::EOF_GRACEFUL,
          .engine_output_pending = 0,
          .sock_send_ret = std::nullopt,
          .sock_recv_ret = std::nullopt,
          .expected_error = std::error_code{}, // Default generic error_code = Success (0)
          .expected_bytes = 0 // Standard EOF return
        }

#if defined(NDEBUG) && !defined(DCHECK_ALWAYS_ON)
        // Case 6: Read Conflict (Release Mode Only)
        // Scenario: A read operation is already in progress. In Release builds
        // (where DCHECK is compiled out), the code must not crash but instead handle the
        // concurrency error gracefully.
        // Expected: The runtime check should detect the conflict and return EAGAIN.
        ,  // <-- Comma to continue the list
        TryRecvScenario{
            .test_name = "ReadConflict_ReturnsTryAgain",
            .initial_state = TestDelegator::GetReadInProgress(),
            .engine_read_ret = Engine::NEED_READ_AND_MAYBE_WRITE,
            .sock_send_ret = std::nullopt,
            .sock_recv_ret = std::nullopt,
            .expected_error = std::make_error_code(std::errc::resource_unavailable_try_again),
        }
#endif
        ));

}  // namespace util::tls
