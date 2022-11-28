// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/tls/tls_engine.h"

#include <string_view>
#include <openssl/err.h>

#include <boost/fiber/fiber.hpp>
#include <boost/fiber/operations.hpp>

#include "base/gtest.h"
#include "base/logging.h"
#include "util/tls/tls_socket.h"

namespace util {
namespace tls {

using namespace std;
using namespace boost;

string SSLError(unsigned long e) {
  char buf[256];
  ERR_error_string_n(e, buf, sizeof(buf));
  return buf;
}

static int VerifyCallback(int ok, X509_STORE_CTX* ctx) {
  LOG(WARNING) << "verify_callback: " << ok;
  return 1;
}

SSL_CTX* CreateSslCntx() {
  SSL_CTX* ctx = SSL_CTX_new(TLS_method());

  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);

  SSL_CTX_set_options(ctx, SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS);

  // SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER | SSL_VERIFY_FAIL_IF_NO_PEER_CERT, NULL);
  SSL_CTX_set_verify(ctx, SSL_VERIFY_NONE, VerifyCallback);

  // Can also be defined using "ADH:@SECLEVEL=0" cipher string below.
  SSL_CTX_set_security_level(ctx, 0);

  CHECK_EQ(1, SSL_CTX_set_cipher_list(ctx, "ADH"));

  return ctx;
}
class SslStreamTest : public testing::Test {
 public:
  struct Options {
    string name;

    unsigned mutate_indx = 0;
    uint8_t mutate_val = 0;
    bool drain_output = false;

    Options(std::string_view v) : name{v} {
    }
  };

  using OpCb = std::function<Engine::OpResult(Engine*)>;

 protected:
  static void SetUpTestSuite() {
  }

  void SetUp() override;

  void TearDown() override {
    client_engine_.reset();
    server_engine_.reset();
  }

  unique_ptr<Engine> client_engine_, server_engine_;
  OpCb srv_handshake_, client_handshake_, shutdown_op_;
  OpCb write_op_, read_op_;

  Options client_opts_{"client"}, srv_opts_{"server"};

  unique_ptr<uint8_t[]> tmp_buf_;
  enum { TMP_CAPACITY = 1024 };
  size_t read_sz_ = 0;
};

void SslStreamTest::SetUp() {
  SSL_CTX* ctx = CreateSslCntx();

  client_engine_.reset(new Engine(ctx));
  server_engine_.reset(new Engine(ctx));
  SSL_CTX_free(ctx);

  // Configure server side.
  SSL* ssl = server_engine_->native_handle();
  SSL_set_options(ssl, SSL_OP_CIPHER_SERVER_PREFERENCE);
  CHECK_EQ(1, SSL_set_dh_auto(ssl, 1));

  tmp_buf_.reset(new uint8_t[TMP_CAPACITY]);

  srv_handshake_ = [](Engine* eng) { return eng->Handshake(Engine::SERVER); };
  client_handshake_ = [](Engine* eng) { return eng->Handshake(Engine::CLIENT); };
  read_op_ = [this](Engine* eng) { return eng->Read(tmp_buf_.get(), TMP_CAPACITY); };
  shutdown_op_ = [](Engine* eng) { return eng->Shutdown(); };
  write_op_ = [this](Engine* eng) {
    return eng->Write(Engine::Buffer{tmp_buf_.get(), TMP_CAPACITY});
  };

  ERR_print_errors_fp(stderr);  // Empties the queue.
}

static unsigned long RunPeer(SslStreamTest::Options opts, SslStreamTest::OpCb cb, Engine* src,
                             Engine* dest) {
  unsigned input_pending = 1;
  while (true) {
    auto op_result = cb(src);
    if (!op_result) {
      return op_result.error();
    }
    VLOG(1) << opts.name << " OpResult: " << *op_result;
    unsigned output_pending = src->OutputPending();
    if (output_pending > 0) {
      if (opts.drain_output)
        src->FetchOutputBuf();
      else {
        auto buf_result = src->PeekOutputBuf();
        CHECK(buf_result);
        VLOG(1) << opts.name << " wrote " << buf_result->size() << " bytes";
        CHECK(!buf_result->empty());

        if (opts.mutate_indx) {
          uint8_t* mem = const_cast<uint8_t*>(buf_result->data());
          mem[opts.mutate_indx % buf_result->size()] = opts.mutate_val;
          opts.mutate_indx = 0;
        }

        auto write_result = dest->WriteBuf(*buf_result);
        if (!write_result) {
          return write_result.error();
        }
        CHECK_GT(*write_result, 0);
        src->ConsumeOutputBuf(*write_result);
      }
    }
    if (*op_result >= 0) {  // Shutdown or empty read/write may return 0.
      return 0;
    }
    if (*op_result == Engine::EOF_STREAM) {
      LOG(WARNING) << opts.name << " stream truncated";
      return 0;
    }

    if (input_pending == 0 && output_pending == 0) {  // dropped connection
      LOG(INFO) << "Dropped connections for " << opts.name;

      return ERR_PACK(ERR_LIB_USER, 0, ERR_R_OPERATION_FAIL);
    }
    this_fiber::yield();
    input_pending = src->InputPending();
    VLOG(1) << "Input size: " << input_pending;
  }
}

TEST_F(SslStreamTest, BIO_s_bio_err) {
  // BIO_new creates a default buffer of 17KB.
  BIO* bio1 = BIO_new(BIO_s_bio());
  constexpr char kData[] = "ROMAN";
  ASSERT_EQ(0, ERR_get_error());

  EXPECT_LT(BIO_write(bio1, kData, sizeof(kData)), 0);
  int e = ERR_get_error();
  EXPECT_NE(0, e);

  EXPECT_EQ(BIO_R_UNINITIALIZED, ERR_GET_REASON(e));
  BIO_free(bio1);
}

TEST_F(SslStreamTest, BIO_s_bio_ZeroCopy) {
  BIO *bio1, *bio2;

  // writesize = 0, means we use default 17KB buffer.
  ASSERT_EQ(1, BIO_new_bio_pair(&bio1, 0, &bio2, 0));

  char *buf1 = nullptr, *buf2 = nullptr;

  // Fetch buffer to the direct memory buffer.
  ssize_t write_size = BIO_nwrite0(bio1, &buf1);
  EXPECT_EQ(17 * 1024, write_size);
  memset(buf1, 'a', write_size);

  EXPECT_EQ(write_size, BIO_nwrite(bio1, nullptr, write_size));  // commit.
  EXPECT_EQ(-1, BIO_nwrite(bio1, nullptr, 1));                   // No space to commit.
  EXPECT_EQ(0, BIO_ctrl_get_write_guarantee(bio1));
  EXPECT_EQ(0, BIO_ctrl_pending(bio1));            // 0 read pending.
  EXPECT_EQ(write_size, BIO_ctrl_wpending(bio1));  // 17KB write pending.

  EXPECT_EQ(write_size, BIO_ctrl_pending(bio2));  // 17KB read pending.
  EXPECT_EQ(0, BIO_ctrl_wpending(bio2));          // 0 write pending.

  ASSERT_EQ(write_size, BIO_nread0(bio2, &buf2));
  EXPECT_EQ(0, memcmp(buf1, buf2, write_size));
  EXPECT_EQ(write_size, BIO_nread(bio2, nullptr, write_size));

  // bio1 is empty again.
  EXPECT_EQ(write_size, BIO_ctrl_get_write_guarantee(bio1));

  // Another way to write to BIO without using BIO_nwrite0.
  // Instead of fetching a pointer to buffer, we commit first and then write using memcpy.
  buf1 = nullptr;
  EXPECT_EQ(write_size, BIO_nwrite(bio1, &buf1, write_size));  // commit first.

  // BIO is single-threaded object, hence noone will read from bio2 as long
  // as we do not switch to reading ourselves. Therefore the order of committing and writing is not
  // important.
  memset(buf1, 'a', write_size);

  BIO_free(bio1);
  BIO_free(bio2);
}

TEST_F(SslStreamTest, Handshake) {
  unsigned long cl_err = 0, srv_err = 0;

  auto client_fb = fibers::fiber([&] {
    cl_err = RunPeer(client_opts_, client_handshake_, client_engine_.get(), server_engine_.get());
  });

  auto server_fb = fibers::fiber([&] {
    srv_err = RunPeer(srv_opts_, srv_handshake_, server_engine_.get(), client_engine_.get());
  });

  client_fb.join();
  server_fb.join();
  ASSERT_EQ(0, cl_err);
  ASSERT_EQ(0, srv_err);
}

TEST_F(SslStreamTest, HandshakeErrServer) {
  unsigned long cl_err = 0, srv_err = 0;

  auto client_fb = fibers::fiber([&] {
    cl_err = RunPeer(client_opts_, client_handshake_, client_engine_.get(), server_engine_.get());
  });

  srv_opts_.mutate_indx = 100;
  srv_opts_.mutate_val = 'R';

  auto server_fb = fibers::fiber([&] {
    srv_err = RunPeer(srv_opts_, srv_handshake_, server_engine_.get(), client_engine_.get());
  });

  client_fb.join();
  server_fb.join();

  LOG(INFO) << SSLError(cl_err);
  LOG(INFO) << SSLError(srv_err);

  ASSERT_NE(0, cl_err);
}

TEST_F(SslStreamTest, ReadShutdown) {
  unsigned long cl_err = 0, srv_err = 0;

  auto client_fb = fibers::fiber([&] {
    cl_err = RunPeer(client_opts_, client_handshake_, client_engine_.get(), server_engine_.get());
  });

  auto server_fb = fibers::fiber([&] {
    srv_err = RunPeer(srv_opts_, srv_handshake_, server_engine_.get(), client_engine_.get());
  });

  client_fb.join();
  server_fb.join();

  ASSERT_EQ(0, cl_err);
  ASSERT_EQ(0, srv_err);

  client_fb = fibers::fiber([&] {
    cl_err = RunPeer(client_opts_, shutdown_op_, client_engine_.get(), server_engine_.get());
  });

  server_fb = fibers::fiber(
      [&] { srv_err = RunPeer(srv_opts_, read_op_, server_engine_.get(), client_engine_.get()); });

  server_fb.join();
  client_fb.join();
  ASSERT_EQ(0, cl_err);
  ASSERT_EQ(0, srv_err);

  int shutdown_srv = SSL_get_shutdown(server_engine_->native_handle());
  int shutdown_client = SSL_get_shutdown(client_engine_->native_handle());
  ASSERT_EQ(SSL_RECEIVED_SHUTDOWN, shutdown_srv);
  ASSERT_EQ(SSL_SENT_SHUTDOWN, shutdown_client);

  client_fb = fibers::fiber([&] {
    cl_err = RunPeer(client_opts_, shutdown_op_, client_engine_.get(), server_engine_.get());
  });

  server_fb = fibers::fiber([&] {
    srv_err = RunPeer(srv_opts_, shutdown_op_, server_engine_.get(), client_engine_.get());
  });
  server_fb.join();
  client_fb.join();

  ASSERT_EQ(0, cl_err) << SSLError(cl_err);
  ASSERT_EQ(0, srv_err);

  shutdown_srv = SSL_get_shutdown(server_engine_->native_handle());
  shutdown_client = SSL_get_shutdown(client_engine_->native_handle());
  ASSERT_EQ(SSL_RECEIVED_SHUTDOWN | SSL_SENT_SHUTDOWN, shutdown_srv);
  ASSERT_EQ(shutdown_client, shutdown_srv);
}

TEST_F(SslStreamTest, Write) {
  unsigned long cl_err = 0, srv_err = 0;

  auto client_fb = fibers::fiber([&] {
    cl_err = RunPeer(client_opts_, client_handshake_, client_engine_.get(), server_engine_.get());
  });

  auto server_fb = fibers::fiber([&] {
    srv_err = RunPeer(srv_opts_, srv_handshake_, server_engine_.get(), client_engine_.get());
  });

  client_fb.join();
  server_fb.join();
  client_opts_.drain_output = true;
  for (size_t i = 0; i < 10; ++i) {
    cl_err = RunPeer(client_opts_, write_op_, client_engine_.get(), server_engine_.get());
    ASSERT_EQ(0, cl_err);
  }
}


TEST_F(SslStreamTest, Socket) {
  TlsSocket socket;
}

void BM_TlsWrite(benchmark::State& state) {
  unique_ptr<Engine> client_engine, server_engine;
  SslStreamTest::Options sopts{"srv"}, copts{"client"};
  auto cl_handshake = [](Engine* eng) { return eng->Handshake(Engine::CLIENT); };
  auto srv_handshake = [](Engine* eng) { return eng->Handshake(Engine::SERVER); };
  SSL_CTX* ctx = CreateSslCntx();
  client_engine.reset(new Engine(ctx));
  server_engine.reset(new Engine(ctx));
  SSL_CTX_free(ctx);

  SSL* ssl = server_engine->native_handle();
  CHECK_EQ(1, SSL_set_dh_auto(ssl, 1));

  auto client_fb = fibers::fiber(
      [&] { RunPeer(copts, cl_handshake, client_engine.get(), server_engine.get()); });

  auto server_fb = fibers::fiber(
      [&] { RunPeer(sopts, srv_handshake, server_engine.get(), client_engine.get()); });

  client_fb.join();
  server_fb.join();

  copts.drain_output = true;

  size_t len = state.range(0);
  unique_ptr<uint8_t[]> wb(new uint8_t[len]);
  Engine::Buffer buf{wb.get(), len};

  auto write_cb = [&](Engine* eng) { return eng->Write(buf); };
  while (state.KeepRunning()) {
    CHECK_EQ(0u, RunPeer(copts, write_cb, client_engine.get(), server_engine.get()));
  }
}
BENCHMARK(BM_TlsWrite)->Arg(1024)->Arg(4096)->ArgName("bytes");

}  // namespace tls
}  // namespace util
