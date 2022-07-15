// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <absl/strings/ascii.h>

#include "base/init.h"
#include "base/io_buf.h"
#include "examples/pingserver/resp_parser.h"
#include "util/accept_server.h"
#include "util/asio_stream_adapter.h"
#include "util/http/http_handler.h"
#include "util/tls/tls_socket.h"
#include "util/fiber_socket_base.h"
#include "util/uring/uring_fiber_algo.h"
#include "util/uring/uring_pool.h"
#include "util/uring/proactor.h"
#include "util/varz.h"

using namespace boost;
using namespace std;
using namespace util;
using redis::RespParser;
using uring::Proactor;
using uring::UringPool;
using absl::GetFlag;

ABSL_FLAG(int32_t, http_port, 8080, "Http port.");
ABSL_FLAG(int32_t, port, 6380, "Redis port");
ABSL_FLAG(uint32_t, iouring_depth, 512, "Io uring depth");
ABSL_FLAG(bool, tls, false, "Enable tls");
ABSL_FLAG(bool, tls_verify_peer, false,
            "Require peer certificate. Please note that this flag requires loading of "
            "server certificates (not sure why).");
ABSL_FLAG(bool, use_incoming_cpu, false, "If true uses incoming cpu of a socket in order to distribute incoming connections");
ABSL_FLAG(string, tls_cert, "", "");
ABSL_FLAG(string, tls_key, "", "");
ABSL_FLAG(string, unixsocket, "", "");

VarzQps ping_qps("ping-qps");

inline void ToUpper(const RespParser::Buffer* val) {
  for (auto& c : *val) {
    c = absl::ascii_toupper(c);
  }
}

static inline std::string_view ToAbsl(const absl::Span<uint8_t>& s) {
  return std::string_view{reinterpret_cast<char*>(s.data()), s.size()};
}

class PingConnection : public Connection {
 public:
  PingConnection(SSL_CTX* ctx) : ctx_(ctx) {
  }

  void Handle(Proactor::IoResult res, int32_t payload, Proactor* mgr);

  void StartPolling(int fd, Proactor* mgr);

 private:
  void HandleRequests() final;

  SSL_CTX* ctx_ = nullptr;
};

atomic_int conn_id{1};

void PingConnection::HandleRequests() {
  auto& props = this_fiber::properties<FiberProps>();

  props.set_name(absl::StrCat("ping/", conn_id.fetch_add(1, memory_order_relaxed)));

  system::error_code ec;
  unique_ptr<tls::TlsSocket> tls_sock;
  if (ctx_) {
    tls_sock.reset(new tls::TlsSocket(socket_.get()));
    tls_sock->InitSSL(ctx_);

    FiberSocketBase::AcceptResult aresult = tls_sock->Accept();
    if (!aresult) {
      LOG(ERROR) << "Error handshaking " << aresult.error().message();
      return;
    } else {
      LOG(INFO) << "TLS handshake succeeded";
    }
  }
  FiberSocketBase* peer = tls_sock ? (FiberSocketBase*)tls_sock.get() : socket_.get();

  AsioStreamAdapter<FiberSocketBase> asa(*peer);
  base::IoBuf io_buf{1024};
  RespParser resp_parser;
  uint32_t consumed = 0;
  vector<RespParser::Buffer> args;
  while (true) {
    auto dest = io_buf.AppendBuffer();
    asio::mutable_buffer mb(dest.data(), dest.size());

    size_t res = asa.read_some(mb, ec);
    if (FiberSocketBase::IsConnClosed(ec))
      break;

    uint64_t now = peer->proactor()->GetMonotonicTimeNs();
    uint64_t delta_usec = (now - props.resume_ts()) / 1000;
    if (delta_usec > 1000) {
      LOG(INFO) << "Running too long " << props.name() << " " << delta_usec;
    }

    CHECK(!ec) << ec << "/" << ec.message();
    VLOG(1) << "Read " << res << " bytes";
    io_buf.CommitWrite(res);
    RespParser::Status st = resp_parser.Parse(io_buf.InputBuffer(), &consumed, &args);
    io_buf.ConsumeInput(consumed);

    if (st == RespParser::RESP_OK) {
      if (args.empty())
        continue;
      ToUpper(&args[0]);
      std::string_view cmd = ToAbsl(args.front());
      if (cmd == "PING") {
        ping_qps.Inc();
        const char kReply[] = "+PONG\r\n";
        auto b = boost::asio::buffer(kReply, sizeof(kReply) - 1);

        asa.write_some(b, ec);
        if (ec) {
          break;
        }
        now = peer->proactor()->GetMonotonicTimeNs();
        delta_usec = (now - props.resume_ts()) / 1000;
        LOG_IF(INFO, delta_usec > 1000) << "Running too long " << props.name() << " " << delta_usec;
      } else {
        const char kReply[] = "+OK\r\n";
        auto b = boost::asio::buffer(kReply, sizeof(kReply) - 1);
        asa.write_some(b, ec);
        if (ec) {
          break;
        }
      }
    } else if (st == RespParser::MORE_INPUT) {
      continue;
    } else {
      break;
    }
  }

  VLOG(1) << "Connection shutting down";
  socket_->Shutdown(SHUT_RDWR);
}

class PingListener : public ListenerInterface {
 public:
  PingListener(SSL_CTX* ctx) : ctx_(ctx) {
  }

  ~PingListener() {
    SSL_CTX_free(ctx_);
  }

  virtual Connection* NewConnection(ProactorBase* context) final {
    return new PingConnection(ctx_);
  }

  ProactorBase* PickConnectionProactor(LinuxSocketBase* sock) final;
 private:
  SSL_CTX* ctx_;
};


ProactorBase* PingListener::PickConnectionProactor(LinuxSocketBase* sock) {
  int fd = sock->native_handle();

  int cpu;
  socklen_t len = sizeof(cpu);

  CHECK_EQ(0, getsockopt(fd, SOL_SOCKET, SO_INCOMING_CPU, &cpu, &len));
  VLOG(1) << "Got socket from cpu " << cpu;
  bool use_incoming = GetFlag(FLAGS_use_incoming_cpu);

  if (use_incoming) {
    vector<unsigned> ids = pool()->MapCpuToThreads(cpu);
    if (!ids.empty()) {
      return pool()->at(ids.front());
    }
  }
  return pool()->GetNextProactor();
}

static int MyVerifyCb(int preverify_ok, X509_STORE_CTX* x509_ctx) {
  LOG(INFO) << "preverify " << preverify_ok;
  return 1;
}

// To connect: openssl s_client  -cipher "ADH:@SECLEVEL=0" -state -crlf  -connect 127.0.0.1:6380
static SSL_CTX* CreateSslCntx() {
  SSL_CTX* ctx = SSL_CTX_new(TLS_server_method());

  // TO connect with redis-cli:
  // ./src/redis-cli --tls -p 6380 --insecure  PING
  // For redis-cli we need to load certificate in order to use a common cipher.
  string tls_key = GetFlag(FLAGS_tls_key);

  if (!tls_key.empty()) {
    CHECK_EQ(1, SSL_CTX_use_PrivateKey_file(ctx, tls_key.c_str(), SSL_FILETYPE_PEM));
    string tls_cert = GetFlag(FLAGS_tls_cert);
    if (!tls_cert.empty()) {
      CHECK_EQ(1, SSL_CTX_use_certificate_chain_file(ctx, tls_cert.c_str()));
    }
  }
  SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);

  SSL_CTX_set_options(ctx, SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS);

  unsigned mask = SSL_VERIFY_PEER;
  bool tls_verify_peer = GetFlag(FLAGS_tls_verify_peer);
  if (tls_verify_peer)
    mask |= SSL_VERIFY_FAIL_IF_NO_PEER_CERT;
  SSL_CTX_set_verify(ctx, mask, MyVerifyCb);
  // SSL_CTX_set_verify_depth(ctx, 0);

  // Can also be defined using "ADH:@SECLEVEL=0" cipher string below.
  SSL_CTX_set_security_level(ctx, 0);

  CHECK_EQ(1, SSL_CTX_set_cipher_list(ctx, "ADH:DEFAULT"));
  CHECK_EQ(1, SSL_CTX_set_dh_auto(ctx, 1));

  return ctx;
}

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);
  int port = GetFlag(FLAGS_port);
  string uds = GetFlag(FLAGS_unixsocket);

  if (uds.empty()) {
    CHECK_GT(port, 0);
  }

  SSL_CTX* ctx = nullptr;

  if (GetFlag(FLAGS_tls)) {
    ctx = CreateSslCntx();
  }

  UringPool pp{GetFlag(FLAGS_iouring_depth)};
  pp.Run();
  ping_qps.Init(&pp);

  AcceptServer uring_acceptor(&pp);
  PingListener* listener = new PingListener(ctx);

  if (uds.empty()) {
    uring_acceptor.AddListener(port, listener);
  } else {
    unlink(uds.c_str());
    error_code ec = uring_acceptor.AddUDSListener(uds.c_str(), listener);
    CHECK(!ec) << ec;
  }

  int http_port = GetFlag(FLAGS_http_port);
  if (http_port >= 0) {
    uint16_t port = uring_acceptor.AddListener(http_port, new HttpListener<>);
    LOG(INFO) << "Started http server on port " << port;
  }

  uring_acceptor.Run();
  uring_acceptor.Wait();

  pp.Stop();

  return 0;
}
