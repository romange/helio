// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

// clang-format off
#include <sys/time.h>
#include <sys/mman.h>

#include <queue>

// clang-format on

#include <absl/strings/str_cat.h>

#include <boost/asio/read.hpp>

#include "base/histogram.h"
#include "base/init.h"
#include "util/accept_server.h"
#include "util/asio_stream_adapter.h"
#include "util/fibers/dns_resolve.h"
#include "util/fibers/pool.h"
#include "util/fibers/synchronization.h"
#include "util/http/http_handler.h"
#include "util/varz.h"

#ifdef __linux__
#include "util/fibers/uring_proactor.h"
#include "util/fibers/uring_socket.h"
#endif

using namespace util;

using fb2::DnsResolve;
using fb2::Fiber;
using fb2::Mutex;

using namespace std;

using tcp = ::boost::asio::ip::tcp;

using absl::GetFlag;

ABSL_FLAG(bool, epoll, false, "If true use epoll for server");
ABSL_FLAG(int32, http_port, 8080, "Http port.");
ABSL_FLAG(int32, port, 8081, "Echo server port");
ABSL_FLAG(uint32, n, 1000, "Number of requests per connection");
ABSL_FLAG(uint32, c, 10, "Number of connections per thread");
ABSL_FLAG(uint32, size, 1, "Message size, 0 for hardcoded 4 byte pings");
ABSL_FLAG(uint32, backlog, 1024, "Accept queue length");
ABSL_FLAG(uint32, p, 1, "pipelining factor");
ABSL_FLAG(string, connect, "", "hostname or ip address to connect to in client mode");
ABSL_FLAG(uint32, write_num, 1000, "");
ABSL_FLAG(uint32, max_pending_writes, 300, "");
ABSL_FLAG(uint32, max_clients, 1 << 16, "");
ABSL_FLAG(bool, raw, true,
          "If true, does not send/receive size parameter during "
          "the connection handshake");
ABSL_FLAG(bool, tcp_nodelay, true, "use tcp_nodelay option for server sockets");
ABSL_FLAG(bool, multishot, false, "If true, iouring sockets use multishot receives");
ABSL_FLAG(uint16_t, bufring_size, 256, "Size of the buffer ring for iouring sockets");
ABSL_FLAG(bool, use_incoming_cpu, false,
          "If true uses SO_INCOMING_CPU in order to distribute incoming connections");
ABSL_FLAG(bool, ipv6, false, "If true, use ipv6 for server");

VarzQps ping_qps("ping-qps");
VarzCount connections("connections");

const char kMaxConnectionsError[] = "max connections reached\r\n";
constexpr size_t kBufLen = 64;

class EchoConnection : public Connection {
 public:
  EchoConnection() {
  }

 private:
  struct SendMsgState {
    bool is_raw = false;
    error_code ec;
    size_t cur_sendmsg_len = 0;
    vector<iovec> vec;
    vector<FiberSocketBase::ProvidedBuffer> kept_buffers;
    FiberSocketBase::ProvidedBuffer pbuf;
    unsigned buf_len;
  };

  void HandleRequests() final;

  error_code ReadMsg();

  // Returns true if we still need the buffer because it's referenced by iovec.
  bool ProcessSingleBuffer(SendMsgState* state);
  void ProcessFully(SendMsgState* state);

  error_code Send(bool is_raw, const vector<iovec>& vec);

  queue<FiberSocketBase::ProvidedBuffer> prov_buffers_;
  uint64_t replies_ = 0;
  size_t req_len_ = 0;
};

std::error_code EchoConnection::ReadMsg() {
  FiberSocketBase::ProvidedBuffer pb[8];
  VLOG(2) << "Waiting for socket read";
  unsigned num_bufs = socket_->RecvProvided(8, pb);

  for (unsigned i = 0; i < num_bufs; ++i) {
    if (pb[i].res_len > 0) {
      prov_buffers_.push(pb[i]);
    } else {
      DCHECK_EQ(i, 0u);
      return error_code(-pb[i].res_len, system_category());
    }
  }
  VLOG(2) << "Received " << num_bufs << " buffers";
  return {};
}

static thread_local base::Histogram send_hist;

void EchoConnection::HandleRequests() {
  ThisFiber::SetName("HandleRequests");

  uint8_t buf[8];

  int yes = 1;
  if (GetFlag(FLAGS_tcp_nodelay)) {
    CHECK_EQ(0, setsockopt(socket_->native_handle(), IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)));
  }

#ifdef __linux__
  bool is_multishot = GetFlag(FLAGS_multishot);
  bool is_iouring = socket_->proactor()->GetKind() == ProactorBase::IOURING;
  if (is_multishot && is_iouring) {
    static_cast<fb2::UringSocket*>(socket_.get())->EnableRecvMultishot();
  }
#endif
  connections.IncBy(1);

  auto ep = socket_->RemoteEndpoint();

  VLOG(1) << "New connection from " << ep;

  SendMsgState state;
  state.vec.resize(1);
  state.vec[0].iov_base = buf;
  state.vec[0].iov_len = 8;

  state.is_raw = GetFlag(FLAGS_raw);

  if (state.is_raw) {
    req_len_ = GetFlag(FLAGS_size);
  } else {
    VLOG(1) << "Waiting for size header from " << ep;
    auto es = socket_->Recv(buf, 0);
    if (!es.has_value()) {
      if (es.error().value() == ECONNABORTED)
        return;
      LOG(FATAL) << "Bad Conn Handshake " << es.error() << " for socket " << ep;
    } else {
      CHECK_EQ(es.value(), 8U);
      VLOG(1) << "Received size from " << ep;
      uint8_t ack = 1;
      socket_->WriteSome(io::Bytes(&ack, 1));  // Send ACK
    }

    CHECK(es.has_value() && es.value() == sizeof(buf));
    req_len_ = absl::little_endian::Load64(buf);
  }

  CHECK_LE(req_len_, 1UL << 26);

  // after the handshake.
  while (true) {
    state.ec = ReadMsg();
    if (FiberSocketBase::IsConnClosed(state.ec)) {
      VLOG(1) << "Closing " << ep << " after " << replies_ << " replies";
      break;
    }
    CHECK(!state.ec) << state.ec;
    ping_qps.Inc();

    state.vec[0].iov_base = buf;
    state.vec[0].iov_len = 4;
    absl::little_endian::Store32(buf, req_len_);

    while (!prov_buffers_.empty()) {
      state.pbuf = prov_buffers_.front();
      state.buf_len = state.pbuf.res_len;
      prov_buffers_.pop();
      ProcessFully(&state);
      if (state.ec)
        break;
    }
    if (state.ec)
      break;
  }

  VLOG(1) << "Connection ended " << ep;
  connections.IncBy(-1);
}

bool EchoConnection::ProcessSingleBuffer(SendMsgState* state) {
  auto GetBufferStart = [](const FiberSocketBase::ProvidedBuffer& pb) -> uint8_t* {
    if (pb.type == FiberSocketBase::kHeapType)
      return pb.start;
#ifdef __linux__
    return static_cast<fb2::UringProactor*>(ProactorBase::me())->GetBufRingPtr(0, pb.buf_id);
#endif
    return nullptr;
  };

  size_t len = std::min<unsigned>(state->buf_len, kBufLen);
  size_t buf_offset = 0;
  uint8_t* start = GetBufferStart(state->pbuf);
  while (state->cur_sendmsg_len + len >= req_len_) {
    unsigned needed = req_len_ - state->cur_sendmsg_len;
    state->vec.push_back({start + buf_offset, needed});
    state->ec = Send(state->is_raw, state->vec);
    state->vec.resize(1);
    for (auto& pb : state->kept_buffers) {
      VLOG(2) << "Return buffer id " << pb.buf_id << " " << pb.res_len;
      socket_->ReturnProvided(pb);
    }
    state->kept_buffers.clear();
    state->cur_sendmsg_len = 0;
    buf_offset += needed;
    len -= needed;
    if (state->ec)
      return false;
  }

  if (len) {  // consume the whole buffer and can not send yet.
    state->vec.push_back({start + buf_offset, len});
    state->cur_sendmsg_len += len;
    return true;
  }
  return true;
}

void EchoConnection::ProcessFully(SendMsgState* state) {
#ifdef __linux__
  fb2::UringProactor* up = static_cast<fb2::UringProactor*>(socket_->proactor());
  while (state->buf_len > kBufLen) {  // process bundle
    ProcessSingleBuffer(state);
    state->buf_len -= kBufLen;
    ++state->pbuf.buf_pos;
    state->pbuf.buf_id = up->GetBufIdByPos(0, state->pbuf.buf_pos);
    if (state->ec)
      return;
  }
#endif
  if (ProcessSingleBuffer(state)) {
    state->kept_buffers.push_back(state->pbuf);
  } else {
    VLOG(1) << "Return buffer id " << state->pbuf.buf_id << " " << state->pbuf.res_len;
    socket_->ReturnProvided(state->pbuf);
  }
}

error_code EchoConnection::Send(bool is_raw, const vector<iovec>& vec) {
  VLOG(1) << "Send response " << replies_;

  error_code ec;
  if (is_raw) {
    auto prev = absl::GetCurrentTimeNanos();
    ec = socket_->Write(vec.data() + 1, vec.size() - 1);
    auto now = absl::GetCurrentTimeNanos();
    send_hist.Add((now - prev) / 1000);
  } else {
    ec = socket_->Write(vec.data(), vec.size());
  }

  ++replies_;
  return ec;
}

class EchoListener : public ListenerInterface {
 public:
  EchoListener() {
    SetMaxClients(GetFlag(FLAGS_max_clients));
  }
  virtual Connection* NewConnection(ProactorBase* context) final {
    VLOG(1) << "thread_id " << context->GetPoolIndex();
    return new EchoConnection;
  }

  void OnMaxConnectionsReached(FiberSocketBase* sock) override {
    sock->Write(io::Buffer(kMaxConnectionsError));
  }

  ProactorBase* PickConnectionProactor(FiberSocketBase* sock) final;
};

ProactorBase* EchoListener::PickConnectionProactor(FiberSocketBase* sock) {
#ifdef __linux__
  bool use_incoming = GetFlag(FLAGS_use_incoming_cpu);
  int fd = sock->native_handle();

  int cpu;
  socklen_t len = sizeof(cpu);

  if (getsockopt(fd, SOL_SOCKET, SO_INCOMING_CPU, &cpu, &len) == 0) {
    VLOG(1) << "Got socket from cpu " << cpu;

    if (use_incoming) {
      vector<unsigned> ids = pool()->MapCpuToThreads(cpu);
      if (!ids.empty()) {
        VLOG(1) << "Using proactor " << ids.front();
        return pool()->at(ids.front());
      }
    }
  }
#endif
  return pool()->GetNextProactor();
}

void RunServer(ProactorPool* pp) {
  ping_qps.Init(pp);
  connections.Init(pp);

  AcceptServer acceptor(pp);
  acceptor.set_back_log(GetFlag(FLAGS_backlog));

  char bind_addr[INET6_ADDRSTRLEN];
  if (GetFlag(FLAGS_ipv6)) {
    strcpy(bind_addr, "::");
  } else {
    strcpy(bind_addr, "0.0.0.0");
  }
  acceptor.AddListener(bind_addr, GetFlag(FLAGS_port), new EchoListener);
  if (GetFlag(FLAGS_http_port) >= 0) {
    error_code ec = acceptor.AddListener(bind_addr, GetFlag(FLAGS_http_port), new HttpListener<>);
    CHECK(!ec) << "Could not open port " << GetFlag(FLAGS_http_port) << " " << ec;
    LOG(INFO) << "Started http server on port " << GetFlag(FLAGS_http_port);
  }

  acceptor.Run();
  acceptor.Wait();
  base::Histogram send_res;
  Mutex mu;

  pp->AwaitFiberOnAll([&](auto*) {
    unique_lock lk(mu);
    send_res.Merge(send_hist);
  });
  LOG(INFO) << "Send histogram " << send_res.ToString();
}

class Driver {
  std::unique_ptr<FiberSocketBase> socket_;

  Driver(const Driver&) = delete;

  uint64_t send_id_ = 0;

 public:
  Driver(ProactorBase* p);

  void Connect(unsigned index, const tcp::endpoint& ep);
  size_t Run(base::Histogram* dest);

 private:
  void SendSingle();

  uint8_t buf_[8];
};

Driver::Driver(ProactorBase* p) {
  socket_.reset(p->CreateSocket());
}

void Driver::Connect(unsigned index, const tcp::endpoint& ep) {
  size_t iter = 0;
  size_t kMaxIter = 3;
  VLOG(1) << "Driver::Connect-Start " << index;

  bool is_raw = GetFlag(FLAGS_raw);

  for (; iter < kMaxIter; ++iter) {
    uint64_t start = absl::GetCurrentTimeNanos();
    auto ec = socket_->Connect(ep);
    CHECK(!ec) << ec.message();
    VLOG(1) << "Connected to " << socket_->RemoteEndpoint();

    uint64_t start1 = absl::GetCurrentTimeNanos();
    uint64_t delta_msec = (start1 - start) / 1000000;
    LOG_IF(ERROR, delta_msec > 1000) << "Slow connect1 " << index << " " << delta_msec << " ms";

    if (is_raw) {
      SendSingle();
      break;
    }

    // Send msg size.
    absl::little_endian::Store64(buf_, absl::GetFlag(FLAGS_size));
    io::Result<size_t> es = socket_->WriteSome(buf_);
    CHECK(es) << es.error();  // Send expected payload size.
    CHECK_EQ(8U, es.value());

    uint64_t start2 = absl::GetCurrentTimeNanos();
    delta_msec = (start2 - start) / 1000000;
    LOG_IF(ERROR, delta_msec > 2000) << "Slow connect2 " << index << " " << delta_msec << " ms";

    // wait for ack.
    es = socket_->Recv(io::MutableBytes(buf_, 1), 0);
    delta_msec = (absl::GetCurrentTimeNanos() - start2) / 1000000;
    LOG_IF(ERROR, delta_msec > 2000) << "Slow connect3 " << index << " " << delta_msec << " ms";

    if (es) {
      CHECK_EQ(1U, es.value());
      break;
    }

    // There could be scenario where tcp Connect succeeds, but socket in fact is not really
    // connected (which I suspect happens due to small accept queue) and
    // we discover this upon first Recv. I am not sure why it happens. Right now I retry.
    CHECK(es.error() == std::errc::connection_reset) << es.error();

    ec = socket_->Close();
    LOG_IF(WARNING, !ec) << "Socket close failed: " << ec.message();
    LOG(WARNING) << "Driver " << index << " retries";
  }

  int bufferlen = 1 << 14;
  CHECK_EQ(0, setsockopt(socket_->native_handle(), SOL_SOCKET, SO_SNDBUF, &bufferlen,
                         sizeof(bufferlen)));
  CHECK_EQ(0, setsockopt(socket_->native_handle(), SOL_SOCKET, SO_RCVBUF, &bufferlen,
                         sizeof(bufferlen)));

  CHECK_LT(iter, kMaxIter) << "Maximum reconnects reached";
  VLOG(1) << "Driver::Connect-End " << index;
}

void Driver::SendSingle() {
  size_t req_size = absl::GetFlag(FLAGS_size);
  std::unique_ptr<uint8_t[]> msg(new uint8_t[req_size]);
  memcpy(msg.get(), &send_id_, std::min(sizeof(send_id_), req_size));
  ++send_id_;

  error_code ec = socket_->Write(io::Bytes{msg.get(), req_size});
  CHECK(!ec) << ec.message();
  auto res = socket_->Read(io::MutableBytes(msg.get(), req_size));
  CHECK(res) << res.error();
  CHECK_EQ(res.value(), req_size);
}

size_t Driver::Run(base::Histogram* dest) {
  base::Histogram hist;
  size_t req_size = absl::GetFlag(FLAGS_size);
  std::unique_ptr<uint8_t[]> msg(new uint8_t[req_size]);

  iovec vec[2];
  vec[0].iov_len = 4;
  vec[0].iov_base = buf_;
  vec[1].iov_len = absl::GetFlag(FLAGS_size);
  vec[1].iov_base = msg.get();

  auto lep = socket_->LocalEndpoint();
  CHECK(socket_->IsOpen());

  AsioStreamAdapter<> adapter(*socket_);
  io::Result<size_t> es;

  bool conn_close = false;
  size_t i = 0;

  size_t pipeline_cnt = absl::GetFlag(FLAGS_p);
  bool is_raw = GetFlag(FLAGS_raw);

  for (; i < absl::GetFlag(FLAGS_n); ++i) {
    auto start = absl::GetCurrentTimeNanos();

    for (size_t j = 0; j < pipeline_cnt; ++j) {
      memcpy(msg.get(), &send_id_, std::min(sizeof(send_id_), req_size));
      ++send_id_;
      error_code ec = socket_->Write(io::Bytes{msg.get(), req_size});
      if (ec && FiberSocketBase::IsConnClosed(ec)) {
        conn_close = true;
        break;
      }
      CHECK(!ec) << ec.message();
    }

    if (conn_close)
      break;

    for (size_t j = 0; j < pipeline_cnt; ++j) {
      // DVLOG(1) << "Recv " << lep << " " << i;
      if (!is_raw) {
        es = socket_->Recv(vec, 1);
        if (!es && FiberSocketBase::IsConnClosed(es.error())) {
          conn_close = true;
          break;
        }

        CHECK(es.has_value()) << "RecvError: " << es.error() << "/" << lep;
      }

      ::boost::system::error_code ec;
      size_t sz = ::boost::asio::read(
          adapter, ::boost::asio::buffer(msg.get(), absl::GetFlag(FLAGS_size)), ec);

      CHECK(!ec) << ec.message();
      CHECK_EQ(sz, absl::GetFlag(FLAGS_size));
    }

    uint64_t dur = absl::GetCurrentTimeNanos() - start;

    hist.Add(dur / 1000);
    if (conn_close)
      break;
  }

  std::ignore = socket_->Shutdown(SHUT_RDWR);
  dest->Merge(hist);

  return i;
}

mutex lat_mu;
base::Histogram lat_hist;

class TLocalClient {
  ProactorBase* p_;
  vector<std::unique_ptr<Driver>> drivers_;

  TLocalClient(const TLocalClient&) = delete;

 public:
  TLocalClient(ProactorBase* p) : p_(p) {
    drivers_.resize(absl::GetFlag(FLAGS_c));
    for (size_t i = 0; i < drivers_.size(); ++i) {
      drivers_[i].reset(new Driver{p});
    }
  }

  void Connect(tcp::endpoint ep);
  size_t Run();
};

void TLocalClient::Connect(tcp::endpoint ep) {
  LOG(INFO) << "TLocalClient::Connect-Start";
  vector<Fiber> fbs(drivers_.size());
  for (size_t i = 0; i < fbs.size(); ++i) {
    fbs[i] = MakeFiber([&, i] {
      ThisFiber::SetName(absl::StrCat("connect/", i));
      uint64_t start = absl::GetCurrentTimeNanos();
      drivers_[i]->Connect(i, ep);
      uint64_t delta_msec = (absl::GetCurrentTimeNanos() - start) / 1000000;
      LOG_IF(ERROR, delta_msec > 4000) << "Slow DriverConnect " << delta_msec << " ms";
    });
  }
  for (auto& fb : fbs)
    fb.Join();
  LOG(INFO) << "TLocalClient::Connect-End";
  base::FlushLogs();
}

size_t TLocalClient::Run() {
  ThisFiber::SetName("RunClient");
  base::Histogram hist;

  LOG(INFO) << "RunClient " << p_->GetPoolIndex();

  vector<Fiber> fbs(drivers_.size());
  size_t res = 0;
  for (size_t i = 0; i < fbs.size(); ++i) {
    fbs[i] = MakeFiber([&, i] {
      ThisFiber::SetName(absl::StrCat("run/", i));
      res += drivers_[i]->Run(&hist);
    });
  }

  for (auto& fb : fbs)
    fb.Join();
  unique_lock<mutex> lk(lat_mu);
  lat_hist.Merge(hist);

  return res;
}

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  CHECK_GT(absl::GetFlag(FLAGS_port), 0);

  std::unique_ptr<ProactorPool> pp;

#ifdef __linux__
  if (absl::GetFlag(FLAGS_epoll)) {
    pp.reset(fb2::Pool::Epoll());
  } else {
    pp.reset(fb2::Pool::IOUring(256));
  }
#else
  pp.reset(fb2::Pool::Epoll());
#endif

  pp->Run();

  if (absl::GetFlag(FLAGS_connect).empty()) {
#ifdef __linux__
    if (!absl::GetFlag(FLAGS_epoll)) {
      pp->AwaitBrief([](unsigned, auto* pb) {
        fb2::UringProactor* up = static_cast<fb2::UringProactor*>(pb);
        up->RegisterBufferRing(0, absl::GetFlag(FLAGS_bufring_size), kBufLen);
      });
    }
#endif
    RunServer(pp.get());
  } else {
    CHECK_GT(absl::GetFlag(FLAGS_size), 0U);

    char ip_addr[INET6_ADDRSTRLEN];
    auto* proactor = pp->GetNextProactor();

    error_code ec = proactor->Await(
        [&] { return DnsResolve(absl::GetFlag(FLAGS_connect).c_str(), 0, ip_addr, proactor); });

    CHECK_EQ(0, ec.value()) << "Could not resolve " << absl::GetFlag(FLAGS_connect) << " " << ec;
    thread_local std::unique_ptr<TLocalClient> client;
    auto address = ::boost::asio::ip::make_address(ip_addr);
    tcp::endpoint ep{address, uint16_t(absl::GetFlag(FLAGS_port))};

    pp->AwaitFiberOnAll([&](auto* p) {
      client.reset(new TLocalClient(p));
      client->Connect(ep);
    });

    auto start = absl::GetCurrentTimeNanos();
    atomic_uint64_t num_reqs{0};

    pp->AwaitFiberOnAll([&](auto* p) { num_reqs.fetch_add(client->Run(), memory_order_relaxed); });

    auto dur = absl::GetCurrentTimeNanos() - start;
    size_t dur_ms = std::max<size_t>(1, dur / 1000000);

    CONSOLE_INFO << "Total time " << dur_ms << " ms, num reqs: " << num_reqs.load()
                 << " qps: " << (num_reqs.load() * 1000 / dur_ms) << "\n";
    CONSOLE_INFO << "Overall latency (usec) \n" << lat_hist.ToString();
  }
  pp->Stop();

  return 0;
}
