// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

// clang-format off
#include <sys/time.h>
#include <sys/mman.h>

#include <linux/errqueue.h>
#include <linux/net_tstamp.h>
// clang-format on

#include <boost/asio/read.hpp>

#include "base/histogram.h"
#include "base/init.h"
#include "util/accept_server.h"
#include "util/asio_stream_adapter.h"
#include "util/http/http_handler.h"
#include "util/varz.h"

using namespace util;

#ifdef USE_FB2
#include "util/fibers/dns_resolve.h"
#include "util/fibers/pool.h"
#include "util/fibers/synchronization.h"

using fb2::DnsResolve;
using fb2::Fiber;
using fb2::Mutex;

#else
#include <boost/fiber/operations.hpp>

#include "util/dns_resolve.h"
#include "util/epoll/epoll_pool.h"
#include "util/fibers/fiber.h"
#include "util/uring/uring_fiber_algo.h"
#include "util/uring/uring_file.h"
#include "util/uring/uring_pool.h"
#include "util/uring/uring_socket.h"

using fibers_ext::Fiber;
using fibers_ext::Mutex;
using uring::Proactor;
using uring::SubmitEntry;
using uring::UringPool;
using uring::UringSocket;

#endif

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
ABSL_FLAG(string, write_file, "", "");
ABSL_FLAG(uint32, write_num, 1000, "");
ABSL_FLAG(uint32, max_pending_writes, 300, "");
ABSL_FLAG(bool, sqe_async, false, "");
ABSL_FLAG(bool, o_direct, true, "");
ABSL_FLAG(bool, raw, true,
          "If true, does not send/receive size parameter during "
          "the connection handshake");
ABSL_FLAG(bool, tcp_nodelay, false, "if true - use tcp_nodelay option for server sockets");

VarzQps ping_qps("ping-qps");
VarzCount connections("connections");

namespace {

constexpr size_t kInitialSize = 1UL << 20;
#ifndef USE_FB2
struct TL {
  std::unique_ptr<util::uring::LinuxFile> file;

  void Open(const string& path) {
    CHECK(!file);

    // | O_DIRECT
    int flags = O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC;
    if (GetFlag(FLAGS_o_direct))
      flags |= O_DIRECT;

    auto res = uring::OpenLinux(path, flags, 0666);
    CHECK(res);
    file = move(res.value());
#if 0
    Proactor* proactor = (Proactor*)ProactorBase::me();
    uring::FiberCall fc(proactor);
    fc->PrepFallocate(file->fd(), 0, 0, kInitialSize);
    uring::FiberCall::IoResult io_res = fc.Get();
    CHECK_EQ(0, io_res);
#endif
  }

  void WriteToFile();

  uint32_t pending_write_reqs = 0;
  uint32_t max_pending_reqs = 1024;
  fibers_ext::EventCount evc;
};

void TL::WriteToFile() {
  static void* blob = mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);

  auto ring_cb = [this](uring::Proactor::IoResult res, uint32_t flags, int64_t payload) {
    if (res < 0)
      LOG(ERROR) << "Error writing to file";

    if (this->pending_write_reqs == GetFlag(FLAGS_max_pending_writes)) {
      this->evc.notifyAll();
    }
    --this->pending_write_reqs;
  };

  evc.await([this] { return pending_write_reqs < GetFlag(FLAGS_max_pending_writes); });

  ++pending_write_reqs;
  uring::Proactor* proactor = (uring::Proactor*)ProactorBase::me();

  uring::SubmitEntry se = proactor->GetSubmitEntry(move(ring_cb), 0);
  se.PrepWrite(file->fd(), blob, 4096, 0);
  if (GetFlag(FLAGS_sqe_async))
    se.sqe()->flags |= IOSQE_ASYNC;

  if (pending_write_reqs > max_pending_reqs) {
    LOG(INFO) << "Pending write reqs: " << pending_write_reqs;
    max_pending_reqs = pending_write_reqs * 1.2;
  }
}

static thread_local TL* tl = nullptr;
#endif

}  // namespace

class EchoConnection : public Connection {
 public:
  EchoConnection() {
  }

 private:
  void HandleRequests() final;

  std::error_code ReadMsg(size_t* sz);

  std::unique_ptr<uint8_t[]> work_buf_;
  size_t req_len_ = 0;
};

std::error_code EchoConnection::ReadMsg(size_t* sz) {
  io::MutableBytes mb(work_buf_.get(), req_len_);

  /*LinuxSocketBase* sock = (LinuxSocketBase*)socket_.get();
  fb2::UringProactor* p = (fb2::UringProactor*)sock->proactor();
  fb2::FiberCall fc(p);
  // io_uring_prep_recv(sqe, fd, buf + offs, len, 0);

  fc->PrepRecv(sock->native_handle(), mb.data(), mb.size(), 0);
  fc.Get();
  *sz = req_len_;*/
  auto res = socket_->Recv(mb, 0);
#if 1
  if (res) {
    *sz = *res;
    CHECK_EQ(*sz, req_len_);
    return {};
  }

  return res.error();
#endif
  return error_code{};
}

static thread_local base::Histogram send_hist;

void EchoConnection::HandleRequests() {
  ThisFiber::SetName("HandleRequests");

  std::error_code ec;
  size_t sz;
  iovec vec[2];
  uint8_t buf[8];

  int yes = 1;
  LinuxSocketBase* sock = (LinuxSocketBase*)socket_.get();
  if (GetFlag(FLAGS_tcp_nodelay)) {
    CHECK_EQ(0, setsockopt(sock->native_handle(), IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)));
  }

  connections.IncBy(1);
  vec[0].iov_base = buf;
  vec[0].iov_len = 8;

  auto ep = sock->RemoteEndpoint();

  VLOG(1) << "New connection from " << ep;

  bool is_raw = GetFlag(FLAGS_raw);

  if (is_raw) {
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

    // size_t bs = asio::read(asa, asio::buffer(buf), ec);
    CHECK(es.has_value() && es.value() == sizeof(buf));
    req_len_ = absl::little_endian::Load64(buf);
  }

  CHECK_LE(req_len_, 1UL << 26);
  work_buf_.reset(new uint8_t[req_len_]);

  // after the handshake.
  while (true) {
    ec = ReadMsg(&sz);
    if (FiberSocketBase::IsConnClosed(ec)) {
      VLOG(1) << "Closing " << ep;
      break;
    }
    CHECK(!ec) << ec;
    ping_qps.Inc();

    vec[0].iov_base = buf;
    vec[0].iov_len = 4;
    absl::little_endian::Store32(buf, sz);
    vec[1].iov_base = work_buf_.get();
    vec[1].iov_len = sz;

#ifndef USE_FB2
    if (tl)
      tl->WriteToFile();
#endif
    if (is_raw) {
      auto prev = absl::GetCurrentTimeNanos();
      // send(sock->native_handle(), work_buf_.get(), sz, 0);
      ec = socket_->Write(vec + 1, 1);
      // socket_->Send(io::Bytes{work_buf_.get(), sz}, 0);
      auto now = absl::GetCurrentTimeNanos();
      send_hist.Add((now - prev) / 1000);
    } else {
      ec = socket_->Write(vec, 2);
    }
    if (ec)
      break;
  }

  VLOG(1) << "Connection ended " << ep;
  connections.IncBy(-1);
}

class EchoListener : public ListenerInterface {
 public:
  virtual Connection* NewConnection(ProactorBase* context) final {
    return new EchoConnection;
  }
};

void RunServer(ProactorPool* pp) {
  ping_qps.Init(pp);
  connections.Init(pp);

  AcceptServer acceptor(pp);
  acceptor.set_back_log(GetFlag(FLAGS_backlog));

  acceptor.AddListener(GetFlag(FLAGS_port), new EchoListener);
  if (GetFlag(FLAGS_http_port) >= 0) {
    uint16_t port = acceptor.AddListener(GetFlag(FLAGS_http_port), new HttpListener<>);
    LOG(INFO) << "Started http server on port " << port;
  }

#ifndef USE_FB2
  if (!GetFlag(FLAGS_write_file).empty()) {
    static void* blob =
        mmap(NULL, 4096, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS, -1, 0);
    CHECK(MAP_FAILED != blob);

    auto write_file = [](unsigned index, ProactorBase* pb) {
      string file = absl::StrCat(GetFlag(FLAGS_write_file), "-",
                                 absl::Dec(pb->GetIndex(), absl::kZeroPad4), ".bin");
      tl = new TL;
      tl->Open(file);

#if 0
      Proactor* proactor = (Proactor*)pb;

      for (unsigned i = 0; i < FLAGS_write_num; ++i) {
        auto ring_cb = [](uring::Proactor::IoResult res, uint32_t flags, int64_t payload) {
          if (res < 0)
            LOG(ERROR) << "Error writing to file " << -res;
        };

        uring::SubmitEntry se = proactor->GetSubmitEntry(move(ring_cb), 0);
        se.PrepWrite(tl->file->fd(), blob, 4096, 0);
        se.sqe()->flags |= IOSQE_ASYNC;
        if (i % 1000 == 0) {
          fibers_ext::Yield();
        }
      }
      LOG(INFO) << "Write file finished " << index;
#endif
    };

    pp->AwaitFiberOnAll(write_file);
    LOG(INFO) << "All Writes finished ";
  }
#endif

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
  std::unique_ptr<LinuxSocketBase> socket_;

  Driver(const Driver&) = delete;

 public:
  Driver(ProactorBase* p);

  void Connect(unsigned index, const tcp::endpoint& ep);
  size_t Run(base::Histogram* dest);

 private:
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

    socket_->Close();
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

size_t Driver::Run(base::Histogram* dest) {
  base::Histogram hist;

  std::unique_ptr<uint8_t[]> msg(new uint8_t[absl::GetFlag(FLAGS_size)]);

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
  size_t req_size = absl::GetFlag(FLAGS_size);
  size_t pipeline_cnt = absl::GetFlag(FLAGS_p);
  bool is_raw = GetFlag(FLAGS_raw);

  for (; i < absl::GetFlag(FLAGS_n); ++i) {
    auto start = absl::GetCurrentTimeNanos();

    for (size_t j = 0; j < pipeline_cnt; ++j) {
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

  socket_->Shutdown(SHUT_RDWR);
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
  google::FlushLogFiles(google::INFO);
}

size_t TLocalClient::Run() {
  ThisFiber::SetName("RunClient");
  base::Histogram hist;

  LOG(INFO) << "RunClient " << p_->GetIndex();

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
#ifdef USE_FB2
  if (absl::GetFlag(FLAGS_epoll)) {
    pp.reset(fb2::Pool::Epoll());
  } else {
    pp.reset(fb2::Pool::IOUring(256));
  }
#else
  if (absl::GetFlag(FLAGS_epoll)) {
    pp.reset(new epoll::EpollPool);
  } else {
    pp.reset(new uring::UringPool(256));
  }
#endif
  pp->Run();

  if (absl::GetFlag(FLAGS_connect).empty()) {
    RunServer(pp.get());
  } else {
    CHECK_GT(absl::GetFlag(FLAGS_size), 0U);

    char ip_addr[INET6_ADDRSTRLEN];
#ifdef USE_FB2
    auto* proactor = pp->GetNextProactor();

    error_code ec = proactor->Await(
        [&] { return DnsResolve(absl::GetFlag(FLAGS_connect).c_str(), 0, ip_addr, proactor); });
#else
    error_code ec = DnsResolve(absl::GetFlag(FLAGS_connect).c_str(), 0, ip_addr);
#endif

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
