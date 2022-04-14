// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

// clang-format off
#include <sys/time.h>

#include <linux/errqueue.h>
#include <linux/net_tstamp.h>
// clang-format on

#include <boost/asio/read.hpp>
#include <boost/fiber/operations.hpp>

#include "base/histogram.h"
#include "base/init.h"
#include "util/accept_server.h"
#include "util/asio_stream_adapter.h"
#include "util/epoll/ev_pool.h"
#include "util/http/http_handler.h"
#include "util/uring/uring_fiber_algo.h"
#include "util/uring/uring_file.h"
#include "util/uring/uring_pool.h"
#include "util/uring/uring_socket.h"
#include "util/varz.h"

using namespace std;
using namespace util;
using uring::Proactor;
using uring::SubmitEntry;
using uring::UringPool;
using uring::UringSocket;
using tcp = ::boost::asio::ip::tcp;

using IoResult = Proactor::IoResult;
using ::boost::fibers::fiber;

DEFINE_bool(epoll, false, "If true use epoll for server");
DEFINE_int32(http_port, 8080, "Http port.");
DEFINE_int32(port, 8081, "Echo server port");
DEFINE_uint32(n, 1000, "Number of requests per connection");
DEFINE_uint32(c, 10, "Number of connections per thread");
DEFINE_uint32(size, 1, "Message size, 0 for hardcoded 4 byte pings");
DEFINE_uint32(backlog, 1024, "Accept queue length");
DEFINE_uint32(p, 1, "pipelining factor");
DEFINE_string(connect, "", "hostname or ip address to connect to in client mode");
DEFINE_string(write_file, "", "");
// DEFINE_uint32(write_num, 1000, "");

VarzQps ping_qps("ping-qps");
VarzCount connections("connections");

namespace {

// Returns 0 on success.
int ResolveDns(string_view host, char* dest) {
  struct addrinfo hints, *servinfo;

  memset(&hints, 0, sizeof(hints));
  hints.ai_family = AF_UNSPEC;
  hints.ai_socktype = SOCK_STREAM;
  hints.ai_protocol = IPPROTO_TCP;
  hints.ai_flags = AI_ALL;

  int res = getaddrinfo(host.data(), NULL, &hints, &servinfo);
  if (res != 0)
    return res;

  static_assert(INET_ADDRSTRLEN < INET6_ADDRSTRLEN, "");

  res = EAI_FAMILY;
  for (addrinfo* p = servinfo; p != NULL; p = p->ai_next) {
    if (p->ai_family == AF_INET) {
      struct sockaddr_in* ipv4 = (struct sockaddr_in*)p->ai_addr;
      const char* inet_res = inet_ntop(p->ai_family, &ipv4->sin_addr, dest, INET6_ADDRSTRLEN);
      CHECK_NOTNULL(inet_res);
      res = 0;
      break;
    }
    LOG(WARNING) << "Only IPv4 is supported";
  }

  freeaddrinfo(servinfo);

  return res;
}
}  // namespace

class EchoConnection : public Connection {
 public:
  EchoConnection() {
  }

 private:
  void HandleRequests() final;
  ::boost::system::error_code ReadMsg(size_t* sz);

  std::unique_ptr<uint8_t[]> work_buf_;
  size_t req_len_ = 0;
};

::boost::system::error_code EchoConnection::ReadMsg(size_t* sz) {
  ::boost::system::error_code ec;
  AsioStreamAdapter<FiberSocketBase> asa(*socket_);

  size_t bs = ::boost::asio::read(asa, ::boost::asio::buffer(work_buf_.get(), req_len_), ec);
  CHECK(ec || bs == req_len_);

  *sz = bs;
  return ec;
}

constexpr size_t kInitialSize = 1UL << 20;
struct TL {
  std::unique_ptr<util::uring::LinuxFile> file;

  void Open(const string& path) {
    CHECK(!file);

    auto res = uring::OpenLinux(path, O_CREAT | O_WRONLY | O_TRUNC | O_CLOEXEC, 0666);
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
};

static thread_local TL* tl = nullptr;

void EchoConnection::HandleRequests() {
  boost::this_fiber::properties<FiberProps>().set_name("HandleRequests");

  ::boost::system::error_code ec;
  size_t sz;
  iovec vec[2];
  uint8_t buf[8];

  int yes = 1;
  LinuxSocketBase* sock = (LinuxSocketBase*)socket_.get();
  CHECK_EQ(0, setsockopt(sock->native_handle(), IPPROTO_TCP, TCP_NODELAY, &yes, sizeof(yes)));

  connections.IncBy(1);
  AsioStreamAdapter<FiberSocketBase> asa(*socket_);
  vec[0].iov_base = buf;
  vec[0].iov_len = 8;

  auto ep = sock->RemoteEndpoint();
  VLOG(1) << "Waiting for size header from " << ep;
  auto es = socket_->Recv(buf);
  if (!es.has_value()) {
    if (es.error().value() == ECONNABORTED)
      return;
    LOG(FATAL) << "Bad Conn Handshake " << es.error() << " for socket " << ep;
  } else {
    CHECK_EQ(es.value(), 8U);
    VLOG(1) << "Received size from " << ep;
    uint8_t val = 1;
    socket_->Send(io::Bytes(&val, 1));
  }

  // size_t bs = asio::read(asa, asio::buffer(buf), ec);
  CHECK(es.has_value() && es.value() == sizeof(buf));
  req_len_ = absl::little_endian::Load64(buf);

  CHECK_LE(req_len_, 1UL << 18);
  work_buf_.reset(new uint8_t[req_len_]);

  uring::Proactor* proactor = (uring::Proactor*)ProactorBase::me();

  static thread_local uint32_t pending_write_reqs = 0;
  static thread_local uint32_t max_pending_reqs = 1024;

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

    if (tl) {  // write file
      static string blob(4096, 'X');

      auto ring_cb = [&](uring::Proactor::IoResult res, uint32_t flags, int64_t payload) {
        if (res < 0)
          LOG(ERROR) << "Error writing to file";
        --pending_write_reqs;
      };
      ++pending_write_reqs;
      uring::SubmitEntry se = proactor->GetSubmitEntry(move(ring_cb), 0);
      se.PrepWrite(tl->file->fd(), blob.data(), blob.size(), 0);
      se.sqe()->flags |= IOSQE_ASYNC;

      if (pending_write_reqs > max_pending_reqs) {
        LOG(INFO) << "Pending write reqs: " << pending_write_reqs;
        max_pending_reqs = pending_write_reqs * 1.1;
      }
    }
    auto res = socket_->Send(vec, 2);
    if (!res)
      break;
  }
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
  acceptor.set_back_log(FLAGS_backlog);

  acceptor.AddListener(FLAGS_port, new EchoListener);
  if (FLAGS_http_port >= 0) {
    uint16_t port = acceptor.AddListener(FLAGS_http_port, new HttpListener<>);
    LOG(INFO) << "Started http server on port " << port;
  }

  if (!FLAGS_write_file.empty()) {
    auto write_file = [](unsigned index, ProactorBase* pb) {
      string file =
          absl::StrCat(FLAGS_write_file, "-", absl::Dec(pb->GetIndex(), absl::kZeroPad4), ".bin");
      tl = new TL;
      tl->Open(file);

#if 0
      static string blob(4096, 'X');
      Proactor* proactor = (Proactor*)pb;

      for (unsigned i = 0; i < FLAGS_write_num; ++i) {
        auto ring_cb = [](uring::Proactor::IoResult res, uint32_t flags, int64_t payload) {
          if (res < 0)
            LOG(ERROR) << "Error writing to file";
        };

        uring::SubmitEntry se = proactor->GetSubmitEntry(move(ring_cb), 0);
        se.PrepWrite(tl->file->fd(), blob.data(), blob.size(), 0);
        if (i % 1000 == 0)
          boost::this_fiber::sleep_for(1us);
      }
      LOG(INFO) << "Write file finished " << index;
#endif
    };

    pp->AwaitFiberOnAll(write_file);
  }

  acceptor.Run();
  acceptor.Wait();
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
  for (; iter < kMaxIter; ++iter) {
    uint64_t start = absl::GetCurrentTimeNanos();
    auto ec = socket_->Connect(ep);
    CHECK(!ec) << ec.message();
    VLOG(1) << "Connected to " << socket_->RemoteEndpoint();

    uint64_t start1 = absl::GetCurrentTimeNanos();
    uint64_t delta_msec = (start1 - start) / 1000000;
    LOG_IF(ERROR, delta_msec > 1000) << "Slow connect1 " << index << " " << delta_msec << " ms";

    // Send msg size.
    absl::little_endian::Store64(buf_, FLAGS_size);
    io::Result<size_t> es = socket_->Send(buf_);
    CHECK(es) << es.error();  // Send expected payload size.
    CHECK_EQ(8U, es.value());

    uint64_t start2 = absl::GetCurrentTimeNanos();
    delta_msec = (start2 - start) / 1000000;
    LOG_IF(ERROR, delta_msec > 2000) << "Slow connect2 " << index << " " << delta_msec << " ms";

    es = socket_->Recv(io::MutableBytes(buf_, 1));
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
  CHECK_LT(iter, kMaxIter) << "Maximum reconnects reached";
  VLOG(1) << "Driver::Connect-End " << index;
}

size_t Driver::Run(base::Histogram* dest) {
  base::Histogram hist;

  std::unique_ptr<uint8_t[]> msg(new uint8_t[FLAGS_size]);

  iovec vec[2];
  vec[0].iov_len = 4;
  vec[0].iov_base = buf_;
  vec[1].iov_len = FLAGS_size;
  vec[1].iov_base = msg.get();

  auto lep = socket_->LocalEndpoint();
  CHECK(socket_->IsOpen());

  AsioStreamAdapter<> adapter(*socket_);
  io::Result<size_t> es;

  bool conn_close = false;
  size_t i = 0;
  for (; i < FLAGS_n; ++i) {
    auto start = absl::GetCurrentTimeNanos();

    for (size_t j = 0; j < FLAGS_p; ++j) {
      es = socket_->Send(io::Bytes{msg.get(), FLAGS_size});
      if (!es && FiberSocketBase::IsConnClosed(es.error())) {
        conn_close = true;
        break;
      }
      CHECK(es.has_value()) << es.error();
      CHECK_EQ(es.value(), FLAGS_size);
    }

    if (conn_close)
      break;

    for (size_t j = 0; j < FLAGS_p; ++j) {
      // DVLOG(1) << "Recv " << lep << " " << i;
      es = socket_->Recv(vec, 1);
      if (!es && FiberSocketBase::IsConnClosed(es.error())) {
        conn_close = true;
        break;
      }

      CHECK(es.has_value()) << "RecvError: " << es.error() << "/" << lep;

      ::boost::system::error_code ec;
      size_t sz = ::boost::asio::read(adapter, ::boost::asio::buffer(msg.get(), FLAGS_size), ec);
      CHECK(!ec) << ec;
      CHECK_EQ(sz, FLAGS_size);
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
    drivers_.resize(FLAGS_c);
    for (size_t i = 0; i < drivers_.size(); ++i) {
      drivers_[i].reset(new Driver{p});
    }
  }

  void Connect(tcp::endpoint ep);
  size_t Run();
};

void TLocalClient::Connect(tcp::endpoint ep) {
  LOG(INFO) << "TLocalClient::Connect-Start";
  vector<::boost::fibers::fiber> fbs(drivers_.size());
  for (size_t i = 0; i < fbs.size(); ++i) {
    fbs[i] = ::boost::fibers::fiber([&, i] {
      ::boost::this_fiber::properties<FiberProps>().set_name(absl::StrCat("connect/", i));
      uint64_t start = absl::GetCurrentTimeNanos();
      drivers_[i]->Connect(i, ep);
      uint64_t delta_msec = (absl::GetCurrentTimeNanos() - start) / 1000000;
      LOG_IF(ERROR, delta_msec > 4000) << "Slow DriverConnect " << delta_msec << " ms";
    });
  }
  for (auto& fb : fbs)
    fb.join();
  LOG(INFO) << "TLocalClient::Connect-End";
  google::FlushLogFiles(google::INFO);
}

size_t TLocalClient::Run() {
  ::boost::this_fiber::properties<FiberProps>().set_name("RunClient");
  base::Histogram hist;

  LOG(INFO) << "RunClient " << p_->GetIndex();

  vector<fiber> fbs(drivers_.size());
  size_t res = 0;
  for (size_t i = 0; i < fbs.size(); ++i) {
    fbs[i] = fiber([&, i] {
      ::boost::this_fiber::properties<FiberProps>().set_name(absl::StrCat("run/", i));
      res += drivers_[i]->Run(&hist);
    });
  }

  for (auto& fb : fbs)
    fb.join();
  unique_lock<mutex> lk(lat_mu);
  lat_hist.Merge(hist);

  return res;
}

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  CHECK_GT(FLAGS_port, 0);

  std::unique_ptr<ProactorPool> pp;
  if (FLAGS_epoll) {
    pp.reset(new epoll::EvPool);
  } else {
    pp.reset(new UringPool);
  }
  pp->Run();

  if (FLAGS_connect.empty()) {
    RunServer(pp.get());
  } else {
    CHECK_GT(FLAGS_size, 0U);

    char ip_addr[INET6_ADDRSTRLEN];

    int resolve_err = ResolveDns(FLAGS_connect, ip_addr);
    CHECK_EQ(0, resolve_err) << "Could not resolve " << FLAGS_connect << " "
                             << gai_strerror(resolve_err);
    thread_local std::unique_ptr<TLocalClient> client;
    auto address = ::boost::asio::ip::make_address(ip_addr);
    tcp::endpoint ep{address, uint16_t(FLAGS_port)};

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
