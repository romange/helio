// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/epoll_socket.h"

#include <errno.h>
#include <netinet/in.h>

#include "absl/cleanup/cleanup.h"

#ifdef __linux__
#include <sys/epoll.h>
#else
#include <sys/event.h>
#endif

#include "base/logging.h"
#include "base/stl_util.h"

#define VSOCK(verbosity) VLOG(verbosity) << "sock[" << native_handle() << "] "
#define DVSOCK(verbosity) DVLOG(verbosity) << "sock[" << native_handle() << "] "

namespace util {
namespace fb2 {

using namespace std;
using nonstd::make_unexpected;

namespace {

inline EpollSocket::error_code from_errno() {
  return EpollSocket::error_code(errno, system_category());
}

inline ssize_t posix_err_wrap(ssize_t res, EpollSocket::error_code* ec) {
  if (res == -1) {
    *ec = from_errno();
  } else if (res < 0) {
    LOG(WARNING) << "Bad posix error " << res;
  }
  return res;
}

nonstd::unexpected<error_code> MakeUnexpected(std::errc code) {
  return make_unexpected(make_error_code(code));
}

#ifdef __linux__
constexpr int kEventMask = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP;

int AcceptSock(int fd) {
  sockaddr_in client_addr;
  socklen_t addr_len = sizeof(client_addr);
  int res = accept4(fd, (struct sockaddr*)&client_addr, &addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
  return res;
}

int CreateSockFd() {
  return socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
}

/*void RegisterEvents(int poll_fd, int sock_fd, uint32_t user_data) {
  epoll_event ev;
  ev.events = kEventMask;
  ev.data.u32 = user_data;

  CHECK_EQ(0, epoll_ctl(poll_fd, EPOLL_CTL_MOD, sock_fd, &ev));
}*/

#elif defined(__FreeBSD__) || defined(__APPLE__)

constexpr int kEventMask = POLLIN | POLLOUT;

int AcceptSock(int fd) {
  sockaddr_in client_addr;
  socklen_t addr_len = sizeof(client_addr);
  int res = accept(fd, (struct sockaddr*)&client_addr, &addr_len);
  if (res >= 0) {
    SetNonBlocking(fd);
    SetCloexec(fd);
  }

  return res;
}

int CreateSockFd() {
  int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (fd >= 0) {
    SetNonBlocking(fd);
    SetCloexec(fd);
  }
  return fd;
}

/*
void RegisterEvents(int poll_fd, int sock_fd, uint32_t user_data) {
  struct kevent kev[2];
  uint64_t ud = user_data;
  EV_SET(&kev[0], sock_fd, EVFILT_WRITE, EV_ADD, 0, 0, (void*)ud);
  EV_SET(&kev[1], sock_fd, EVFILT_READ, EV_ADD, 0, 0, (void*)ud);
  CHECK_EQ(0, kevent(poll_fd, kev, 2, NULL, 0, NULL));
}
*/

#else
#error "Unsupported platform"
#endif

}  // namespace

class EpollSocket::PendingReq {
  error_code ec_;
  detail::FiberInterface* context_;
  PendingReq** dest_;

 public:
  PendingReq(PendingReq** dest) : context_(detail::FiberActive()), dest_(dest) {
    *dest_ = this;
  }

  ~PendingReq() {
    *dest_ = nullptr;
  }

  bool IsSuspended() const {
    return !context_->list_hook.is_linked();
  }

  string_view name() const {
    return context_->name();
  }

  error_code Suspend(uint32_t timeout);

  void Activate(error_code ec);
};

error_code EpollSocket::PendingReq::Suspend(uint32_t timeout) {
  bool timed_out = false;
  if (timeout == UINT32_MAX) {
    context_->Suspend();
  } else {
    timed_out = context_->WaitUntil(chrono::steady_clock::now() + chrono::milliseconds(timeout));
  }

  if (timed_out)
    return make_error_code(errc::operation_canceled);

  return this->ec_;
}

void EpollSocket::PendingReq::Activate(error_code ec) {
  ec_ = ec;

  ActivateSameThread(detail::FiberActive(), context_);
}

bool EpollSocket::AsyncReq::Run(int fd, bool is_send) {
  msghdr msg;
  memset(&msg, 0, sizeof(msg));
  msg.msg_iov = vec;
  msg.msg_iovlen = len;

  ssize_t res;
  res = is_send ? sendmsg(fd, &msg, MSG_NOSIGNAL) : recvmsg(fd, &msg, 0);

  if (res > 0) {
    cb(res);
    return true;
  }

  if (res == 0) {
    CHECK(!is_send);  // can only happen with recvmsg
    cb(MakeUnexpected(errc::connection_aborted));
    return true;
  }

  if (errno == EAGAIN)
    return false;

  error_code ec = from_errno();
  cb(make_unexpected(ec));
  return true;
}

EpollSocket::EpollSocket(int fd)
    : LinuxSocketBase(fd, nullptr), async_write_pending_(0), async_read_pending_(0) {
  write_req_ = read_req_ = nullptr;
}

EpollSocket::~EpollSocket() {
  DCHECK_LT(fd_, 0) << "Socket must have been closed explicitly.";
  error_code ec = Close();  // Quietly close.

  LOG_IF(WARNING, ec) << "Error closing socket " << ec << "/" << ec.message();
}

auto EpollSocket::Close() -> error_code {
  error_code ec;
  if (fd_ >= 0) {
    DCHECK_EQ(GetProactor()->thread_id(), pthread_self());

    int fd = native_handle();
    DVSOCK(1) << "Closing socket";
    if (arm_index_ >= 0)
      GetProactor()->Disarm(fd, arm_index_);
    posix_err_wrap(::close(fd), &ec);
    fd_ = -1;
    arm_index_ = -1;
  }
  return ec;
}

void EpollSocket::OnSetProactor() {
  if (fd_ >= 0) {
    CHECK_LT(arm_index_, 0);

    auto cb = [this](uint32 mask, int err, EpollProactor* cntr) { Wakey(mask, err, cntr); };

    arm_index_ = GetProactor()->Arm(native_handle(), std::move(cb), kEventMask);
    DVSOCK(2) << "OnSetProactor " << arm_index_;
  }
}

// A bit hacky code. I assume here that OnResetProactor is called in a differrent thread
// than of Proactor.
void EpollSocket::OnResetProactor() {
  if (arm_index_ >= 0) {
    GetProactor()->AwaitBrief([this] { GetProactor()->Disarm(native_handle(), arm_index_); });
    arm_index_ = -1;
  }
}

auto EpollSocket::Accept() -> AcceptResult {
  CHECK(proactor());

  error_code ec;

  int real_fd = native_handle();
  CHECK(read_req_ == NULL);

  do {
    if (fd_ & IS_SHUTDOWN) {
      return MakeUnexpected(errc::connection_aborted);
    }

    int res = AcceptSock(real_fd);
    if (res >= 0) {
      EpollSocket* fs = new EpollSocket;
      fs->fd_ = (res << kFdShift) | (fd_ & kInheritedFlags);
      return fs;
    }

    DCHECK_EQ(-1, res);

    if (errno != EAGAIN) {
      ec = from_errno();
      break;
    }

    PendingReq req(&read_req_);
    ec = req.Suspend(UINT32_MAX);
  } while (!ec);

  return make_unexpected(ec);
}

error_code EpollSocket::Connect(const endpoint_type& ep, std::function<void(int)> on_pre_connect) {
  CHECK_EQ(fd_, -1);
  CHECK(proactor() && proactor()->InMyThread());

  error_code ec;

  int fd = CreateSockFd();
  if (posix_err_wrap(fd, &ec) < 0)
    return ec;

  CHECK(read_req_ == NULL);
  CHECK(write_req_ == NULL);

  fd_ = (fd << kFdShift);
  OnSetProactor();

  if (on_pre_connect) {
    on_pre_connect(fd);
  }

  // Unlike with other socket operations, connect does not require a repeated attempt, and
  // in case of EINPROGRESS. It is enough to wait for the completion write event.
  DVSOCK(2) << "Connecting";

  int res = connect(fd, (const sockaddr*)ep.data(), ep.size());
  if (res == -1) {
    if (errno == EINPROGRESS) {
      PendingReq req(&write_req_);
      ec = req.Suspend(timeout());
    } else {
      ec = from_errno();
    }
  }

#ifndef __linux__
  if (!ec) {
    // On BSD we need to check for errors after connect. They come asynchronously, hence we
    // wait for some time to try and collect them.
    ThisFiber::SleepFor(chrono::milliseconds(1));
    int error = 0;
    socklen_t len = sizeof(error);
    getsockopt(fd, SOL_SOCKET, SO_ERROR, &error, &len);
    if (error) {
      ec = error_code(error, std::system_category());
    }
  }
#endif

  if (ec) {
    GetProactor()->Disarm(fd, arm_index_);
    if (close(fd) < 0) {
      LOG(WARNING) << "Could not close fd " << strerror(errno);
    }
    fd_ = -1;
  }

  return ec;
}

auto EpollSocket::WriteSome(const iovec* ptr, uint32_t len) -> Result<size_t> {
  CHECK(proactor());
  CHECK_GT(len, 0U);
  CHECK_GE(fd_, 0);
  DCHECK(!async_write_pending_);

  msghdr msg;
  memset(&msg, 0, sizeof(msg));
  msg.msg_iov = const_cast<iovec*>(ptr);
  msg.msg_iovlen = len;

  int fd = native_handle();
  error_code ec;

  do {
    if (fd_ & IS_SHUTDOWN) {
      ec = make_error_code(errc::broken_pipe);
      break;
    }

    ssize_t res = sendmsg(fd, &msg, MSG_NOSIGNAL);
    if (res >= 0) {
      return res;
    }

    DCHECK_EQ(res, -1);

    if (errno != EAGAIN) {
      ec = from_errno();
      break;
    }
    PendingReq req(&write_req_);

    ec = req.Suspend(timeout());
  } while (!ec);

  // ETIMEDOUT can happen if a socket does not have keepalive enabled or for some reason
  // TCP connection did indeed stopped getting tcp keep alive packets.
  if (!base::_in(ec.value(), {ECONNABORTED, EPIPE, ECONNRESET})) {
    LOG(ERROR) << "sock[" << fd << "] Unexpected error " << ec.message() << " " << RemoteEndpoint();
  }

  VSOCK(1) << "Error " << ec << " on " << RemoteEndpoint();
  return make_unexpected(std::move(ec));
}

void EpollSocket::AsyncWriteSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) {
  if (fd_ & IS_SHUTDOWN) {
    cb(make_unexpected(make_error_code(errc::broken_pipe)));
    return;
  }

  CHECK(async_write_req_ == nullptr);  // we do not allow queuing multiple async requests.

  AsyncReq req{const_cast<iovec*>(v), len, std::move(cb)};
  if (req.Run(native_handle(), true))
    return;

  async_write_req_ = new AsyncReq(std::move(req));
  async_write_pending_ = 1;
}

void EpollSocket::AsyncReadSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) {
  auto res = ReadSome(v, len);
  cb(res);
}

auto EpollSocket::RecvMsg(const msghdr& msg, int flags) -> Result<size_t> {
  CHECK(proactor());
  CHECK_GE(fd_, 0);
  CHECK_GT(size_t(msg.msg_iovlen), 0U);

  CHECK(read_req_ == NULL);

  int fd = native_handle();
  error_code ec;
  do {
    if (fd_ & IS_SHUTDOWN) {
      ec = make_error_code(errc::connection_aborted);
      break;
    }

    ssize_t res = recvmsg(fd, const_cast<msghdr*>(&msg), flags);
    if (res > 0) {  // if res is 0, that means a peer closed the socket.
      return res;
    }

    if (res == 0) {
      ec = make_error_code(errc::connection_aborted);
      break;
    }

    if (errno != EAGAIN) {
      ec = from_errno();
      break;
    }

    PendingReq req(&read_req_);
    ec = req.Suspend(timeout());
  } while (!ec);

  DVSOCK(1) << "Got " << ec.message();

  // ETIMEDOUT can happen if a socket does not have keepalive enabled or for some reason
  // TCP connection did indeed stopped getting tcp keep alive packets.
  if (!base::_in(ec.value(), {ECONNABORTED, EPIPE, ECONNRESET, ETIMEDOUT})) {
    LOG(ERROR) << "sock[" << fd << "] Unexpected error " << ec.message() << " " << RemoteEndpoint();
  }

  VSOCK(1) << "Error on " << RemoteEndpoint() << ": " << ec.message();

  return make_unexpected(std::move(ec));
}

unsigned EpollSocket::RecvProvided(unsigned buf_len, ProvidedBuffer* dest) {
  DCHECK_GT(buf_len, 0u);

  int fd = native_handle();
  error_code ec;
  do {
    if (fd_ & IS_SHUTDOWN) {
      ec = make_error_code(errc::broken_pipe);
      break;
    }

    io::MutableBytes buf = proactor()->AllocateBuffer(bufreq_sz_);
    ssize_t res = recv(fd, buf.data(), buf.size(), 0);
    if (res > 0) {  // if res is 0, that means a peer closed the socket.
      size_t ures = res;
      dest[0].cookie = 1;
      dest[0].err_no = 0;

      // Handle buffer shrinkage.
      if (bufreq_sz_ > kMinBufSize && ures < bufreq_sz_ / 2) {
        bufreq_sz_ = absl::bit_ceil(ures);
        io::MutableBytes buf2 = proactor()->AllocateBuffer(ures);
        DCHECK_GE(buf2.size(), ures);

        memcpy(buf2.data(), buf.data(), ures);
        proactor()->DeallocateBuffer(buf);
        dest[0].buffer = {buf2.data(), ures};
        dest[0].allocated = buf2.size();

        return 1;
      }

      dest[0].buffer = {buf.data(), ures};
      dest[0].allocated = buf.size();

      // Handle buffer expansion.
      unsigned num_bufs = 1;
      while (buf.size() == bufreq_sz_) {
        if (bufreq_sz_ < kMaxBufSize) {
          bufreq_sz_ *= 2;
        }

        if (num_bufs == buf_len)
          break;

        buf = proactor()->AllocateBuffer(bufreq_sz_);
        res = recv(fd, buf.data(), buf.size(), 0);
        if (res <= 0) {
          proactor()->DeallocateBuffer(buf);
          break;
        }
        ures = res;
        dest[num_bufs].buffer = {buf.data(), ures};
        dest[num_bufs].allocated = buf.size();
        dest[num_bufs].cookie = 1;
        dest[num_bufs].err_no = 0;
        ++num_bufs;
      }

      return num_bufs;
    }  // res > 0

    proactor()->DeallocateBuffer(buf);

    if (res == 0) {
      ec = make_error_code(errc::connection_aborted);
      break;
    }

    if (errno != EAGAIN) {
      ec = from_errno();
      break;
    }

    PendingReq req(&read_req_);
    ec = req.Suspend(timeout());
  } while (!ec);

  DVSOCK(1) << "Got " << ec.message();
  dest[0].SetError(ec.value());

  return 1;
}

void EpollSocket::ReturnProvided(const ProvidedBuffer& pbuf) {
  DCHECK_EQ(pbuf.cookie, 1);
  DCHECK(!pbuf.buffer.empty());

  proactor()->DeallocateBuffer(
      io::MutableBytes{const_cast<uint8_t*>(pbuf.buffer.data()), pbuf.allocated});
}

io::Result<size_t> EpollSocket::Recv(const io::MutableBytes& mb, int flags) {
  msghdr msg;
  memset(&msg, 0, sizeof(msg));
  iovec vec[1];

  msg.msg_iov = vec;
  msg.msg_iovlen = 1;
  vec[0].iov_base = mb.data();
  vec[0].iov_len = mb.size();
  return RecvMsg(msg, flags);
}

auto EpollSocket::Shutdown(int how) -> error_code {
  auto ec = LinuxSocketBase::Shutdown(how);

#ifdef __APPLE__
  // Since kqueue won't notify listen sockets when shutdown, explicitly wake
  // up any read contexts. Note this will do nothing if there is no
  // read_req_ so its safe to call multiple times.
  Wakey(EpollProactor::EPOLL_IN, 0, nullptr);
#endif

  return ec;
}

void EpollSocket::RegisterOnErrorCb(std::function<void(uint32_t)> cb) {
  DCHECK(!error_cb_);
  error_cb_ = std::move(cb);
}

void EpollSocket::CancelOnErrorCb() {
  error_cb_ = {};
}

void EpollSocket::Wakey(uint32_t ev_mask, int error, EpollProactor* cntr) {
  DVSOCK(2) << "Wakey " << ev_mask;
#ifdef __linux__
  constexpr uint32_t kErrMask = EPOLLERR | EPOLLHUP | EPOLLRDHUP;
#else
  constexpr uint32_t kErrMask = POLLERR | POLLHUP;
#endif

  error_code ec;
  if ((ev_mask & POLLERR) && error)
    ec = error_code{error, system_category()};
  else if (ev_mask & POLLHUP) {
    ec = make_error_code(errc::connection_aborted);
  }

  if (ev_mask & (EpollProactor::EPOLL_IN | kErrMask)) {
    if (async_read_pending_) {
      DCHECK(async_read_req_);

      auto finalize = [this] {
        delete async_read_req_;
        async_read_req_ = nullptr;
        async_read_pending_ = 0;
      };
      if (ec) {
        async_read_req_->cb(make_unexpected(ec));
        finalize();
      } else if (async_read_req_->Run(native_handle(), false)) {
        finalize();
      }
    } else {
      // It could be that we activated context already, but has not switched to it yet.
      // Meanwhile a new event has arrived that triggered this callback again.
      if (read_req_ && read_req_->IsSuspended()) {
        DVSOCK(2) << "Wakey: Schedule read in " << read_req_->name();
        read_req_->Activate(ec);
      }
    }
  }

  if (ev_mask & (EpollProactor::EPOLL_OUT | kErrMask)) {
    if (async_write_pending_) {
      DCHECK(async_write_req_);

      auto finalize = [this] {
        delete async_write_req_;
        async_write_req_ = nullptr;
        async_write_pending_ = 0;
      };

      if (ec) {
        async_write_req_->cb(make_unexpected(ec));
        finalize();
      } else if (async_write_req_->Run(native_handle(), true)) {
        finalize();
      }
    } else {
      // It could be that we activated context already but has not switched to it yet.
      // Meanwhile a new event has arrived that triggered this callback again.
      if (write_req_ && write_req_->IsSuspended()) {
        DVSOCK(2) << "Wakey: Schedule write in " << write_req_->name();
        write_req_->Activate(ec);
      }
    }
  }

  if (error_cb_ && (ev_mask & kErrMask)) {
    error_cb_(ev_mask);
  }
}

}  // namespace fb2
}  // namespace util
