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

#ifdef __linux__
constexpr int kEventMask = EPOLLIN | EPOLLOUT | EPOLLET | EPOLLRDHUP;

int AcceptSock(int fd) {
  sockaddr_storage client_addr;
  socklen_t addr_len = sizeof(client_addr);
  return accept4(fd, (struct sockaddr*)&client_addr, &addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
}

int CreateSockFd(int family) {
  return socket(family, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
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
  sockaddr_storage client_addr;
  socklen_t addr_len = sizeof(client_addr);
  int res = accept(fd, (struct sockaddr*)&client_addr, &addr_len);
  if (res >= 0) {
    SetNonBlocking(res);
    SetCloexec(res);
  }

  return res;
}

int CreateSockFd(int family) {
  int fd = socket(family, SOCK_STREAM, IPPROTO_TCP);
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

std::pair<bool, EpollSocket::Result<size_t>> EpollSocket::AsyncReq::Run(int fd, bool is_send) {
  msghdr msg;
  memset(&msg, 0, sizeof(msg));
  msg.msg_iov = vec;
  msg.msg_iovlen = len;

  ssize_t res;
  res = is_send ? sendmsg(fd, &msg, MSG_NOSIGNAL) : recvmsg(fd, &msg, 0);

  if (res > 0) {
    return {true, res};
  }

  if (res == 0) {
    CHECK(!is_send);  // can only happen with recvmsg
    return {true, MakeUnexpected(errc::connection_aborted)};
  }

  if (errno == EAGAIN)
    return {false, 0};

  return {true, make_unexpected(from_errno())};
}

EpollSocket::EpollSocket(int fd) : LinuxSocketBase(fd, nullptr), flags_(0) {
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
    DCHECK(!proactor() || proactor()->InMyThread());

    int fd = native_handle();
    DVSOCK(1) << "Closing socket";
    if (arm_index_ >= 0)
      GetProactor()->Disarm(fd, arm_index_);
    posix_err_wrap(::close(fd), &ec);
    fd_ = -1;
    arm_index_ = -1;

    // Cleanup pending requests
    if (async_write_pending_) {
      DCHECK(async_write_req_);
      async_write_req_->cb(MakeUnexpected(errc::operation_canceled));
      delete async_write_req_;
      async_write_req_ = nullptr;
      async_write_pending_ = 0;
    }

    if (async_read_pending_) {
      DCHECK(async_read_req_);
      async_read_req_->cb(MakeUnexpected(errc::operation_canceled));
      delete async_read_req_;
      async_read_req_ = nullptr;
      async_read_pending_ = 0;
    } else if (recv_hook_registered_) {
      delete on_recv_;
      on_recv_ = nullptr;
      recv_hook_registered_ = 0;
    }
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

  int fd = CreateSockFd(ep.address().is_v4() ? AF_INET : AF_INET6);
  if (posix_err_wrap(fd, &ec))
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
    if (arm_index_ >= 0) {
      GetProactor()->Disarm(fd, arm_index_);
      arm_index_ = -1;
    }
    if (close(fd) < 0) {
      LOG(WARNING) << "Could not close fd " << strerror(errno);
    }
    fd_ = -1;
  }

  return ec;
}

io::Result<size_t> EpollSocket::WriteSome(const iovec* ptr, uint32_t len) {
  CHECK(proactor());
  CHECK_GT(len, 0U);
  CHECK_GE(fd_, 0);
  DCHECK(!async_write_req_);

  error_code ec;
  do {
    Result<size_t> res = TrySend(ptr, len);
    if (res) {
      return res;
    }

    if (res.error().value() != EAGAIN) {
      ec = res.error();
      break;
    }
    PendingReq req(&write_req_);

    ec = req.Suspend(timeout());
  } while (!ec);

  // ETIMEDOUT can happen if a socket does not have keepalive enabled or for some reason
  // TCP connection did indeed stopped getting tcp keep alive packets.
  // ECANCELED if the operation was cancelled due to timeout, see PendingReq::Suspend().
  if (!base::_in(ec.value(), {ECONNABORTED, EPIPE, ECONNRESET, ECANCELED})) {
    LOG(ERROR) << "sock[" << native_handle() << "] Unexpected error " << ec.message() << " "
               << RemoteEndpoint();
  }

  VSOCK(1) << "Error " << ec << " on " << RemoteEndpoint();
  return make_unexpected(std::move(ec));
}

void EpollSocket::AsyncWriteSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb) {
  Result<size_t> res = TrySend(v, len);

  if (res || res.error().value() != EAGAIN) {
    cb(res);
    return;
  }

  CHECK(async_write_req_ == nullptr);  // we do not allow queuing multiple async requests.

  async_write_req_ = new AsyncReq(const_cast<iovec*>(v), len, std::move(cb));
  async_write_pending_ = 1;
}

// TODO implement async functionality
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

  // Wake any suspended fibers so that they can handle the shutdown event before the socket closes.
  // In the absence of this wake if the socket closes soon after shutdown without any yields in
  // between, suspended read/write fibers can hang indefinitely because socket close removes the fd
  // from epoll interest set and unsets the callback handler.
  Wakey(EpollProactor::EPOLL_IN | ProactorBase::EPOLL_OUT, 0, nullptr);

  return ec;
}

void EpollSocket::RegisterOnErrorCb(std::function<void(uint32_t)> cb) {
  DCHECK(!error_cb_);
  error_cb_ = std::move(cb);
}

void EpollSocket::CancelOnErrorCb() {
  error_cb_ = {};
}

void EpollSocket::RegisterOnRecv(std::function<void(const RecvNotification&)> cb) {
  CHECK(!async_read_pending_);
  CHECK(!recv_hook_registered_);

  int fd = native_handle();
  CHECK_GE(fd, 0);

  recv_hook_registered_ = 1;
  on_recv_ = new OnRecvRecord{std::move(cb)};

  // It is possible that by the time we register the hook there is already pending data.
  // TODO: a cleaner approach would be if epoll socket would track notifications itself.
  char buffer[2];  // A small buffer is sufficient for peeking
  ssize_t bytes_peeked = recv(fd, buffer, sizeof(buffer), MSG_PEEK | MSG_DONTWAIT);

  if (bytes_peeked > 0) {
    on_recv_->cb(RecvNotification{});
  } else if (bytes_peeked != -1 || errno != EAGAIN) {
    LOG(FATAL) << "TBD: " << bytes_peeked << " " << errno;
  }
}

void EpollSocket::ResetOnRecvHook() {
  if (recv_hook_registered_) {
    recv_hook_registered_ = 0;
    delete on_recv_;
    on_recv_ = nullptr;
  }
}

void EpollSocket::HandleAsyncRequest(error_code ec, bool is_send) {
  uint8_t async_pending = is_send ? async_write_pending_ : async_read_pending_;
  if (async_pending) {
    auto& async_request = is_send ? async_write_req_ : async_read_req_;
    DCHECK(async_request);

    auto finalize_and_fetch_cb = [this, &async_request, is_send]() {
      auto cb = std::move(async_request->cb);
      delete async_request;
      async_request = nullptr;
      if (is_send)
        async_write_pending_ = 0;
      else
        async_read_pending_ = 0;
      return cb;
    };

    if (ec) {
      auto cb = finalize_and_fetch_cb();
      cb(make_unexpected(ec));
    } else if (auto res = async_request->Run(native_handle(), is_send); res.first) {
      auto cb = finalize_and_fetch_cb();
      cb(res.second);
    }
  } else if (recv_hook_registered_ && !is_send && !ec) {
    on_recv_->cb(RecvNotification{});
  } else {
    auto& sync_request = is_send ? write_req_ : read_req_;
    // It could be that we activated context already, but has not switched to it yet.
    // Meanwhile a new event has arrived that triggered this callback again.
    if (sync_request && sync_request->IsSuspended()) {
      DVSOCK(2) << "Wakey: Schedule read in " << sync_request->name();
      sync_request->Activate(ec);
    }
  }
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
    HandleAsyncRequest(ec, false /*is_send */);
  }

  if (ev_mask & (EpollProactor::EPOLL_OUT | kErrMask)) {
    HandleAsyncRequest(ec, true /*is_send */);
  }

  if (error_cb_ && (ev_mask & kErrMask)) {
    error_cb_(ev_mask);
  }
}

}  // namespace fb2
}  // namespace util
