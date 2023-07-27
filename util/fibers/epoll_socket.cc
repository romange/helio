// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/epoll_socket.h"

#include <netinet/in.h>

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

namespace {

inline EpollSocket::error_code from_errno() {
  return EpollSocket::error_code(errno, std::system_category());
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
  return nonstd::make_unexpected(make_error_code(code));
}

#ifdef __linux__
constexpr int kEventMask = EPOLLIN | EPOLLOUT | EPOLLET;

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
    int prev = fcntl(res, F_GETFL, 0);
    fcntl(res, F_SETFL, prev | FD_CLOEXEC | O_NONBLOCK);
  }

  return res;
}

int CreateSockFd() {
  int fd = socket(AF_INET, SOCK_STREAM, IPPROTO_TCP);
  if (fd >= 0) {
    int prev = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, prev | FD_CLOEXEC | O_NONBLOCK);
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

EpollSocket::EpollSocket(int fd) : LinuxSocketBase(fd, nullptr) {
}

EpollSocket::~EpollSocket() {
  DCHECK_LT(fd_, 0) << "Socket must have been closed explicitly.";
  error_code ec = Close();  // Quietly close.

  LOG_IF(WARNING, ec) << "Error closing socket " << ec << "/" << ec.message();
}

auto EpollSocket::Close() -> error_code {
  error_code ec;
  if (fd_ >= 0) {
    int fd = native_handle();
    DVSOCK(1) << "Closing socket";
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
  CHECK(read_context_ == NULL);

  read_context_ = detail::FiberActive();
  DVSOCK(2) << "Accepting from " << read_context_->name();

  while (true) {
    if (fd_ & IS_SHUTDOWN) {
      return MakeUnexpected(errc::connection_aborted);
    }

    int res = AcceptSock(real_fd);
    if (res >= 0) {
      EpollSocket* fs = new EpollSocket;
      fs->fd_ = res << kFdShift;  // we keep some flags in the first 3 bits of fd_.
      read_context_ = nullptr;
      return fs;
    }

    DCHECK_EQ(-1, res);

    if (errno != EAGAIN) {
      ec = from_errno();
      break;
    }

    read_context_->Suspend();
  }
  read_context_ = nullptr;
  return nonstd::make_unexpected(ec);
}

auto EpollSocket::Connect(const endpoint_type& ep) -> error_code {
  CHECK_EQ(fd_, -1);
  CHECK(proactor() && proactor()->InMyThread());

  error_code ec;

  int fd = CreateSockFd();
  if (posix_err_wrap(fd, &ec) < 0)
    return ec;

  CHECK(read_context_ == NULL);
  CHECK(write_context_ == NULL);

  fd_ = (fd << kFdShift);
  OnSetProactor();

  write_context_ = detail::FiberActive();

  // RegisterEvents(GetProactor()->ev_loop_fd(), fd, arm_index_ + 1024);

  DVSOCK(2) << "Connecting";

  while (true) {
    int res = connect(fd, (const sockaddr*)ep.data(), ep.size());
    if (res == 0) {
      break;
    }

    if (errno != EINPROGRESS) {
      ec = from_errno();
      break;
    }

    if (SuspendMyself(write_context_, &ec)) {
      break;
    }
  }

  write_context_ = nullptr;
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

  CHECK(write_context_ == NULL);

  msghdr msg;
  memset(&msg, 0, sizeof(msg));
  msg.msg_iov = const_cast<iovec*>(ptr);
  msg.msg_iovlen = len;

  ssize_t res;
  int fd = native_handle();
  write_context_ = detail::FiberActive();

  while (true) {
    if (fd_ & IS_SHUTDOWN) {
      res = ECONNABORTED;
      break;
    }

    res = sendmsg(fd, &msg, MSG_NOSIGNAL);
    if (res >= 0) {
      write_context_ = nullptr;
      return res;
    }

    DCHECK_EQ(res, -1);
    res = errno;

    if (res != EAGAIN) {
      break;
    }
    DVLOG(1) << "Suspending " << fd << "/" << write_context_->name();
    write_context_->Suspend();
  }

  write_context_ = nullptr;

  // ETIMEDOUT can happen if a socket does not have keepalive enabled or for some reason
  // TCP connection did indeed stopped getting tcp keep alive packets.
  if (!base::_in(res, {ECONNABORTED, EPIPE, ECONNRESET})) {
    LOG(ERROR) << "sock[" << fd << "] Unexpected error " << res << "/" << strerror(res) << " "
               << RemoteEndpoint();
  }

  if (res == EPIPE)  // We do not care about EPIPE that can happen when we shutdown our socket.
    res = ECONNABORTED;

  std::error_code ec(res, std::system_category());
  VSOCK(1) << "Error " << ec << " on " << RemoteEndpoint();

  return nonstd::make_unexpected(std::move(ec));
}

void EpollSocket::AsyncWriteSome(const iovec* v, uint32_t len, AsyncWriteCb cb) {
  auto res = WriteSome(v, len);
  cb(res);
}

auto EpollSocket::RecvMsg(const msghdr& msg, int flags) -> Result<size_t> {
  CHECK(proactor());
  CHECK_GE(fd_, 0);
  CHECK_GT(size_t(msg.msg_iovlen), 0U);

  CHECK(read_context_ == NULL);

  int fd = native_handle();
  read_context_ = detail::FiberActive();

  ssize_t res;
  error_code ec;
  while (true) {
    if (fd_ & IS_SHUTDOWN) {
      res = ECONNABORTED;
      break;
    }

    res = recvmsg(fd, const_cast<msghdr*>(&msg), flags);
    if (res > 0) {  // if res is 0, that means a peer closed the socket.
      read_context_ = nullptr;
      return res;
    }

    if (res == 0 || errno != EAGAIN) {
      break;
    }

    if (SuspendMyself(read_context_, &ec) && ec) {
      return nonstd::make_unexpected(std::move(ec));
    }
  }

  read_context_ = nullptr;

  // Error handling - finale part.
  if (res == -1) {
    res = errno;
  } else if (res == 0) {
    res = ECONNABORTED;
  }

  DVSOCK(1) << "Got " << res;

  // ETIMEDOUT can happen if a socket does not have keepalive enabled or for some reason
  // TCP connection did indeed stopped getting tcp keep alive packets.
  if (!base::_in(res, {ECONNABORTED, EPIPE, ECONNRESET})) {
    LOG(ERROR) << "sock[" << fd << "] Unexpected error " << res << "/" << strerror(res) << " "
               << RemoteEndpoint();
  }

  ec = std::error_code(res, std::system_category());
  VSOCK(1) << "Error " << ec << " on " << RemoteEndpoint();

  return nonstd::make_unexpected(std::move(ec));
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

uint32_t EpollSocket::PollEvent(uint32_t event_mask, std::function<void(uint32_t)> cb) {
  return 0;
}

uint32_t EpollSocket::CancelPoll(uint32_t id) {
  return 0;
}

bool EpollSocket::SuspendMyself(detail::FiberInterface* cntx, std::error_code* ec) {
  epoll_mask_ = 0;
  kev_error_ = 0;

  DVSOCK(2) << "Suspending " << cntx->name();
  if (timeout() == UINT32_MAX) {
    cntx->Suspend();
  } else {
    cntx->WaitUntil(chrono::steady_clock::now() + chrono::milliseconds(timeout()));
  }

  DVSOCK(2) << "Resuming " << cntx->name() << " em: " << epoll_mask_ << ", errno: " << kev_error_;

  if (epoll_mask_ & POLLERR) {
    *ec = error_code(kev_error_, system_category());
    return false;
  }
  if (epoll_mask_ & POLLHUP) {
    *ec = make_error_code(errc::connection_aborted);
  } else if (epoll_mask_ == 0) {  // timeout
    *ec = make_error_code(errc::operation_canceled);
  }
  return true;
}

void EpollSocket::Wakey(uint32_t ev_mask, int error, EpollProactor* cntr) {
  DVSOCK(2) << "Wakey " << ev_mask;
#ifdef __linux__
  constexpr uint32_t kErrMask = EPOLLERR | EPOLLHUP;
#else
  constexpr uint32_t kErrMask = POLLERR | POLLHUP;
#endif

  if (error)
    kev_error_ = error;

  if (ev_mask & (EpollProactor::EPOLL_IN | kErrMask)) {
    epoll_mask_ |= ev_mask;

    // It could be that we scheduled current_context_ already, but has not switched to it yet.
    // Meanwhile a new event has arrived that triggered this callback again.
    if (read_context_ && !read_context_->list_hook.is_linked()) {
      DVSOCK(2) << "Wakey: Schedule read ";
      detail::FiberActive()->ActivateOther(read_context_);
    }
  }

  if (ev_mask & (EpollProactor::EPOLL_OUT | kErrMask)) {
    epoll_mask_ |= ev_mask;

    // It could be that we scheduled current_context_ already but has not switched to it yet.
    // Meanwhile a new event has arrived that triggered this callback again.
    if (write_context_ && !write_context_->list_hook.is_linked()) {
      DVSOCK(2) << "Wakey: Schedule write ";
      detail::FiberActive()->ActivateOther(write_context_);
    }
  }
}

}  // namespace fb2
}  // namespace util
