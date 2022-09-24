// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/epoll/epoll_socket.h"

#include <netinet/in.h>
#include <sys/epoll.h>

#include "base/logging.h"
#include "base/stl_util.h"

#define VSOCK(verbosity) VLOG(verbosity) << "sock[" << native_handle() << "] "
#define DVSOCK(verbosity) DVLOG(verbosity) << "sock[" << native_handle() << "] "

namespace util {
namespace epoll {

using namespace std;
using namespace boost;

namespace {

inline FiberSocket::error_code from_errno() {
  return FiberSocket::error_code(errno, std::system_category());
}

inline ssize_t posix_err_wrap(ssize_t res, FiberSocket::error_code* ec) {
  if (res == -1) {
    *ec = from_errno();
  } else if (res < 0) {
    LOG(WARNING) << "Bad posix error " << res;
  }
  return res;
}

}  // namespace

FiberSocket::FiberSocket(int fd) : LinuxSocketBase(fd, nullptr) {
}

FiberSocket::~FiberSocket() {
  error_code ec = Close();  // Quietly close.

  LOG_IF(WARNING, ec) << "Error closing socket " << ec << "/" << ec.message();
}

auto FiberSocket::Close() -> error_code {
  error_code ec;
  if (fd_ >= 0) {
    DVSOCK(1) << "Closing socket";

    int fd = native_handle();
    GetProactor()->Disarm(fd, arm_index_);
    posix_err_wrap(::close(fd), &ec);
    fd_ = -1;
  }
  return ec;
}

void FiberSocket::OnSetProactor() {
  if (fd_ >= 0) {
    CHECK_LT(arm_index_, 0);

    auto cb = [this](uint32 mask, EpollProactor* cntr) { Wakey(mask, cntr); };

    arm_index_ = GetProactor()->Arm(native_handle(), std::move(cb), EPOLLIN | EPOLLET);
  }
}

auto FiberSocket::Accept() -> AcceptResult {
  CHECK(proactor());

  sockaddr_in client_addr;
  socklen_t addr_len = sizeof(client_addr);
  error_code ec;

  int real_fd = native_handle();
  current_context_ = fibers::context::active();

  while (true) {
    int res =
        accept4(real_fd, (struct sockaddr*)&client_addr, &addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (res >= 0) {
      FiberSocket* fs = new FiberSocket;
      fs->fd_ = res << 3;  // we keep some flags in the first 3 bits of fd_.
      current_context_ = nullptr;
      return fs;
    }

    DCHECK_EQ(-1, res);

    if (errno != EAGAIN) {
      ec = from_errno();
      break;
    }

    current_context_->suspend();
  }
  current_context_ = nullptr;
  return nonstd::make_unexpected(ec);
}

auto FiberSocket::Connect(const endpoint_type& ep) -> error_code {
  CHECK_EQ(fd_, -1);
  CHECK(proactor() && proactor()->InMyThread());

  error_code ec;

  int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (posix_err_wrap(fd, &ec) < 0)
    return ec;

  fd_ = (fd << 3);
  OnSetProactor();
  current_context_ = fibers::context::active();

  epoll_event ev;
  ev.events = EPOLLIN | EPOLLOUT | EPOLLET;
  ev.data.u32 = arm_index_ + 1024;  // TODO: to fix it.

  CHECK_EQ(0, epoll_ctl(GetProactor()->ev_loop_fd(), EPOLL_CTL_MOD, fd, &ev));
  while (true) {
    int res = connect(fd, (const sockaddr*)ep.data(), ep.size());
    if (res == 0) {
      break;
    }

    if (errno != EINPROGRESS) {
      ec = from_errno();
      break;
    }

    DVLOG(2) << "Suspending " << fibers_ext::short_id(current_context_);
    current_context_->suspend();
    DVLOG(2) << "Resuming " << fibers_ext::short_id(current_context_);
  }
  current_context_ = nullptr;

  if (ec) {
    GetProactor()->Disarm(fd, arm_index_);
    if (close(fd) < 0) {
      LOG(WARNING) << "Could not close fd " << strerror(errno);
    }
    fd_ = -1;
  }

  ev.events = EPOLLIN | EPOLLET;
  CHECK_EQ(0, epoll_ctl(GetProactor()->ev_loop_fd(), EPOLL_CTL_MOD, fd, &ev));

  return ec;
}

auto FiberSocket::WriteSome(const iovec* ptr, uint32_t len) -> Result<size_t> {
  CHECK(proactor());
  CHECK_GT(len, 0U);
  CHECK_GE(fd_, 0);

  if (fd_ & IS_SHUTDOWN) {
    return nonstd::make_unexpected(std::make_error_code(std::errc::connection_aborted));
  }

  msghdr msg;
  memset(&msg, 0, sizeof(msg));
  msg.msg_iov = const_cast<iovec*>(ptr);
  msg.msg_iovlen = len;

  ssize_t res;
  int fd = native_handle();
  current_context_ = fibers::context::active();

  while (true) {
    res = sendmsg(fd, &msg, MSG_NOSIGNAL);
    if (res >= 0) {
      current_context_ = nullptr;
      return res;
    }

    DCHECK_EQ(res, -1);
    res = errno;

    if (res != EAGAIN) {
      break;
    }
    current_context_->suspend();
  }

  current_context_ = nullptr;

  // Error handling - finale part.
  if (!base::_in(res, {ECONNABORTED, EPIPE, ECONNRESET})) {
    LOG(FATAL) << "Unexpected error " << res << "/" << strerror(res);
  }

  if (res == EPIPE)  // We do not care about EPIPE that can happen when we shutdown our socket.
    res = ECONNABORTED;

  std::error_code ec(res, std::system_category());
  VSOCK(1) << "Error " << ec << " on " << RemoteEndpoint();

  return nonstd::make_unexpected(std::move(ec));
}

void FiberSocket::AsyncWriteSome(const iovec* v, uint32_t len, AsyncWriteCb cb) {
  auto res = WriteSome(v, len);
  cb(res);
}

auto FiberSocket::RecvMsg(const msghdr& msg, int flags) -> Result<size_t> {
  CHECK(proactor());
  CHECK_GE(fd_, 0);
  CHECK_GT(size_t(msg.msg_iovlen), 0U);

  if (fd_ & IS_SHUTDOWN) {
    return nonstd::make_unexpected(std::make_error_code(std::errc::connection_aborted));
  }

  int fd = native_handle();
  current_context_ = fibers::context::active();

  ssize_t res;
  while (true) {
    res = recvmsg(fd, const_cast<msghdr*>(&msg), flags);
    if (res > 0) {  // if res is 0, that means a peer closed the socket.
      current_context_ = nullptr;
      return res;
    }

    if (res == 0 || errno != EAGAIN) {
      break;
    }
    DVLOG(1) << "Suspending " << fd << "/" << fibers_ext::short_id(current_context_);
    current_context_->suspend();
  }

  current_context_ = nullptr;

  // Error handling - finale part.
  if (res == 0) {
    res = ECONNABORTED;
  } else {
    DCHECK_EQ(-1, res);
    res = errno;
  }

  DVSOCK(1) << "Got " << res;

  if (!base::_in(res, {ECONNABORTED, EPIPE, ECONNRESET})) {
    LOG(FATAL) << "sock[" << fd << "] Unexpected error " << res << "/" << strerror(res);
  }

  std::error_code ec(res, std::system_category());
  VSOCK(1) << "Error " << ec << " on " << RemoteEndpoint();

  return nonstd::make_unexpected(std::move(ec));
}

void FiberSocket::Wakey(uint32_t ev_mask, EpollProactor* cntr) {
  DVLOG(2) << "Wakey " << fd_ << "/" << ev_mask;

  // It could be that we scheduled current_context_ already but has not switched to it yet.
  // Meanwhile a new event has arrived that triggered this callback again.
  if (current_context_ && !current_context_->ready_is_linked()) {
    DVLOG(2) << "Wakey: Scheduling " << fibers_ext::short_id(current_context_);
    fibers::context::active()->schedule(current_context_);
  }
}

}  // namespace epoll
}  // namespace util
