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

}  // namespace

EpollSocket::EpollSocket(int fd) : LinuxSocketBase(fd, nullptr) {
}

EpollSocket::~EpollSocket() {
  error_code ec = Close();  // Quietly close.

  LOG_IF(WARNING, ec) << "Error closing socket " << ec << "/" << ec.message();
}

auto EpollSocket::Close() -> error_code {
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

void EpollSocket::OnSetProactor() {
  if (fd_ >= 0) {
    CHECK_LT(arm_index_, 0);

    auto cb = [this](uint32 mask, EpollProactor* cntr) { Wakey(mask, cntr); };

    arm_index_ = GetProactor()->Arm(native_handle(), std::move(cb), EPOLLIN | EPOLLOUT | EPOLLET);
  }
}

auto EpollSocket::Accept() -> AcceptResult {
  CHECK(proactor());

  sockaddr_in client_addr;
  socklen_t addr_len = sizeof(client_addr);
  error_code ec;

  int real_fd = native_handle();
  CHECK(read_context_ == NULL);

  read_context_ = fibers::context::active();

  while (true) {
    if (fd_ & IS_SHUTDOWN) {
      return MakeUnexpected(errc::connection_aborted);
    }

    int res =
        accept4(real_fd, (struct sockaddr*)&client_addr, &addr_len, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (res >= 0) {
      EpollSocket* fs = new EpollSocket;
      fs->fd_ = res << 3;  // we keep some flags in the first 3 bits of fd_.
      read_context_ = nullptr;
      return fs;
    }

    DCHECK_EQ(-1, res);

    if (errno != EAGAIN) {
      ec = from_errno();
      break;
    }

    read_context_->suspend();
  }
  read_context_ = nullptr;
  return nonstd::make_unexpected(ec);
}

auto EpollSocket::Connect(const endpoint_type& ep) -> error_code {
  CHECK_EQ(fd_, -1);
  CHECK(proactor() && proactor()->InMyThread());

  error_code ec;

  int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (posix_err_wrap(fd, &ec) < 0)
    return ec;

  CHECK(read_context_ == NULL);
  CHECK(write_context_ == NULL);

  fd_ = (fd << 3);
  OnSetProactor();
  write_context_ = fibers::context::active();

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

    DVLOG(2) << "Suspending " << fd << "/" << fibers_ext::short_id(write_context_);
    write_context_->suspend();
    DVLOG(2) << "Resuming " << fibers_ext::short_id(write_context_);
  }
  write_context_ = nullptr;

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
  write_context_ = fibers::context::active();

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
    DVLOG(1) << "Suspending " << fd << "/" << fibers_ext::short_id(write_context_);
    write_context_->suspend();
  }

  write_context_ = nullptr;

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
  read_context_ = fibers::context::active();

  ssize_t res;
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
    DVLOG(1) << "Suspending " << fd << "/" << fibers_ext::short_id(read_context_);
    read_context_->suspend();
  }

  read_context_ = nullptr;

  // Error handling - finale part.
  if (res == -1) {
    res = errno;
  } else if (res == 0) {
    res = ECONNABORTED;
  }

  DVSOCK(1) << "Got " << res;

  if (!base::_in(res, {ECONNABORTED, EPIPE, ECONNRESET})) {
    LOG(FATAL) << "sock[" << fd << "] Unexpected error " << res << "/" << strerror(res);
  }

  std::error_code ec(res, std::system_category());
  VSOCK(1) << "Error " << ec << " on " << RemoteEndpoint();

  return nonstd::make_unexpected(std::move(ec));
}

uint32_t EpollSocket::PollEvent(uint32_t event_mask, std::function<void(uint32_t)> cb) {
  return 0;
}

uint32_t EpollSocket::CancelPoll(uint32_t id) {
  return 0;
}

void EpollSocket::Wakey(uint32_t ev_mask, EpollProactor* cntr) {
  DVLOG(2) << "Wakey " << native_handle() << "/" << ev_mask;

  constexpr uint32_t kErrMask = EPOLLERR | EPOLLHUP;

  if (ev_mask & (EPOLLIN | kErrMask)) {
    // It could be that we scheduled current_context_ already but has not switched to it yet.
    // Meanwhile a new event has arrived that triggered this callback again.
    if (read_context_ && !read_context_->ready_is_linked()) {
      DVLOG(2) << "Wakey: Schedule read " << native_handle();
      fibers::context::active()->schedule(read_context_);
    }
  }

  if (ev_mask & (EPOLLOUT | kErrMask)) {
    // It could be that we scheduled current_context_ already but has not switched to it yet.
    // Meanwhile a new event has arrived that triggered this callback again.
    if (write_context_ && !write_context_->ready_is_linked()) {
      DVLOG(2) << "Wakey: Schedule write " << native_handle();
      fibers::context::active()->schedule(write_context_);
    }
  }
}

}  // namespace epoll
}  // namespace util
