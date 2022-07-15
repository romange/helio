// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/uring/uring_socket.h"

#include <netinet/in.h>
#include <poll.h>

#include <boost/fiber/context.hpp>

#include "base/logging.h"
#include "base/stl_util.h"
#include "util/uring/proactor.h"

#define VSOCK(verbosity) VLOG(verbosity) << "sock[" << native_handle() << "] "
#define DVSOCK(verbosity) DVLOG(verbosity) << "sock[" << native_handle() << "] "

namespace util {
namespace uring {

using namespace std;
using IoResult = Proactor::IoResult;
using nonstd::make_unexpected;

namespace {

inline ssize_t posix_err_wrap(ssize_t res, UringSocket::error_code* ec) {
  if (res == -1) {
    *ec = UringSocket::error_code(errno, system_category());
  } else if (res < 0) {
    LOG(WARNING) << "Bad posix error " << res;
  }
  return res;
}

auto Unexpected(std::errc e) {
  return make_unexpected(make_error_code(e));
}

}  // namespace

UringSocket::~UringSocket() {
  error_code ec = Close();  // Quietly close.

  LOG_IF(WARNING, ec) << "Error closing socket " << ec << "/" << ec.message();
}

auto UringSocket::Close() -> error_code {
  error_code ec;
  if (fd_ >= 0) {
    DVSOCK(1) << "Closing socket";

    int fd = native_handle();
    Proactor* p = GetProactor();
    if ((fd_ & REGISTER_FD) && p) {
      unsigned fixed_fd = fd;
      fd = p->TranslateFixedFd(fixed_fd);
      p->UnregisterFd(fixed_fd);
    }
    posix_err_wrap(::close(fd), &ec);
    fd_ = -1;
  }
  return ec;
}

auto UringSocket::Accept() -> AcceptResult {
  CHECK(proactor());

  error_code ec;

  int fd = native_handle();
  int real_fd = (fd_ & REGISTER_FD) ? GetProactor()->TranslateFixedFd(fd) : fd;
  while (true) {
    int res = accept4(real_fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (res >= 0) {
      UringSocket* fs = new UringSocket{nullptr};
      fs->fd_ = res << 3;
      return fs;
    }

    DCHECK_EQ(-1, res);

    if (errno == EAGAIN) {
      FiberCall fc(GetProactor());
      fc->PrepPollAdd(fd, POLLIN);
      fc->sqe()->flags |= register_flag();
      IoResult io_res = fc.Get();

      // tcp sockets set POLLERR but UDS set POLLHUP.
      if ((io_res & (POLLERR | POLLHUP)) != 0) {
        return Unexpected(errc::connection_aborted);
      }

      continue;
    }

    posix_err_wrap(res, &ec);
    return make_unexpected(ec);
  }
}

auto UringSocket::Connect(const endpoint_type& ep) -> error_code {
  CHECK_EQ(fd_, -1);
  CHECK(proactor() && proactor()->InMyThread());

  error_code ec;

  int fd = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (posix_err_wrap(fd, &ec) < 0)
    return ec;

  Proactor* p = GetProactor();
  CHECK(!p->HasSqPoll()) << "Not supported with SQPOLL, TBD";

  unsigned dense_id = fd;

  if (p->HasRegisterFd()) {
    dense_id = p->RegisterFd(fd);
    fd_ = (dense_id << 3) | REGISTER_FD;
  } else {
    fd_ = (dense_id << 3);
  }

  IoResult io_res;
  ep.data();

  FiberCall fc(p, timeout());
  fc->PrepConnect(dense_id, (const sockaddr*)ep.data(), ep.size());
  fc->sqe()->flags |= register_flag();
  io_res = fc.Get();

  if (io_res < 0) {  // In that case connect returns -errno.
    ec = error_code(-io_res, system_category());
  } else {
    // Not sure if this check is needed, to be on the safe side.
    int serr = 0;
    socklen_t slen = sizeof(serr);
    CHECK_EQ(0, getsockopt(fd, SOL_SOCKET, SO_ERROR, &serr, &slen));
    CHECK_EQ(0, serr);
  }
  return ec;
}

auto UringSocket::WriteSome(const iovec* ptr, uint32_t len) -> Result<size_t> {
  CHECK(proactor());
  CHECK_GT(len, 0U);
  CHECK_GE(fd_, 0);

  if (fd_ & IS_SHUTDOWN) {
    return Unexpected(errc::connection_aborted);
  }

  int fd = native_handle();
  Proactor* p = GetProactor();
  ssize_t res = 0;

  if (len == 1) {
    while (true) {
      FiberCall fc(p, timeout());
      fc->PrepSend(fd, ptr->iov_base, ptr->iov_len, 0);
      fc->sqe()->flags |= register_flag();

      res = fc.Get();  // Interrupt point
      if (res >= 0) {
        return res;  // Fastpath
      }

      DVSOCK(2) << "Got " << res;
      res = -res;
      if (res == EAGAIN)  // EAGAIN can happen in case of CQ overflow.
        continue;

      if (res == EPIPE)  // We do not care about EPIPE that can happen when we shutdown our socket.
        res = ECONNABORTED;

      break;
    }
  } else {  // len > 1
    msghdr msg;
    memset(&msg, 0, sizeof(msg));
    msg.msg_iov = const_cast<iovec*>(ptr);
    msg.msg_iovlen = len;

    while (true) {
      FiberCall fc(p, timeout());
      fc->PrepSendMsg(fd, &msg, MSG_NOSIGNAL);
      fc->sqe()->flags |= register_flag();

      res = fc.Get();  // Interrupt point
      if (res >= 0) {
        return res;  // Fastpath
      }

      DVSOCK(2) << "Got " << res;
      res = -res;
      if (res == EAGAIN)  // EAGAIN can happen in case of CQ overflow.
        continue;

      if (res == EPIPE)  // We do not care about EPIPE that can happen when we shutdown our socket.
        res = ECONNABORTED;

      break;
    };
  }

  error_code ec(res, system_category());
  VSOCK(1) << "Error " << ec << " on " << RemoteEndpoint();

  return make_unexpected(std::move(ec));
}

void UringSocket::AsyncWriteSome(const iovec* v, uint32_t len, AsyncWriteCb cb) {
  if (fd_ & IS_SHUTDOWN) {
    cb(Unexpected(errc::connection_aborted));
    return;
  }

  // this time we can not store it on stack.
  //
  msghdr* msg = new msghdr;
  memset(msg, 0, sizeof(msghdr));
  msg->msg_iov = const_cast<iovec*>(v);
  msg->msg_iovlen = len;

  int fd = native_handle();
  Proactor* proactor = GetProactor();

  auto mycb = [msg, cb = std::move(cb)](Proactor::IoResult res, uint32_t flags, int64_t) {
    delete msg;

    if (res >= 0) {
      cb(res);
      return;
    }

    if (res == EPIPE)  // We do not care about EPIPE that can happen when we shutdown our socket.
      res = ECONNABORTED;

    cb(make_unexpected(error_code{-res, generic_category()}));
  };

  SubmitEntry se = proactor->GetSubmitEntry(std::move(mycb), 0);
  se.PrepSendMsg(fd, msg, MSG_NOSIGNAL);
  se.sqe()->flags |= register_flag();
}

auto UringSocket::RecvMsg(const msghdr& msg, int flags) -> Result<size_t> {
  CHECK(proactor());
  CHECK_GE(fd_, 0);

  if (fd_ & IS_SHUTDOWN) {
    return Unexpected(errc::connection_aborted);
  }
  int fd = native_handle();
  Proactor* p = GetProactor();
  DCHECK(ProactorBase::me() == p);

  ssize_t res;
  while (true) {
    FiberCall fc(p, timeout());
    fc->PrepRecvMsg(fd, &msg, flags);
    fc->sqe()->flags |= register_flag();
    res = fc.Get();

    if (res > 0) {
      return res;
    }
    DVSOCK(2) << "Got " << res;

    res = -res;
    // EAGAIN can happen in case of CQ overflow.
    if (res == EAGAIN && (flags & MSG_DONTWAIT) == 0) {
      continue;
    }

    if (res == 0)
      res = ECONNABORTED;
    break;
  }

  error_code ec(res, system_category());
  VSOCK(1) << "Error " << ec << " on " << RemoteEndpoint();

  return make_unexpected(std::move(ec));
}

uint32_t UringSocket::PollEvent(uint32_t event_mask, std::function<void(uint32_t)> cb) {
  int fd = native_handle();
  Proactor* p = GetProactor();

  auto se_cb = [cb = std::move(cb)](Proactor::IoResult res, uint32_t flags, int64_t) { cb(res); };

  SubmitEntry se = p->GetSubmitEntry(std::move(se_cb), 0);
  se.PrepPollAdd(fd, event_mask);
  se.sqe()->flags |= register_flag();

  return se.sqe()->user_data;
}

uint32_t UringSocket::CancelPoll(uint32_t id) {
  FiberCall fc(GetProactor());
  fc->PrepPollRemove(id);

  IoResult io_res = fc.Get();
  if (io_res < 0)
    io_res = -io_res;

  return io_res;
}

}  // namespace uring
}  // namespace util
