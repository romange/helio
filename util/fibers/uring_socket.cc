// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/fibers/uring_socket.h"

#include <netinet/in.h>
#include <poll.h>

#include "base/logging.h"
#include "base/stl_util.h"

#define VSOCK(verbosity) VLOG(verbosity) << "sock[" << native_handle() << "] "
#define DVSOCK(verbosity) DVLOG(verbosity) << "sock[" << native_handle() << "] "

namespace util {
namespace fb2 {

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
  DCHECK_LT(fd_, 0) << "Socket must have been closed explicitly.";
  error_code ec = Close();  // Quietly close.

  LOG_IF(WARNING, ec) << "Error closing socket " << ec << "/" << ec.message();
}

auto UringSocket::Close() -> error_code {
  error_code ec;
  if (fd_ >= 0) {
    DCHECK_EQ(GetProactor()->thread_id(), pthread_self());

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
  VSOCK(2) << "Accept [" << real_fd << "]";

  while (true) {
    int res = accept4(real_fd, NULL, NULL, SOCK_NONBLOCK | SOCK_CLOEXEC);
    if (res >= 0) {
      UringSocket* fs = new UringSocket{nullptr};
      fs->fd_ = res << kFdShift;
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

  int fd = socket(ep.protocol().family(), SOCK_STREAM | SOCK_NONBLOCK | SOCK_CLOEXEC, IPPROTO_TCP);
  if (posix_err_wrap(fd, &ec) < 0)
    return ec;

  VSOCK(1) << "Connect [" << fd << "] " << ep.address().to_string() << ":" << ep.port();

  Proactor* p = GetProactor();
  CHECK(!p->HasSqPoll()) << "Not supported with SQPOLL, TBD";

  unsigned dense_id = fd;

  if (p->HasRegisterFd()) {
    dense_id = p->RegisterFd(fd);
    fd_ = (dense_id << kFdShift) | REGISTER_FD;
  } else {
    fd_ = (dense_id << kFdShift);
  }

  IoResult io_res;
  ep.data();

  FiberCall fc(p, timeout());
  fc->PrepConnect(dense_id, (const sockaddr*)ep.data(), ep.size());
  fc->sqe()->flags |= register_flag();
  io_res = fc.Get();

  if (io_res < 0) {  // In that case connect returns -errno.
    ec = error_code(-io_res, system_category());
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
  size_t short_len = 0;
  uint8_t* reg_buf = nullptr;

  VSOCK(2) << "WriteSome [" << fd << "] " << len;

  // WARNING: raw, experimental code.
  if (p->HasRegisteredBuffers() && len < 16) {
    for (uint32_t i = 0; i < len; ++i) {
      short_len += ptr[i].iov_len;
    }

    if (short_len <= 64) {
      reg_buf = p->ProvideRegisteredBuffer();
    }
  }

  if (reg_buf) {
    uint8_t* next = reg_buf;
    for (uint32_t i = 0; i < len; ++i) {
      memcpy(next, ptr[i].iov_base, ptr[i].iov_len);
      next += ptr[i].iov_len;
    }
    auto cb = [p, reg_buf](detail::FiberInterface*, Proactor::IoResult res, uint32_t flags) {
      p->ReturnRegisteredBuffer(reg_buf);
      CHECK_GT(res, 0);  // TODO - handle errors.
    };

    SubmitEntry se = p->GetSubmitEntry(std::move(cb));
    se.PrepWriteFixed(fd, reg_buf, short_len, 0, 0);
    se.sqe()->flags |= register_flag();

    return short_len;  // optimistic
  }

  if (len == 1) {
    while (true) {
      FiberCall fc(p, timeout());
      fc->PrepSend(fd, ptr->iov_base, ptr->iov_len, MSG_NOSIGNAL);
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
  auto mycb = [msg, cb = std::move(cb)](detail::FiberInterface*, Proactor::IoResult res,
                                        uint32_t flags) {
    delete msg;

    if (res >= 0) {
      cb(res);
      return;
    }

    if (res == EPIPE)  // We do not care about EPIPE that can happen when we shutdown our socket.
      res = ECONNABORTED;

    cb(make_unexpected(error_code{-res, generic_category()}));
  };

  SubmitEntry se = proactor->GetSubmitEntry(std::move(mycb));
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
  VSOCK(2) << "RecvMsg [" << fd << "]";

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

io::Result<size_t> UringSocket::Recv(const io::MutableBytes& mb, int flags) {
  int fd = native_handle();
  Proactor* p = GetProactor();
  DCHECK(ProactorBase::me() == p);

  VSOCK(2) << "Recv [" << fd << "] " << flags;
  ssize_t res;
  while (true) {
    FiberCall fc(p, timeout());
    fc->PrepRecv(fd, mb.data(), mb.size(), flags);
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

  auto se_cb = [cb = std::move(cb)](detail::FiberInterface*, Proactor::IoResult res,
                                    uint32_t flags) { cb(res); };
  SubmitEntry se = p->GetSubmitEntry(std::move(se_cb));
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

void UringSocket::RegisterOnErrorCb(std::function<void(uint32_t)> cb) {
  DCHECK(error_cb_id_ == UINT32_MAX);
  error_cb_id_ = this->PollEvent(POLLERR | POLLHUP, std::move(cb));
}

void UringSocket::CancelOnErrorCb() {
  if (error_cb_id_ != UINT32_MAX) {
    this->CancelPoll(error_cb_id_);
    error_cb_id_ = UINT32_MAX;
  }
}

}  // namespace fb2
}  // namespace util
