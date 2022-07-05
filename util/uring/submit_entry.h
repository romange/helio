// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <liburing/io_uring.h>

namespace util {
namespace uring {

class Proactor;

/**
 * @brief Wraps and prepares SQE for submission.
 *
 * The reason that we need our own prepare interface instead of using default liburing helpers
 * is that we must preserve user_data set by Proactor. Unfortunately, liburing helpers
 * reset it assuming it will be filled after the preparation.
 *
 */
class SubmitEntry {
  io_uring_sqe* sqe_;

 public:
  SubmitEntry() : sqe_(nullptr) {
  }

  void PrepOpenAt(int dfd, const char* path, int flags, mode_t mode) {
    PrepFd(IORING_OP_OPENAT, dfd);
    sqe_->addr = (unsigned long)path;
    sqe_->open_flags = flags;
    sqe_->len = mode;
  }

  // mask is a bit-OR of POLLXXX flags.
  void PrepPollAdd(int fd, unsigned mask) {
    PrepFd(IORING_OP_POLL_ADD, fd);

#if __BYTE_ORDER == __BIG_ENDIAN
	  mask = __swahw32(mask);
#endif
    sqe_->poll32_events = mask;
  }

  void PrepPollRemove(uint64_t uid) {
    PrepFd(IORING_OP_POLL_REMOVE, -1);
    sqe_->addr = uid;
  }

  void PrepRecvMsg(int fd, const struct msghdr* msg, unsigned flags) {
    PrepFd(IORING_OP_RECVMSG, fd);
    sqe_->addr = (unsigned long)msg;
    sqe_->len = 1;
    sqe_->msg_flags = flags;
  }

  void PrepRead(int fd, void* buf, unsigned size, size_t offset) {
    PrepFd(IORING_OP_READ, fd);
    sqe_->addr = (unsigned long)buf;
    sqe_->len = size;
    sqe_->off = offset;
  }

  void PrepReadV(int fd, const struct iovec* vec, unsigned nr_vecs, size_t offset,
                 unsigned flags = 0) {
    PrepFd(IORING_OP_READV, fd);
    sqe_->addr = (unsigned long)vec;
    sqe_->len = nr_vecs;
    sqe_->off = offset;
    sqe_->rw_flags = flags;
  }

  void PrepWrite(int fd, const void* buf, unsigned size, size_t offset) {
    PrepFd(IORING_OP_WRITE, fd);
    sqe_->addr = (unsigned long)buf;
    sqe_->len = size;
    sqe_->off = offset;
  }

  void PrepWriteV(int fd, const struct iovec* vec, unsigned nr_vecs, size_t offset,
                  unsigned flags = 0) {
    PrepFd(IORING_OP_WRITEV, fd);
    sqe_->addr = (unsigned long)vec;
    sqe_->len = nr_vecs;
    sqe_->off = offset;
    sqe_->rw_flags = flags;
  }

  void PrepFallocate(int fd, int mode, off_t offset, off_t len) {
    PrepFd(IORING_OP_FALLOCATE, fd);
    sqe_->off = offset;
    sqe_->len = mode;
    sqe_->addr = uint64_t(len);
  }

  void PrepFadvise(int fd, off_t offset, off_t len, int advice) {
    PrepFd(IORING_OP_FADVISE, fd);
    sqe_->fadvise_advice = advice;
    sqe_->len = len;
    sqe_->off = offset;
  }

  void PrepSend(int fd, const void* buf, size_t len, int flags) {
    PrepFd(IORING_OP_SEND, fd);
    sqe_->addr = (unsigned long)buf;
    sqe_->len = len;
    sqe_->msg_flags = flags;
  }

  void PrepSendMsg(int fd, const struct msghdr* msg, unsigned flags) {
    PrepFd(IORING_OP_SENDMSG, fd);
    sqe_->addr = (unsigned long)msg;
    sqe_->len = 1;
    sqe_->msg_flags = flags;
  }

  void PrepConnect(int fd, const struct sockaddr* addr, socklen_t addrlen) {
    PrepFd(IORING_OP_CONNECT, fd);
    sqe_->addr = (unsigned long)addr;
    sqe_->len = 0;
    sqe_->off = addrlen;
  }

  void PrepClose(int fd) {
    PrepFd(IORING_OP_CLOSE, fd);
  }

  void PrepTimeout(const timespec* ts, bool is_abs = true) {
    PrepFd(IORING_OP_TIMEOUT, -1);
    sqe_->addr = (unsigned long)ts;
    sqe_->len = 1;
    sqe_->timeout_flags = (is_abs ? IORING_TIMEOUT_ABS : 0);
  }

  void PrepTimeoutRemove(unsigned long long userdata) {
    PrepFd(IORING_OP_TIMEOUT_REMOVE, -1);
    sqe_->addr = userdata;
    sqe_->len = 1;
  }

  // Sets up link timeout with relative timespec.
  void PrepLinkTimeout(const timespec* ts) {
    PrepFd(IORING_OP_LINK_TIMEOUT, -1);
    sqe_->addr = (unsigned long)ts;
    sqe_->len = 1;
    sqe_->timeout_flags = 0;
  }

  // how is either: SHUT_RD, SHUT_WR or SHUT_RDWR.
  void PrepShutdown(int fd, int how) {
    PrepFd(IORING_OP_SHUTDOWN, fd);
    sqe_->len = how;
  }

  // TODO: To remove this accessor.
  io_uring_sqe* sqe() {
    return sqe_;
  }

  void PrepNOP() {
    PrepFd(IORING_OP_NOP, -1);
  }

 private:
  explicit SubmitEntry(io_uring_sqe* sqe) : sqe_(sqe) {
  }

  void PrepFd(int op, int fd) {
    sqe_->opcode = op;
    sqe_->fd = fd;
  }

  friend class Proactor;
};

}  // namespace uring
}  // namespace util
