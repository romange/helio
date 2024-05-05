// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/fibers/uring_file.h"

#include <fcntl.h>
#include <sys/stat.h>

#include "base/logging.h"
#include "util/fibers/uring_proactor.h"

namespace util {

namespace fb2 {
using Proactor = UringProactor;

using namespace std;
using namespace io;
using nonstd::make_unexpected;

namespace {

class ReadFileImpl : public ReadonlyFile {
 public:
  ReadFileImpl(int fd, size_t sz, Proactor* proactor)
      : fd_(fd), file_size_(sz), proactor_(proactor) {
  }

  virtual ~ReadFileImpl();

  error_code Close() final;

  SizeOrError Read(size_t offset, const iovec* v, uint32_t len) final;

  size_t Size() const final {
    return file_size_;
  }

  int Handle() const final {
    return fd_;
  };

 private:
  int fd_;
  const size_t file_size_;
  Proactor* proactor_;
};

class WriteFileImpl final : public WriteFile {
 public:
  WriteFileImpl(Proactor* p, std::string_view file_name) : WriteFile(file_name), proactor_(p) {
  }

  virtual ~WriteFileImpl();

  error_code Close();

  Result<size_t> WriteSome(const iovec* v, uint32_t len);

  error_code Open(int flags);

 protected:
  int fd_ = -1;
  Proactor* proactor_;
};

/* generated by: http://ascii.gaetanroger.fr/ with Big font
//  _____                 _                           _        _   _
// |_   _|               | |                         | |      | | (_)
//   | |  _ __ ___  _ __ | | ___ _ __ ___   ___ _ __ | |_ __ _| |_ _  ___  _ __
//   | | | '_ ` _ \| '_ \| |/ _ \ '_ ` _ \ / _ \ '_ \| __/ _` | __| |/ _ \| '_ \
//  _| |_| | | | | | |_) | |  __/ | | | | |  __/ | | | || (_| | |_| | (_) | | | |
// |_____|_| |_| |_| .__/|_|\___|_| |_| |_|\___|_| |_|\__\__,_|\__|_|\___/|_| |_|
//                 | |
//                 |_|
*/

error_code CloseFile(int fd, Proactor* p) {
  if (fd > 0) {
    FiberCall fc(p);
    fc->PrepClose(fd);
    FiberCall::IoResult io_res = fc.Get();
    if (io_res < 0) {
      return error_code{-io_res, system_category()};
    }
  }
  return error_code{};
}

io::Result<size_t> WriteSomeInternal(int fd, const struct iovec* iov, unsigned iovcnt, off_t offset,
                                     unsigned flags, bool direct, Proactor* p) {
  CHECK_GE(fd, 0);
  CHECK_GT(iovcnt, 0u);

  FiberCall fc(p);
  fc->PrepWriteV(fd, iov, iovcnt, offset, flags);
  if (direct)
    fc->sqe()->flags |= IOSQE_FIXED_FILE;
  FiberCall::IoResult io_res = fc.Get();
  if (io_res < 0) {
    return make_unexpected(error_code{-io_res, system_category()});
  }
  return io_res;
}

io::Result<size_t> ReadSomeInternal(int fd, const struct iovec* iov, unsigned iovcnt, off_t offset,
                                    unsigned flags, bool direct, Proactor* p) {
  CHECK_GE(fd, 0);
  CHECK_GT(iovcnt, 0u);

  FiberCall fc(p);
  fc->PrepReadV(fd, iov, iovcnt, offset, flags);
  if (direct)
    fc->sqe()->flags |= IOSQE_FIXED_FILE;

  FiberCall::IoResult io_res = fc.Get();
  if (io_res < 0) {
    return make_unexpected(error_code{-io_res, system_category()});
  }
  return io_res;
}

io::Result<size_t> ReadAll(int fd, size_t offset, uint8_t* next, size_t len, Proactor* p) {
  DCHECK_GT(len, 0u);

  ssize_t read_total = 0;
  while (true) {
    FiberCall fc(p);
    fc->PrepRead(fd, next, len, offset);
    FiberCall::IoResult io_res = fc.Get();
    if (io_res < 0) {
      return make_unexpected(error_code{-io_res, system_category()});
    }

    if (io_res == 0) {
      return read_total;
    }

    read_total += io_res;

    if (size_t(read_total) == len)
      break;

    offset += io_res;
    next += io_res;
  }

  return read_total;
}

ReadFileImpl::~ReadFileImpl() {
  CloseFile(fd_, proactor_);
}

error_code ReadFileImpl::Close() {
  error_code ec = CloseFile(fd_, proactor_);
  fd_ = -1;
  return ec;
}

io::SizeOrError ReadFileImpl::Read(size_t offset, const iovec* v, uint32_t len) {
  DCHECK_GE(fd_, 0);

  if (len == 0)
    return 0;

  ssize_t read_total = 0;

  do {
    io::SizeOrError res = ReadSomeInternal(fd_, v, len, offset + read_total, 0, false, proactor_);

    if (!res)
      return res;

    size_t read = *res;
    if (read == 0)
      return read_total;

    read_total += read;

    while (len && v->iov_len <= read) {  // pass through all completed entries.
      --len;
      read -= v->iov_len;
      ++v;
    }

    if (read > 0) {  // we read through part of the entry.
      DCHECK_GT(len, 0u);

      // Finish the rest of the entry.
      uint8_t* next = reinterpret_cast<uint8_t*>(v->iov_base) + read;
      size_t count = v->iov_len - read;
      res = ReadAll(fd_, offset + read_total, next, count, proactor_);

      if (!res)
        return res;

      read_total += *res;
      if (*res < count) {  // eof
        return read_total;
      }

      ++v;
      --len;
    }
  } while (len > 0);

  return read_total;
}

WriteFileImpl::~WriteFileImpl() {
  CloseFile(fd_, proactor_);
}

error_code WriteFileImpl::Open(int flags) {
  CHECK_EQ(fd_, -1);

  FiberCall fc(proactor_);
  fc->PrepOpenAt(AT_FDCWD, create_file_name_.c_str(), flags, 0644);
  FiberCall::IoResult io_res = fc.Get();

  if (io_res < 0) {
    return error_code{-io_res, system_category()};
  }
  fd_ = io_res;

  return error_code{};
}

error_code WriteFileImpl::Close() {
  error_code ec = CloseFile(fd_, proactor_);
  fd_ = -1;
  return ec;
}

Result<size_t> WriteFileImpl::WriteSome(const iovec* v, uint32_t len) {
  Result<size_t> res = WriteSomeInternal(fd_, v, len, -1, 0, false, proactor_);
  return res;
}

}  // namespace

io::Result<io::WriteFile*> OpenWrite(std::string_view path, io::WriteFile::Options opts) {
  int flags = O_CREAT | O_WRONLY | O_CLOEXEC;
  if (opts.append)
    flags |= O_APPEND;
  else
    flags |= O_TRUNC;

  ProactorBase* me = ProactorBase::me();
  DCHECK(me->GetKind() == ProactorBase::IOURING);

  Proactor* p = static_cast<Proactor*>(CHECK_NOTNULL(me));

  unique_ptr<WriteFileImpl> impl(new WriteFileImpl{p, path});
  error_code ec = impl->Open(flags);
  if (ec)
    return make_unexpected(ec);

  return impl.release();
}

io::Result<io::ReadonlyFile*> OpenRead(std::string_view path) {
  int flags = O_RDONLY | O_CLOEXEC;

  ProactorBase* me = ProactorBase::me();
  DCHECK(me->GetKind() == ProactorBase::IOURING);

  Proactor* p = static_cast<Proactor*>(CHECK_NOTNULL(me));
  FiberCall::IoResult io_res;

  {
    FiberCall fc(p);
    fc->PrepOpenAt(AT_FDCWD, path.data(), flags, 0);
    io_res = fc.Get();

    if (io_res < 0) {
      return make_unexpected(error_code{-io_res, system_category()});
    }
  }

  int fd = io_res;
  struct stat sb;
  if (fstat(fd, &sb) < 0) {
    int e = errno;
    close(fd);
    return make_unexpected(error_code{e, system_category()});
  }

  {
    FiberCall fc(p);
    fc->PrepFadvise(fd, 0, 0, POSIX_FADV_SEQUENTIAL);
    io_res = fc.Get();
    if (io_res < 0) {
      int e = errno;
      close(fd);
      return make_unexpected(error_code{e, system_category()});
    }
  }

  return new ReadFileImpl(fd, sb.st_size, p);
}

LinuxFile::LinuxFile(int fd, Proactor* proactor) : fd_(fd), flags_(0), proactor_(proactor) {
  DCHECK_GE(fd, 0);
  DCHECK(proactor);

  if (proactor->HasDirectFD()) {
    unsigned direct_fd = proactor->RegisterFd(fd);
    if (direct_fd != UringProactor::kInvalidDirectFd) {
      fd_ = direct_fd;
      is_direct_ = 1;
    }
  }
}

LinuxFile::~LinuxFile() {
  error_code ec = Close();
  if (ec) {
    LOG(WARNING) << "Error closing file " << ec;
  }
}

// Corresponds to pwritev2 interface. Has suffix Some because it does not guarantee the full
// write in case of a successful operation.
io::Result<size_t> LinuxFile::WriteSome(const struct iovec* iov, unsigned iovcnt, off_t offset,
                                        unsigned flags) {
  return WriteSomeInternal(fd_, iov, iovcnt, offset, flags, is_direct_, proactor_);
}

// Corresponds to preadv2 interface.
io::Result<size_t> LinuxFile::ReadSome(const struct iovec* iov, unsigned iovcnt, off_t offset,
                                       unsigned flags) {
  return ReadSomeInternal(fd_, iov, iovcnt, offset, flags, is_direct_, proactor_);
}

std::error_code LinuxFile::Close() {
  if (fd_ < 0)
    return {};

  int fd = fd_;
  if (is_direct_) {
    fd = proactor_->UnregisterFd(fd_);
    if (fd < 0) {
      LOG(WARNING) << "Error unregistering fd " << fd_;
      return {};
    }
    is_direct_ = 0;
  }
  fd_ = -1;
  return CloseFile(fd, proactor_);
}

int LinuxFile::GetFd() const {
  return is_direct_ ? proactor_->TranslateDirectFd(fd_) : fd_;
}

std::error_code LinuxFile::ReadFixed(io::MutableBytes dest, off_t offset, unsigned buf_index) {
  FiberCall fc(proactor_);
  fc->PrepReadFixed(fd_, dest.data(), dest.size(), offset, buf_index);
  FiberCall::IoResult io_res = fc.Get();

  if (io_res < 0) {
    return error_code{-io_res, system_category()};
  }

  DCHECK_EQ(size_t(io_res), dest.size());
  return error_code{};
}

error_code LinuxFile::Write(const iovec* iov, unsigned iovcnt, off_t offset, unsigned flags) {
  auto cb = [this, flags, offset](const iovec* iov, unsigned iovcnt) mutable {
    auto res = this->WriteSome(iov, iovcnt, offset, flags);
    if (res) {
      offset += *res;
    }
    return res;
  };

  return io::ApplyExactly(iov, iovcnt, std::move(cb));
}

error_code LinuxFile::Read(const iovec* iov, unsigned iovcnt, off_t offset, unsigned flags) {
  auto cb = [this, flags, offset](const iovec* iov, unsigned iovcnt) mutable {
    auto res = this->ReadSome(iov, iovcnt, offset, flags);
    if (res) {
      offset += *res;
    }
    return res;
  };

  return io::ApplyExactly(iov, iovcnt, std::move(cb));
}

void LinuxFile::ReadFixedAsync(io::MutableBytes dest, off_t offset, unsigned buf_index,
                               AsyncCb cb) {
  auto adapt_cb = [cb = std::move(cb)](detail::FiberInterface* current, UringProactor::IoResult res,
                                       uint32_t flags) { cb(res); };

  SubmitEntry se = proactor_->GetSubmitEntry(std::move(adapt_cb));
  se.PrepReadFixed(fd_, dest.data(), dest.size(), offset, buf_index);
  if (is_direct_)
    se.sqe()->flags |= IOSQE_FIXED_FILE;
}

void LinuxFile::ReadAsync(io::MutableBytes dest, off_t offset, AsyncCb cb) {
  auto adapt_cb = [cb = std::move(cb)](detail::FiberInterface* current, UringProactor::IoResult res,
                                       uint32_t flags) { cb(res); };

  SubmitEntry se = proactor_->GetSubmitEntry(std::move(adapt_cb));
  se.PrepRead(fd_, dest.data(), dest.size(), offset);
  if (is_direct_)
    se.sqe()->flags |= IOSQE_FIXED_FILE;
}

void LinuxFile::WriteFixedAsync(io::Bytes src, off_t offset, unsigned buf_index, AsyncCb cb) {
  auto adapt_cb = [cb = std::move(cb)](detail::FiberInterface* current, UringProactor::IoResult res,
                                       uint32_t flags) { cb(res); };
  SubmitEntry se = proactor_->GetSubmitEntry(std::move(adapt_cb));
  se.PrepWriteFixed(fd_, src.data(), src.size(), offset, buf_index);
  if (is_direct_)
    se.sqe()->flags |= IOSQE_FIXED_FILE;
}

void LinuxFile::WriteAsync(io::Bytes src, off_t offset, AsyncCb cb) {
  auto adapt_cb = [cb = std::move(cb)](detail::FiberInterface*, UringProactor::IoResult res,
                                       uint32_t flags) { cb(res); };
  SubmitEntry se = proactor_->GetSubmitEntry(std::move(adapt_cb));

  se.PrepWrite(fd_, src.data(), src.size(), offset);
  if (is_direct_)
    se.sqe()->flags |= IOSQE_FIXED_FILE;
}

io::Result<std::unique_ptr<LinuxFile>> OpenLinux(std::string_view path, int flags, mode_t mode) {
  ProactorBase* me = ProactorBase::me();
  DCHECK(me->GetKind() == ProactorBase::IOURING);

  Proactor* p = static_cast<Proactor*>(CHECK_NOTNULL(me));
  FiberCall::IoResult io_res;

  {
    FiberCall fc(p);
    fc->PrepOpenAt(AT_FDCWD, path.data(), flags, mode);
    io_res = fc.Get();

    if (io_res < 0) {
      return make_unexpected(error_code{-io_res, system_category()});
    }
  }
  return make_unique<LinuxFile>(io_res, p);
}

}  // namespace fb2
}  // namespace util
