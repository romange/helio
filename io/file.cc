// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "io/file.h"

#include <fcntl.h>
#include <stdio.h>
#include <sys/stat.h>
#include <unistd.h>

#include <memory>

#include "base/logging.h"

using namespace std;
namespace io {
using nonstd::expected;
using nonstd::make_unexpected;

namespace {

static ssize_t read_all(int fd, uint8* buffer, size_t length, size_t offset) {
  size_t left_to_read = length;
  uint8* curr_buf = buffer;
  while (left_to_read > 0) {
    ssize_t read = pread(fd, curr_buf, left_to_read, offset);
    if (read <= 0) {
      return read == 0 ? length - left_to_read : read;
    }

    curr_buf += read;
    offset += read;
    left_to_read -= read;
  }
  return length;
}

class LocalWriteFile : public WriteFile {
 public:
  // flags defined at http://man7.org/linux/man-pages/man2/open.2.html
  LocalWriteFile(std::string_view file_name, int flags) : WriteFile(file_name), flags_(flags) {
  }

  virtual ~LocalWriteFile() override;

  error_code Close() override;

  Result<size_t> WriteSome(const iovec* v, uint32_t len) final;

  error_code Open();

 protected:
  int fd_ = -1;
  int flags_;
};

LocalWriteFile::~LocalWriteFile() {
}

error_code LocalWriteFile::Open() {
  CHECK_EQ(fd_, -1);

  fd_ = open(create_file_name_.c_str(), flags_, 0644);
  if (fd_ < 0) {
    return StatusFileError();
  }
  return error_code{};
}

error_code LocalWriteFile::Close() {
  int res = 0;
  if (fd_ > 0) {
    res = close(fd_);
    fd_ = -1;
  }
  return res < 0 ? StatusFileError() : error_code{};
}

Result<size_t> LocalWriteFile::WriteSome(const iovec* v, uint32_t len) {
  if (len == 0)
    return 0;

  ssize_t written = writev(fd_, v, len);
  if (written >= 0) {
    return written;
  }

  return nonstd::make_unexpected(StatusFileError());
}

}  // namespace

bool Exists(std::string_view fname) {
  return access(fname.data(), F_OK) == 0;
}

bool Delete(std::string_view name) {
  int err;
  if ((err = unlink(name.data())) == 0) {
    return true;
  } else {
    return false;
  }
}

ReadonlyFile::~ReadonlyFile() {
}

// pread() based access.
class PosixReadFile final : public ReadonlyFile {
 private:
  int fd_;
  const size_t file_size_;
  bool drop_cache_;

 public:
  PosixReadFile(int fd, size_t sz, int advice, bool drop)
      : fd_(fd), file_size_(sz), drop_cache_(drop) {
    posix_fadvise(fd_, 0, 0, advice);
  }

  virtual ~PosixReadFile() {
    Close();
  }

  error_code Close() override {
    if (fd_) {
      if (drop_cache_)
        posix_fadvise(fd_, 0, 0, POSIX_FADV_DONTNEED);
      close(fd_);
      fd_ = 0;
    }
    return error_code{};
  }

  SizeOrError Read(size_t offset, const MutableBytes& range) override {
    if (range.empty())
      return 0;

    if (offset > file_size_) {
      return make_unexpected(make_error_code(errc::argument_out_of_domain));
    }
    ssize_t r = read_all(fd_, range.begin(), range.size(), offset);
    if (r < 0) {
      return make_unexpected(StatusFileError());
    }

    return r;
  }

  size_t Size() const final {
    return file_size_;
  }

  int Handle() const final {
    return fd_;
  };
};

expected<ReadonlyFile*, ::error_code> OpenRead(std::string_view name,
                                               const ReadonlyFile::Options& opts) {
  int fd = open(name.data(), O_RDONLY);
  if (fd < 0) {
    return make_unexpected(StatusFileError());
  }
  struct stat sb;
  if (fstat(fd, &sb) < 0) {
    close(fd);
    return make_unexpected(StatusFileError());
  }

  int advice = opts.sequential ? POSIX_FADV_SEQUENTIAL : POSIX_FADV_NORMAL;
  return new PosixReadFile(fd, sb.st_size, advice, opts.drop_cache_on_close);
}

expected<WriteFile*, error_code> OpenWrite(std::string_view file_name, WriteFile::Options opts) {
  int flags = O_CREAT | O_WRONLY | O_CLOEXEC;
  if (opts.append)
    flags |= O_APPEND;
  else
    flags |= O_TRUNC;
  LocalWriteFile* ptr = new LocalWriteFile(file_name, flags);
  error_code ec = ptr->Open();
  if (ec) {
    delete ptr;
    return make_unexpected(ec);
  }
  return ptr;
}

WriteFile::WriteFile(std::string_view name) : create_file_name_(name) {
}

WriteFile::~WriteFile() {
}



Result<size_t> StringFile::WriteSome(const iovec* v, uint32_t len) {
  size_t res = 0;
  for (uint32_t i = 0; i < len; ++i) {
    val.append(reinterpret_cast<const char*>(v[i].iov_base), v[i].iov_len);
    res += v[i].iov_len;
  }
  return res;
}


FileSource::~FileSource() {
  if (own_ == TAKE_OWNERSHIP) {
    auto ec = file_->Close();
    LOG_IF(WARNING, ec) << "Error closing a file " << ec;
    delete file_;
  }
}

io::Result<size_t> FileSource::ReadSome(const io::MutableBytes& dest) {
  io::Result<size_t> res = file_->Read(offset_, dest);
  if (res) {
    offset_ += *res;
  }
  return res;
}

}  // namespace io

namespace std {}  // namespace std
