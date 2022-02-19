// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <sys/types.h>  // for mode_t

#include "io/file.h"

namespace util {
namespace uring {

// The following functions must be called in the context of Proactor thread.
// The objects should be accessed and used in the context of the same thread where
// they have been opened.
io::Result<io::WriteFile*> OpenWrite(std::string_view path,
                                     io::WriteFile::Options opts = io::WriteFile::Options());

io::Result<io::ReadonlyFile*> OpenRead(std::string_view path);

// Uring based linux file. Similarly, works only within the same proactor thread where it has
// been open. Unlike classic IO classes can read and write at specified offsets.
class LinuxFile {
 public:
  virtual ~LinuxFile() {
  }

  // Corresponds to pwritev2 interface. However, it does not guarantee full write
  // in case of a successful operation. Hence "Some".
  virtual io::Result<size_t> WriteSome(const struct iovec* iov, unsigned iovcnt, off_t offset,
                                       unsigned flags) = 0;

  // Corresponds to preadv2 interface.
  virtual io::Result<size_t> ReadSome(const struct iovec* iov, unsigned iovcnt, off_t offset,
                                      unsigned flags) = 0;

  virtual std::error_code Close() = 0;

  int fd() const {
    return fd_;
  }

  // Similar to XXXSome methods but writes fully all the io vectors or fails.
  std::error_code Write(const struct iovec* iov, unsigned iovcnt, off_t offset, unsigned flags);
  std::error_code Read(const struct iovec* iov, unsigned iovcnt, off_t offset, unsigned flags);

  std::error_code Write(io::Bytes src, off_t offset, unsigned flags) {
    iovec vec{.iov_base = const_cast<uint8_t*>(src.data()), .iov_len = src.size()};
    return Write(&vec, 1, offset, flags);
  }

 protected:
  int fd_ = -1;
};

// Equivalent to open(2) call.
io::Result<std::unique_ptr<LinuxFile>> OpenLinux(std::string_view path, int flags, mode_t mode);

}  // namespace uring
}  // namespace util
