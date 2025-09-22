// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <sys/types.h>  // for mode_t
#include <sys/stat.h> // statx

#include "io/file.h"

namespace util {

namespace fb2 {

class UringProactor;

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
  LinuxFile(int fd, UringProactor* proactor);
  ~LinuxFile();

  // Corresponds to pwritev2 interface. However, it does not guarantee full write
  // in case of a successful operation. Hence "Some".
  io::Result<size_t> WriteSome(const struct iovec* iov, unsigned iovcnt, off_t offset,
                               unsigned flags);

  // Corresponds to preadv2 interface.
  io::Result<size_t> ReadSome(const struct iovec* iov, unsigned iovcnt, off_t offset,
                              unsigned flags);

  std::error_code Close();

  // Kept for backward compatibility. Use GetFd.
  int fd() const {
    return GetFd();
  }

  // Always returns linux file descriptor.
  int GetFd() const;

  // Similar to XXXSome methods but writes fully all the io vectors or fails.
  // Is not atomic meaning that it can issue multiple io calls until error occurs or we
  // fully write all the buffers.
  std::error_code Write(const struct iovec* iov, unsigned iovcnt, off_t offset, unsigned flags);

  // Similarly to Write, reads fully all the io vectors or fails.
  std::error_code Read(const struct iovec* iov, unsigned iovcnt, off_t offset, unsigned flags);

  std::error_code Write(io::Bytes src, off_t offset, unsigned flags) {
    iovec vec{.iov_base = const_cast<uint8_t*>(src.data()), .iov_len = src.size()};
    return Write(&vec, 1, offset, flags);
  }

  std::error_code ReadFixed(io::MutableBytes dest, off_t offset, unsigned buf_index);

  // int - io result. negative value is an error code.
  using AsyncCb = std::function<void(int)>;

  // Async versions of Read
  void ReadFixedAsync(io::MutableBytes dest, off_t offset, unsigned buf_index, AsyncCb cb);
  void ReadAsync(io::MutableBytes dest, off_t offset, AsyncCb cb);

  // io_uring fixed version - src must point within the region, contained by the fixed buffer that
  // is specified by buf_index. See io_uring_prep_write_fixed(3) for more details.
  void WriteFixedAsync(io::Bytes src, off_t offset, unsigned buf_index, AsyncCb cb);
  void WriteAsync(io::Bytes src, off_t offset, AsyncCb cb);
  void FallocateAsync(int mode, off_t offset, off_t len, AsyncCb cb);

  std::error_code FSync(unsigned flags = 0 /* full sync */);

 protected:
  int fd_ = -1;
  union {
    unsigned flags_ : 1;
    struct {
      unsigned is_direct_ : 1;
    };
  };

  UringProactor* proactor_;
};

// Equivalent to open(2) call. "flags" is the OR mask of O_XXX constants.
io::Result<std::unique_ptr<LinuxFile>> OpenLinux(std::string_view path, int flags, mode_t mode);


// Equivalent to statx() call
std::error_code StatX(const char* filepath, struct statx *stat, int fd);

}  // namespace fb2
}  // namespace util
