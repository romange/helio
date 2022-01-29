// Copyright 2022, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <absl/types/span.h>

#include <string_view>
#include <system_error>

#include "base/expected.hpp"
#include "base/integral_types.h"
#include "io/io.h"

namespace io {

using SizeOrError = Result<size_t>;

inline ::std::error_code StatusFileError() {
  return ::std::error_code(errno, ::std::system_category());
}

// ReadonlyFile objects are created via factory functions. For example, OpenRead()
// returns posix-based implementation of ReadonlyFile;
// ReadonlyFiles should be destroyed via "obj->Close(); delete obj;" sequence.
//
// ReadonlyFile can not be io::Source because they expose random access interface (offset argument).
class ReadonlyFile {
 protected:
  ReadonlyFile() {
  }

 public:
  struct Options {
    bool sequential = true;           // hint
    bool drop_cache_on_close = true;  // hint
    Options() {
    }
  };

  using MutableBytes = absl::Span<uint8_t>;

  virtual ~ReadonlyFile();

  /// Reads range.size() bytes into dest. if EOF has been reached, returns 0.
  /// range must be non-empty.
  ABSL_MUST_USE_RESULT Result<size_t> Read(size_t offset, const MutableBytes& dest) {
    iovec v{.iov_base = dest.data(), .iov_len = dest.size()};
    return Read(offset, &v, 1);
  }

  // Returns how many bytes were read. It's not an error to return less bytes that was
  // originally requested by (v, len). Depends on the implementation.
  // If 0 is returned then EOF is definitely reached.
  ABSL_MUST_USE_RESULT virtual Result<size_t> Read(size_t offset, const iovec* v, uint32_t len) = 0;

  // releases the system handle for this file. Does not delete `this` instance.
  ABSL_MUST_USE_RESULT virtual ::std::error_code Close() = 0;

  virtual size_t Size() const = 0;

  virtual int Handle() const = 0;
};

// Append only file.
class WriteFile : public Sink {
 public:
  struct Options {
    bool append = false;  // if true - does not overwrite the existing file on open.
  };

  virtual ~WriteFile();

  /*! @brief Flushes remaining data, closes access to a file handle. For asynchronous interfaces
      serves as a barrier and makes sure previous writes are flushed.
   *
   */
  virtual std::error_code Close() = 0;

  using Sink::Write;

  ABSL_MUST_USE_RESULT std::error_code Write(const uint8_t* buffer, size_t length) {
    return Write(Bytes{buffer, length});
  }

  ABSL_MUST_USE_RESULT std::error_code Write(const std::string_view slice) {
    return Write(reinterpret_cast<const uint8_t*>(slice.data()), slice.size());
  }

  //! Returns the file name given during OpenWrite(...) call.
  const std::string& create_file_name() const {
    return create_file_name_;
  }

 protected:
  explicit WriteFile(const std::string_view create_file_name);

  // Name of the created file.
  const std::string create_file_name_;
};

class StringFile : public WriteFile {
 public:
  std::string val;

  StringFile() : WriteFile(std::string{}) {
  }

  std::error_code Close() final {
    return std::error_code{};
  }

  Result<size_t> WriteSome(const iovec* v, uint32_t len) final;
};

/// Sequential-only Source backed by a file.
class FileSource : public Source {
 public:
  FileSource(const FileSource&) = delete;
  FileSource& operator=(const FileSource&) = delete;

  FileSource(ReadonlyFile* file, Ownership own = TAKE_OWNERSHIP) : file_(file), own_(own) {
  }

  FileSource(FileSource&& o) noexcept : offset_(o.offset_), file_(o.file_), own_(o.own_) {
    o.own_ = DO_NOT_TAKE_OWNERSHIP;
    o.file_ = nullptr;
  }

  ~FileSource() {
    Close();
  }

  FileSource& operator=(FileSource&&) noexcept;

  Result<size_t> ReadSome(const iovec* v, uint32_t len) final;

 private:
  void Close();

  size_t offset_ = 0;
  ReadonlyFile* file_;
  Ownership own_;
};

//! Deletes the file returning true iff successful.
bool Delete(std::string_view name);

bool Exists(std::string_view name);

using ReadonlyFileOrError = Result<ReadonlyFile*>;
ABSL_MUST_USE_RESULT ReadonlyFileOrError OpenRead(std::string_view name,
                                                  const ReadonlyFile::Options& opts);

using WriteFileOrError = Result<WriteFile*>;

//! Factory method to create a new writable file object. Calls Open on the
//! resulting object to open the file.
ABSL_MUST_USE_RESULT Result<WriteFile*> OpenWrite(std::string_view path,
                                                  WriteFile::Options opts = WriteFile::Options());

// Reads data from fd to (v, len) using preadv interface.
// Returns -1 if error occurred, otherwise returns number of bytes read.
// return a smaller count than that that is spawned vy (v, len) only if
// EOF reached. Otherwise will make sure to fully fill v.
ssize_t ReadAllPosix(int fd, size_t offset, const iovec* v, uint32_t len);

}  // namespace io
