// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <string_view>
#include <absl/types/span.h>

#include <system_error>

#include "base/expected.hpp"
#include "base/integral_types.h"
#include "io/io.h"

namespace io {

using SizeOrError = Result<size_t>;

inline ::std::error_code StatusFileError() {
  return ::std::error_code(errno, ::std::system_category());
}

// ReadonlyFile objects are created via ReadonlyFile::Open() factory function
// and are destroyed via "obj->Close(); delete obj;" sequence.
//
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

  /// Reads range.size() bytes into range. In case, EOF reached returns 0.
  /// range must be non-empty.
  ABSL_MUST_USE_RESULT virtual Result<size_t> Read(size_t offset, const MutableBytes& range) = 0;

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
  FileSource(ReadonlyFile* file, Ownership own = TAKE_OWNERSHIP) : file_(file), own_(own) {
  }
  ~FileSource();

  Result<size_t> ReadSome(const MutableBytes& dest) final;

 private:
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

}  // namespace io
