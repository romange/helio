// Copyright 2022, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <absl/types/span.h>
#include <sys/uio.h>

#include <string_view>

#include "base/expected.hpp"

namespace io {

using MutableBytes = absl::Span<uint8_t>;
using Bytes = absl::Span<const uint8_t>;

inline Bytes Buffer(std::string_view str) {
  return Bytes{reinterpret_cast<const uint8_t*>(str.data()), str.size()};
}

/// Similar to Rust std::io::Result.
template <typename T, typename E = ::std::error_code> using Result = nonstd::expected<T, E>;

/**
 * @brief The Source class allows for reading bytes from a source. Similar to Rust io::Read trait.
 *
 */
class Source {
 public:
  virtual ~Source() {
  }

  /**
   * @brief  Pull some bytes from this source into the specified buffer, returning how many bytes
   * were read. Requires: dest is not empty.
   *
   * If the return value of this method is size_t n, then implementations must guarantee that 0 <= n
   * <= dest.len(). A nonzero n value indicates that the buffer buf has been filled in with n bytes
   * of data from this source. If n is 0, then it means the following:
   *
   * This reader has reached its “end of file” and will likely no longer be able to produce
   * bytes. Note that this does not mean that the reader will always no longer be able to produce
   * bytes. As an example, on Linux, this method will call the recv syscall for a TcpStream, where
   * returning zero indicates the connection was shut down correctly. While for file, it is possible
   * to reach the end of file and get zero as result, but if more data is appended to the file,
   * future calls to read will return more data.
   *
   * @param dest - destination buffer. Should be non-empty.
   * @return Result<size_t>
   */
  Result<size_t> ReadSome(const MutableBytes& dest) {
    iovec v{.iov_base = dest.data(), .iov_len = dest.size()};
    return ReadSome(&v, 1);
  }

  virtual Result<size_t> ReadSome(const iovec* v, uint32_t len) = 0;

  /**
   * @brief Tries to read at least min_size bytes and at most dest.size() into dest.
   *        min_size should be not greater than dest.size().
   *
   *        Stops reading if one of the following happens:
   *        1. An error occurred or eof is reached (ReadSome() returned 0).
   *        2. At least min_size was read.
   *
   * @param dest
   * @param min_size
   * @return Result<size_t>
   */
  Result<size_t> ReadAtLeast(const MutableBytes& dest, size_t min_size);

  /**
   * @brief Attempts to fill dest till the end using ReadSome calls. Stops reading if one of the
   *        following happens:
   *        1. An error occurred.
   *        2. dest was fully filled, in that case dest.size() is returned.
   *        3. ReadSome returned 0 (eof), in that case Read returns n, 0 <= n <= dest.len().
   *
   *
   * @param dest
   * @return Result<size_t>
   */
  Result<size_t> Read(const MutableBytes& dest) {
    return ReadAtLeast(dest, dest.size());
  }
};

class Sink {
 public:
  virtual ~Sink() {
  }

  /**
   * @brief Writes a buffer into this sink, returning how many bytes were written.
   * This function will attempt to write the entire contents of buf, but the entire write
   * may not succeed, or the write may also generate an error. A call to write represents at
   * most one attempt to write to any wrapped object.
   *
   * If the return value is integer n then it must be guaranteed that n <= buf.len(). A return
   * value of 0 typically means that the underlying object is no longer able to accept bytes an
   * will likely not be able to in the future as well, or that the buffer provided is empty.
   *
   * @param dest - source buffer
   * @return Result<size_t>
   */
  Result<size_t> WriteSome(Bytes buf) {
    iovec v{.iov_base = const_cast<uint8_t*>(buf.data()), .iov_len = buf.size()};
    return WriteSome(&v, 1);
  }

  /**
   * @brief Writes a vector of buffers into this sink, returning how many bytes were written.
   *
   * @param len - must be positive
   */
  virtual Result<size_t> WriteSome(const iovec* v, uint32_t len) = 0;

  /**
   * @brief Writes the entire buffer.
   *
   * This method will continuously call WriteSome until there is no more data to
   * be written or an error is returned.
   *
   * @param buf
   * @return std::error_code
   */
  std::error_code Write(Bytes buf) {
    iovec v{.iov_base = const_cast<uint8_t*>(buf.data()), .iov_len = buf.size()};
    return Write(&v, 1);
  }

  /**
   * @brief Writes the entire io vector into this writer.
   *
   * @param vec
   * @param len - must be positive.
   * @return std::error_code
   */
  std::error_code Write(const iovec* vec, uint32_t len);
};

class AsyncSink {
 public:
  using AsyncWriteCb = std::function<void(Result<size_t>)>;

  // Dispatches the write call asynchronously and immediately exits.
  // The caller must make sure that (v, len) are valid until cb is called.
  virtual void AsyncWriteSome(const iovec* v, uint32_t len, AsyncWriteCb cb) = 0;

  // Wrapper around AsyncWriteSome that makes sure that the passed vectir is written to
  // completion. Copies (v, len) internally so it can be discarded after the call.
  void AsyncWrite(const iovec* v, uint32_t len, std::function<void(std::error_code)> cb);

  void AsyncWrite(Bytes buf, std::function<void(std::error_code)> cb) {
    iovec v{const_cast<uint8_t*>(buf.data()), buf.size()};
    AsyncWrite(&v, 1, std::move(cb));
  }
};

class PrefixSource : public Source {
 public:
  PrefixSource(Bytes prefix, Source* upstream) : prefix_(prefix), upstream_(upstream) {
  }

  Result<size_t> ReadSome(const iovec* v, uint32_t len) final;

  Bytes unused_prefix() const {
    return offs_ >= prefix_.size() ? Bytes{} : prefix_.subspan(offs_);
  }

 private:
  Bytes prefix_;
  Source* upstream_;
  size_t offs_ = 0;
};

class NullSink final : public Sink {
 public:
  Result<size_t> WriteSome(const iovec* v, uint32_t len);
};

class StringSink final : public ::io::Sink {
 public:
  ::io::Result<size_t> WriteSome(const iovec* v, uint32_t len);

  const std::string& str() const {
    return str_;
  }

  void Clear() {
    str_.clear();
  }

 private:
  std::string str_;
};

template <typename SomeFunc>
std::error_code ApplyExactly(const iovec* v, uint32_t len, SomeFunc&& func) {
  const iovec* endv = v + len;
  while (v != endv) {
    Result<size_t> res = func(v, endv - v);
    if (!res) {
      return res.error();
    }

    size_t done = *res;

    while (v != endv && done >= v->iov_len) {
      done -= v->iov_len;
      ++v;
    }

    if (done == 0)
      continue;

    // Finish the rest of the entry.
    uint8_t* next = reinterpret_cast<uint8_t*>(v->iov_base) + done;
    uint8_t* base_end = reinterpret_cast<uint8_t*>(v->iov_base) + v->iov_len;
    do {
      iovec iovv{next, size_t(base_end - next)};
      res = func(&iovv, 1);
      if (!res) {
        return res.error();
      }
      next += *res;
    } while (next != base_end);
    ++v;
  }
  return std::error_code{};
}

}  // namespace io
