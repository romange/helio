// Copyright 2022, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "io/io.h"

#include <cstring>

#include "base/logging.h"
#include "io/sync_stream_interface.h"

using namespace std;

namespace io {

namespace {

// Writes some bytes and handles some corner cases including retry on interrupted.
inline Result<size_t> WriteSomeBytes(const iovec* v, uint32_t len, Sink* dest) {
  Result<size_t> res;

  do {
    res = dest->WriteSome(v, len);
    if (res && *res == 0)
      return nonstd::make_unexpected(make_error_code(errc::io_error));
  } while (!res && res.error() == errc::interrupted);

  return res;
}

}  // namespace


Result<size_t> Source::ReadAtLeast(const MutableBytes& dest, size_t min_size) {
  DCHECK_GE(dest.size(), min_size);
  size_t to_read = 0;
  MutableBytes cur = dest;

  while (to_read < min_size) {
    io::Result<size_t> res = ReadSome(cur);
    if (!res)
      return res;

    if (*res == 0)
      break;

    to_read += *res;
    cur.remove_prefix(*res);
  }

  return to_read;
}

Result<size_t> PrefixSource::ReadSome(const iovec* v, uint32_t len) {
  CHECK(len > 0 && v != nullptr);

  if (offs_ < prefix_.size()) {
    size_t sz = std::min(prefix_.size() - offs_, v->iov_len);
    memcpy(v->iov_base, prefix_.data() + offs_, sz);
    offs_ += sz;
    return sz;
  }

  return upstream_->ReadSome(v, len);
}

error_code Sink::Write(const iovec* v, uint32_t len) {
  return ApplyExactly(v, len, [this](const auto* v, uint32_t len) {
    return WriteSomeBytes(v, len, this);
  });
  return error_code{};
}

Result<size_t> NullSinkStream::Send(const iovec* ptr, size_t len) {
  size_t res = 0;
  for (size_t i = 0; i < len; ++i) {
    res += ptr[i].iov_len;
  }
  return res;
}

Result<size_t> NullSink::WriteSome(const iovec* v, uint32_t len) {
  size_t res = 0;
  for (uint32_t i = 0; i < len; ++i) {
    res += v[i].iov_len;
  }
  return res;
}

::io::Result<size_t> StringSink::WriteSome(const iovec* ptr, uint32_t len) {
  size_t res = 0;
  for (size_t i = 0; i < len; ++i) {
    str_.append((char*)ptr[i].iov_base, ptr[i].iov_len);
    res += ptr[i].iov_len;
  }
  return res;
}

}  // namespace io
