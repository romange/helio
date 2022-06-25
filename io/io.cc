// Copyright 2022, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "io/io.h"

#include <absl/container/fixed_array.h>

#include <cstring>

#include "base/logging.h"

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

struct AsyncWriteState {
  absl::FixedArray<iovec, 4> arr;
  iovec *cur;
  AsyncSink* owner;

  function<void(error_code)> cb;

  AsyncWriteState(const iovec* v, uint32_t length) : arr(length) {
    cur = arr.data();
    std::copy(v, v + length, cur);
  }

  void OnCb(Result<size_t> res);
};

void AsyncWriteState::OnCb(Result<size_t> res) {
  if (!res) {
    cb(res.error());
    delete this;
    return;
  }

  size_t sz = *res;
  while (cur->iov_len <= sz) {
    sz -= cur->iov_len;
    ++cur;

    if (cur == arr.end()) {  // Successfully finished all the operations.
      DCHECK_EQ(0u, sz);

      cb(error_code{});

      delete this;
      return;
    }
  }

  char* base = (char*)cur->iov_base;
  cur->iov_len -= sz;
  cur->iov_base = base + sz;

  // continue issuing requests.
  owner->AsyncWriteSome(cur, arr.end() - cur, [this](Result<size_t> res) { this->OnCb(res); });
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
  return ApplyExactly(v, len,
                      [this](const auto* v, uint32_t len) { return WriteSomeBytes(v, len, this); });
  return error_code{};
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

void AsyncSink::AsyncWrite(const iovec* v, uint32_t len, function<void(error_code)> cb) {
  AsyncWriteState* state = new AsyncWriteState(v, len);
  state->owner = this;
  state->cb = std::move(cb);
  AsyncWriteSome(state->arr.data(), len, [state](Result<size_t> res) { state->OnCb(res); });
}

}  // namespace io
