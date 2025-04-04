// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#define ZSTD_STATIC_LINKING_ONLY

#include "io/zstd_sinksource.h"

#include <zstd.h>

#include "base/logging.h"

namespace io {

using namespace std;

#define DC_HANDLE reinterpret_cast<ZSTD_DStream*>(zstd_handle_)

const unsigned kReadSize = 1 << 12;

ZStdSource::ZStdSource(Source* upstream) : sub_stream_(upstream) {
  CHECK(upstream);
  zstd_handle_ = ZSTD_createDStream();
  size_t const res = ZSTD_initDStream(DC_HANDLE);
  CHECK(!ZSTD_isError(res)) << ZSTD_getErrorName(res);
  buf_.reset(new uint8_t[kReadSize]);
}

ZStdSource::~ZStdSource() {
  ZSTD_freeDStream(DC_HANDLE);
}

Result<size_t> ZStdSource::ReadSome(const iovec* v, uint32_t len) {
  DCHECK_GT(len, 0u);

  // TODO: fill entire iovec.
  ZSTD_outBuffer output = {v->iov_base, v->iov_len, 0};

  do {
    if (offs_ >= buf_len_) {
      auto res = sub_stream_->Read(MutableBytes(buf_.get(), kReadSize));
      if (!res)
        return res;
      if (*res == 0)  // EOF
        break;
      offs_ = 0;
      buf_len_ = *res;
    }

    ZSTD_inBuffer input{buf_.get() + offs_, buf_len_ - offs_, 0};

    size_t to_read = ZSTD_decompressStream(DC_HANDLE, &output, &input);
    if (ZSTD_isError(to_read)) {
      return nonstd::make_unexpected(make_error_code(errc::illegal_byte_sequence));
    }

    offs_ += input.pos;
    if (input.pos < input.size) {
      CHECK_EQ(output.pos, output.size);  // Invariant check.
    }
  } while (output.pos < output.size);

  return output.pos;
}

}  // namespace io
