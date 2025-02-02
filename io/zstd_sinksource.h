// Copyright 2025, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <memory>

#include "io/io.h"

namespace io {

class ZStdSource : public Source {
 public:
  explicit ZStdSource(Source* upstream);
  ~ZStdSource();

 private:
  Result<size_t> ReadSome(const iovec* v, uint32_t len) override;

  std::unique_ptr<Source> sub_stream_;
  void* zstd_handle_;
  std::unique_ptr<uint8_t[]> buf_;
  uint32_t offs_ = 0, buf_len_ = 0;
};

}  // namespace io
