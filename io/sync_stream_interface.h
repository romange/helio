// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <sys/uio.h>

#include "io/io.h"

namespace io {

class SyncStreamInterface {
 public:
  virtual ~SyncStreamInterface() {
  }

  virtual Result<size_t> Send(const iovec* ptr, size_t len) = 0;
  virtual Result<size_t> Recv(iovec* ptr, size_t len) = 0;
};

class NullSinkStream final : public SyncStreamInterface {
 public:
  Result<size_t> Send(const iovec* ptr, size_t len);
  Result<size_t> Recv(iovec* ptr, size_t len) {
    return 0;
  }
};

}  // namespace io
