// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstdint>
#include <memory>

#include "util/fiber_socket_base.h"

namespace util {
namespace tls {

class TlsSocket;
class TlsAsyncReq;
class TestDelegator;

// Owns the active TLS read/write requests and coordinates their asynchronous progress.
class TlsAsyncIo {
 public:
  // CTOR/DTOR are defined in tls_socket_async.cc where TlsAsyncReq is complete for unique_ptr cleanup.
  explicit TlsAsyncIo(TlsSocket* owner);
  ~TlsAsyncIo();

  // Starts an asynchronous read through the TLS engine.
  void AsyncReadSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb);
  // Starts an asynchronous write through the TLS engine.
  void AsyncWriteSome(const iovec* v, uint32_t len, io::AsyncProgressCb cb);
  // Drains already-buffered TLS output without accepting new user data.
  void StartAsyncWrite(io::AsyncProgressCb async_write_cb);

 private:
  friend class TlsAsyncReq;
  friend class TestDelegator;

  TlsSocket* owner_;
  std::unique_ptr<TlsAsyncReq> async_read_req_;
  std::unique_ptr<TlsAsyncReq> async_write_req_;

  // Aliases one of the owned requests while it waits for an in-flight operation.
  TlsAsyncReq* blocked_async_req_ = nullptr;
};

}  // namespace tls
}  // namespace util