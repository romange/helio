// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
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
  // CTOR/DTOR are defined in tls_socket_async.cc where TlsAsyncReq is complete for unique_ptr
  // cleanup.
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

  struct PushResult {
    size_t written;
    int engine_opcode;
  };

  enum class IoFlag : uint8_t { kReadInProgress, kWriteInProgress };

  int EngineRead(const iovec* v);
  PushResult PushUserDataToEngine(const iovec* v, uint32_t len);
  size_t EngineOutputPending() const;
  void EngineCommitInput(size_t size);
  void EngineConsumeOutput(size_t size);

  void StartUpstreamRead(iovec* scratch, io::AsyncProgressCb cb);
  void StartUpstreamWrite(iovec* scratch, io::AsyncProgressCb cb);
  // Returns false when no TLS output remains and no upstream write is scheduled.
  bool ContinueUpstreamWrite(iovec* scratch, io::AsyncProgressCb cb);
  void RunPending();

  // setter/ getters / clearers
  bool read_in_progress() const;
  bool write_in_progress() const;
  void clear_io_in_progress_and_notify(IoFlag flag);
  uint8_t flags_bits() const;
  size_t upstream_write() const;
  FiberSocketBase::native_handle_type native_handle() const;

  TlsSocket* owner_;

  // Two independently active logical operations, both can be alive simultaneously.
  std::unique_ptr<TlsAsyncReq> async_read_req_;
  std::unique_ptr<TlsAsyncReq> async_write_req_;

  // Aliases one of the owned requests while it waits for an in-flight operation.
  TlsAsyncReq* blocked_async_req_ = nullptr;
};

}  // namespace tls
}  // namespace util