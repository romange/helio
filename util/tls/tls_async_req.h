// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <cstddef>
#include <cstdint>
#include <utility>

#include "util/fiber_socket_base.h"

namespace util {
namespace tls {

class TestDelegator;
class TlsAsyncIo;
class TlsSocket;

// Drives one TLS read or write request until it invokes its completion callback.
class TlsAsyncReq {
 public:
  enum Role : std::uint8_t { READER, WRITER };
  // Captures one caller request and the components that advance it.
  TlsAsyncReq(TlsSocket* owner, TlsAsyncIo* async_io, io::AsyncProgressCb cb, const iovec* v,
              uint32_t len, Role role)
      : owner_(owner), async_io_(async_io), caller_completion_cb_(std::move(cb)), vec_(v),
        len_(len), role_(role) {
  }
  TlsAsyncReq(const TlsAsyncReq&) = delete;
  TlsAsyncReq& operator=(const TlsAsyncReq&) = delete;
  TlsAsyncReq(TlsAsyncReq&&) = delete;
  TlsAsyncReq& operator=(TlsAsyncReq&&) = delete;

  // Dispatches the next action requested by the TLS engine.
  void HandleOpAsync(int op_val);
  // Starts an upstream write for the engine's pending output.
  void StartUpstreamWrite();
  void SetEngineWritten(size_t written) {
    engine_written_ = written;
  }

 private:
  friend class TestDelegator;

  TlsSocket* owner_;
  TlsAsyncIo* async_io_;
  io::AsyncProgressCb caller_completion_cb_;
  const iovec* vec_;
  uint32_t len_;
  Role role_;
  iovec scratch_iovec_ = {};
  size_t engine_written_ = 0;
  bool should_read_ = false;

  // Flushes output, then starts the upstream read required by the engine.
  void MaybeSendOutputAsyncWithRead();
  // Flushes pending TLS output when the engine requests a write.
  void MaybeSendOutputAsync();
  // Starts an upstream read into the engine input buffer.
  void StartUpstreamRead();
  // Releases request ownership and destroys this after invoking the caller callback.
  void CompleteAsyncReq(io::Result<size_t> result);
  // Continues or completes the request after an upstream write callback.
  void AsyncWriteProgressCb(io::Result<size_t> write_result);
  // Continues or completes the request after an upstream read callback.
  void AsyncReadProgressCb(io::Result<size_t> result);
  // Advances the read or write state machine after an engine operation.
  void AsyncRoleBasedAction();
  // Resumes the request deferred behind a completed upstream operation.
  void RunPending();
};

}  // namespace tls
}  // namespace util