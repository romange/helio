// Copyright 2025, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <gmock/gmock.h>
#include <stdint.h>

#include <optional>

#include "util/tls/tls_socket.h"

namespace util::tls {

class MockEngine;

// This Delegator accesses private members of any class.
// Assign friendship to a class to enable access.
// We use only this class to avoid multiple friend declarations.
class TestDelegator {
 public:
  static void SetEngine(TlsSocket* sock, std::unique_ptr<Engine> engine) {
    sock->engine_ = std::move(engine);
  }
  static void SetState(TlsSocket* sock, uint32_t state) {
    sock->state_ = state;
  }

  static constexpr uint32_t GetWriteInProgress() {
    return TlsSocket::WRITE_IN_PROGRESS;
  }
  static constexpr uint32_t GetReadInProgress() {
    return TlsSocket::READ_IN_PROGRESS;
  }
  static void SetSsl(Engine& engine, SSL* ssl) {
    engine.ssl_ = ssl;
  }
};

// TLS Engine mock, allowing fine-grained control of SSL/TLS operations in unit tests.
class MockEngine : public Engine {
 public:
  // Use the protected constructor
  MockEngine() : Engine() {
    // Create a dummy SSL object so the destructor has something to check
    dummy_ctx_ = SSL_CTX_new(TLS_method());
    TestDelegator::SetSsl(*this, SSL_new(dummy_ctx_));
  }
  ~MockEngine() {
    SSL_CTX_free(dummy_ctx_);
  }
  MOCK_METHOD(SSL*, native_handle, (), (override));
  MOCK_METHOD(void, set_verify_mode, (int mode), (override));
  MOCK_METHOD(Engine::OpResult, Handshake, (Engine::HandshakeType type), (override));
  MOCK_METHOD(Engine::OpResult, Shutdown, (), (override));
  MOCK_METHOD(Engine::OpResult, Write, (const Engine::Buffer& data), (override));
  MOCK_METHOD(Engine::OpResult, Read, (uint8_t * dest, size_t len), (override));
  MOCK_METHOD(Engine::Buffer, PeekOutputBuf, (), (override));
  MOCK_METHOD(void, ConsumeOutputBuf, (unsigned sz), (override));
  MOCK_METHOD(Engine::MutableBuffer, PeekInputBuf, (), (const, override));
  MOCK_METHOD(void, CommitInput, (unsigned sz), (override));
  MOCK_METHOD(size_t, OutputPending, (), (const, override));
  MOCK_METHOD(size_t, InputPending, (), (const, override));

 private:
  SSL_CTX* dummy_ctx_{};
};

// FiberSocketBase mock, used to simulate socket I/O and error conditions in unit tests.
class MockFiberSocket : public FiberSocketBase {
 public:
  MockFiberSocket() : FiberSocketBase(nullptr) {
  }

  // --- Methods required for the TryRecv tests ---
  MOCK_METHOD(io::Result<size_t>, TryRecv, (io::MutableBytes), (override));
  MOCK_METHOD(io::Result<size_t>, TrySend, (io::Bytes), (override));

  // --- Missing Pure Virtuals (Added to fix Abstract Class error) ---
  MOCK_METHOD(AcceptResult, Accept, (), (override));
  MOCK_METHOD(error_code, Connect, (const endpoint_type&, std::function<void(int)>), (override));
  MOCK_METHOD(bool, IsOpen, (), (const, override));
  MOCK_METHOD(void, set_timeout, (uint32_t), (override));
  MOCK_METHOD(uint32_t, timeout, (), (const, override));
  MOCK_METHOD(void, ResetOnRecvHook, (), (override));
  MOCK_METHOD(io::Result<size_t>, TrySend, (const iovec*, uint32_t), (override));

  // --- Other Pure Virtuals already present ---
  MOCK_METHOD(io::Result<size_t>, WriteSome, (const iovec*, uint32_t), (override));
  MOCK_METHOD(void, AsyncReadSome, (const iovec*, uint32_t, io::AsyncProgressCb), (override));
  MOCK_METHOD(void, AsyncWriteSome, (const iovec*, uint32_t, io::AsyncProgressCb), (override));
  MOCK_METHOD(io::Result<size_t>, RecvMsg, (const msghdr&, int), (override));
  MOCK_METHOD(io::Result<size_t>, Recv, (const io::MutableBytes&, int), (override));
  MOCK_METHOD(native_handle_type, native_handle, (), (const, override));
  MOCK_METHOD(void, RegisterOnErrorCb, (std::function<void(uint32_t)>), (override));
  MOCK_METHOD(void, CancelOnErrorCb, (), (override));
  MOCK_METHOD(error_code, Close, (), (override));
  MOCK_METHOD(error_code, Shutdown, (int), (override));
  MOCK_METHOD(endpoint_type, LocalEndpoint, (), (const, override));
  MOCK_METHOD(endpoint_type, RemoteEndpoint, (), (const, override));
  MOCK_METHOD(bool, IsUDS, (), (const, override));
  MOCK_METHOD(error_code, Create, (unsigned short), (override));
  MOCK_METHOD(error_code, Bind, (const struct sockaddr*, unsigned), (override));
  MOCK_METHOD(error_code, Listen, (unsigned), (override));
  MOCK_METHOD(error_code, Listen, (uint16_t, unsigned), (override));
  MOCK_METHOD(error_code, ListenUDS, (const char*, mode_t, unsigned), (override));
  MOCK_METHOD(void, RegisterOnRecv, (OnRecvCb), (override));
};

}  // namespace util::tls