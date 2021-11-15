// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <absl/types/span.h>
#include <openssl/ssl.h>

#include "io/io.h"

namespace util {
namespace tls {

class Engine {
 public:
  enum HandshakeType { CLIENT = 1, SERVER = 2 };
  enum OpCode {
    EOF_STREAM = -1,
    NEED_READ_AND_MAYBE_WRITE = -2,
    NEED_WRITE = -3,
  };

  using error_code = ::std::error_code;
  using Buffer = io::Bytes;
  using MutableBuffer = io::MutableBytes;

  // If OpResult has error code then it's an openssl error (as returned by ERR_get_error()).
  // In that case the wrapping flow should be stopped since any error there is unretriable.
  // if OpResult has value, then its non-negative value means success depending on the context
  // of the operation. If value == EOF_STREAM it means that a peer closed the SSL connection.
  // if value == NEED_XXX then it means that it should either write data to IO and then read or just write.
  // In any case for non-error OpResult a caller must check OutputPending and write the output buffer
  // to the appropriate channel.
  using OpResult = io::Result<int, unsigned long> ;
  using BufResult = io::Result<Buffer, unsigned long> ;

  // Construct a new engine for the specified context.
  explicit Engine(SSL_CTX* context);

  // Destructor.
  ~Engine();

  // Get the underlying implementation in the native type.
  SSL* native_handle() {
    return ssl_;
  }

  //! Set the peer verification mode. mode is a mask of values specified at
  //! https://www.openssl.org/docs/man1.1.1/man3/SSL_set_verify.html
  void set_verify_mode(int mode) {
    ::SSL_set_verify(ssl_, mode, ::SSL_get_verify_callback(ssl_));
  }

  // Perform an SSL handshake using either SSL_connect (client-side) or
  // SSL_accept (server-side).
  OpResult Handshake(HandshakeType type);

  OpResult Shutdown();

  // Write bytes to the SSL session. Non-negative value - says how much was written.
  OpResult Write(const Buffer& data);

  // Read bytes from the SSL session.
  OpResult Read(uint8_t* dest, size_t len);

  //! Returns output (read) buffer. This operation is destructive, i.e. after calling
  //! this function the buffer is being consumed.
  //! See OutputPending() for checking if there is a output buffer to consume.
  BufResult FetchOutputBuf();

  //! Returns output (read) buffer. This operation is not destructive and
  // following ConsumeOutputBuf should be called.
  BufResult PeekOutputBuf();

  // sz should be not greater than the buffer size from the last PeekOutputBuf() call.
  void ConsumeOutputBuf(unsigned sz);

  //! Writes the buffer into input ssl buffer.
  //! Returns number of written bytes or the error.
  OpResult WriteBuf(const Buffer& buf);

  MutableBuffer PeekInputBuf() const;
  void CommitInput(unsigned sz);

  // Returns size of pending data that needs to be flushed out from SSL to I/O.
  // See https://www.openssl.org/docs/man1.1.0/man3/BIO_new_bio_pair.html
  // Specifically, warning that says: "An application must not rely on the error value of
  // SSL_operation() but must assure that the write buffer is always flushed first".
  size_t OutputPending() const {
    return BIO_ctrl(external_bio_, BIO_CTRL_PENDING, 0, NULL);
  }

  //! It's a bit confusing but when we write into external_bio_ it's like
  //! and input buffer to the engine.
  size_t InputPending() const {
    return BIO_ctrl(external_bio_, BIO_CTRL_WPENDING, 0, NULL);
  }

 private:
  // Disallow copying and assignment.
  Engine(const Engine&) = delete;
  Engine& operator=(const Engine&) = delete;


  // Perform one operation. Returns > 0 on success.
  using EngineOp = int (Engine::*)(void*, std::size_t);

  static OpResult ToOpResult(const SSL* ssl, int result);

  SSL* ssl_;
  BIO* external_bio_;
};

}  // namespace tls
}  // namespace util
