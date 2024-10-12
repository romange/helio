// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
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

    // We use BIO buffers, therefore any SSL operation can end up writing to the internal BIO
    // and result in success, even though the data has not been flushed to the underlying socket.
    // See https://www.openssl.org/docs/man1.0.2/man3/BIO_new_bio_pair.html
    // As a result, we must flush output buffer (if OutputPending() > 0)if  before we do any
    // Socket reads. We could flush after each SSL operation but that would result in fragmented
    // Socket writes which we want to avoid.
    NEED_READ_AND_MAYBE_WRITE = -2,

    // Happens when we write large chunks of data into the ssl engine,
    // and it needs to flush its buffers.
    NEED_WRITE = -3,
  };

  using error_code = ::std::error_code;
  using Buffer = io::Bytes;
  using MutableBuffer = io::MutableBytes;

  // If OpResult has error code then it's an openssl error (as returned by ERR_get_error()).
  // In that case the wrapping flow should be stopped since any error there is unretriable.
  // if OpResult has value, then its non-negative value means success depending on the context
  // of the operation. If value == EOF_STREAM it means that a peer closed the SSL connection.
  // if value == NEED_XXX then it means that it should either write data to IO and then read or just
  // write. In any case for non-error OpResult a caller must check OutputPending and write the
  // output buffer to the appropriate channel.
  using OpResult = io::Result<int, unsigned long>;

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

  //! Returns output buffer which is the read buffer of tls engine.
  //! This operation is not destructive. The buffer contains the encrypted data that
  //! should be written to the upstream socket.
  Buffer PeekOutputBuf();

  //! Tells the engine that sz bytes were consumed from the output (read) buffer.
  //! sz should be not greater than the buffer size from the last PeekOutputBuf() call.
  void ConsumeOutputBuf(unsigned sz);

  //! Writes the buffer into input ssl buffer.
  //! Returns number of written bytes or the error.
  //! TODO: should be replaced with PeekInputBuf, memcpy, CommitInput sequence.
  OpResult WriteBuf(const Buffer& buf);

  // We usually use this function to write from the raw socket to SSL engine.
  // Returns direct reference to the input (write) buffer. This operation is not destructive.
  MutableBuffer PeekInputBuf() const;

  // Commits the input buffer. sz should be not greater
  // than the buffer size from PeekInputBuf() call
  void CommitInput(unsigned sz);

  // Returns size of pending encrypted data that needs to be flushed out from SSL to I/O.
  // See https://www.openssl.org/docs/man1.0.2/man3/BIO_new_bio_pair.html
  // Specifically, warning that says: "An application must not rely on the error value of
  // SSL_operation() but must assure that the write buffer is always flushed first".
  size_t OutputPending() const {
    return BIO_ctrl(external_bio_, BIO_CTRL_PENDING, 0, NULL);
  }

  //! Returns number of pending (encrypted) bytes written to the engine but not yet processed by it.
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

  SSL* ssl_;
  BIO* external_bio_;
};

/// Tries to load CA certificates from predefined (hardcoded) locations.
/// Returns 0 on success, -1 on failure.
int SslProbeSetDefaultCALocation(SSL_CTX* ctx);

}  // namespace tls
}  // namespace util
