// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/tls/tls_socket.h"

#include <openssl/err.h>

#include "base/logging.h"
#include "util/tls/tls_engine.h"

namespace util {
namespace tls {

using namespace std;
using nonstd::make_unexpected;

namespace {

class error_category : public std::error_category {
 public:
  const char* name() const noexcept final {
    return "async.tls";
  }

  string message(int ev) const final;

  error_condition default_error_condition(int ev) const noexcept final;

  bool equivalent(int ev, const error_condition& condition) const noexcept final {
    return condition.value() == ev && &condition.category() == this;
  }

  bool equivalent(const error_code& error, int ev) const noexcept final {
    return error.value() == ev && &error.category() == this;
  }
};

string error_category::message(int ev) const {
  char buf[256];
  ERR_error_string_n(ev, buf, sizeof(buf));
  return buf;
}

error_condition error_category::default_error_condition(int ev) const noexcept {
  return error_condition{ev, *this};
}

error_category tls_category;

inline error_code SSL2Error(unsigned long err) {
  CHECK_LT(err, unsigned(INT_MAX));

  return error_code{int(err), tls_category};
}

}  // namespace

TlsSocket::TlsSocket(FiberSocketBase* next)
    : FiberSocketBase(next ? next->proactor() : nullptr), next_sock_(next) {
}

TlsSocket::~TlsSocket() {
}

void TlsSocket::InitSSL(SSL_CTX* context) {
  CHECK(!engine_);
  engine_.reset(new Engine{context});
}

auto TlsSocket::Shutdown(int how) -> error_code {
  DCHECK(engine_);

  return error_code{};
}

auto TlsSocket::Accept() -> AcceptResult {
  DCHECK(engine_);

  while (true) {
    Engine::OpResult op_result = engine_->Handshake(Engine::SERVER);
    if (!op_result) {
      return make_unexpected(SSL2Error(op_result.error()));
    }

    error_code ec = MaybeSendOutput();
    if (ec) {
      return make_unexpected(ec);
    }

    int op_val = *op_result;

    if (op_val >= 0) {  // Shutdown or empty read/write may return 0.
      break;
    }
    if (op_val == Engine::EOF_STREAM) {
      return make_unexpected(make_error_code(errc::connection_reset));
    }
    if (op_val == Engine::NEED_READ_AND_MAYBE_WRITE) {
      ec = HandleRead();
      if (ec)
        return make_unexpected(ec);
    }
  }

  return nullptr;
}

auto TlsSocket::Connect(const endpoint_type& ep) -> error_code {
  DCHECK(engine_);

  return error_code{};
}

auto TlsSocket::Close() -> error_code {
  DCHECK(engine_);

  return error_code{};
}

io::Result<size_t> TlsSocket::RecvMsg(const msghdr& msg, int flags) {
  DCHECK(engine_);
  DCHECK_GT(size_t(msg.msg_iovlen), 0u);

  DLOG_IF(INFO, flags) << "Flags argument is not supported " << flags;

  auto* io = msg.msg_iov;
  size_t io_len = msg.msg_iovlen;

  Engine::MutableBuffer dest{reinterpret_cast<uint8_t*>(io->iov_base), io->iov_len};
  size_t read_total = 0;

  while (true) {
    DCHECK(!dest.empty());

    size_t read_len = std::min(dest.size(), size_t(INT_MAX));

    Engine::OpResult op_result = engine_->Read(dest.data(), read_len);
    if (!op_result) {
      return make_unexpected(SSL2Error(op_result.error()));
    }

    error_code ec = MaybeSendOutput();
    if (ec) {
      return make_unexpected(ec);
    }

    int op_val = *op_result;

    if (op_val > 0) {
      read_total += op_val;

      if (size_t(op_val) == read_len) {
        if (size_t(op_val) < dest.size()) {
          dest.remove_prefix(op_val);
        } else {
          ++io;
          --io_len;
          if (io_len == 0)
            break;
          dest = Engine::MutableBuffer{reinterpret_cast<uint8_t*>(io->iov_base), io->iov_len};
        }
        continue;  // We read everything we asked for - lets retry.
      }
      break;
    }

    if (read_total)  // if we read something lets return it before we handle other states.
      break;

    if (op_val == Engine::EOF_STREAM) {
      return make_unexpected(make_error_code(errc::connection_reset));
    }

    if (op_val == Engine::NEED_READ_AND_MAYBE_WRITE) {
      ec = HandleRead();
      if (ec)
        return make_unexpected(ec);
    }
  }
  return read_total;
}

io::Result<size_t> TlsSocket::WriteSome(const iovec* ptr, uint32_t len) {
  DCHECK(engine_);
  DCHECK_GT(len, 0u);

  Engine::Buffer dest{reinterpret_cast<uint8_t*>(ptr->iov_base), ptr->iov_len};
  size_t send_total = 0;

  while (true) {
    DCHECK(!dest.empty());

    size_t send_len = std::min(dest.size(), size_t(INT_MAX));

    Engine::OpResult op_result = engine_->Write(dest);
    if (!op_result) {
      return make_unexpected(SSL2Error(op_result.error()));
    }

    error_code ec = MaybeSendOutput();
    if (ec) {
      return make_unexpected(ec);
    }

    int op_val = *op_result;

    if (op_val > 0) {
      send_total += op_val;

      if (size_t(op_val) == send_len) {
        if (size_t(op_val) < dest.size()) {
          dest.remove_prefix(op_val);
        } else {
          ++ptr;
          --len;
          if (len == 0)
            break;
          dest = Engine::MutableBuffer{reinterpret_cast<uint8_t*>(ptr->iov_base), ptr->iov_len};
        }
        continue;  // We read everything we asked for - lets retry.
      }
      break;
    }

    if (send_total)  // if we sent something lets return it before we handle other states.
      break;

    if (op_val == Engine::EOF_STREAM) {
      return make_unexpected(make_error_code(errc::connection_reset));
    }

    if (op_val == Engine::NEED_READ_AND_MAYBE_WRITE) {
      ec = HandleRead();
      if (ec)
        return make_unexpected(ec);
    }
  }

  return send_total;
}

// TODO: to implement async functionality.
void TlsSocket::AsyncWriteSome(const iovec* v, uint32_t len, AsyncWriteCb cb) {
  io::Result<size_t> res = WriteSome(v, len);
  cb(res);
}

SSL* TlsSocket::ssl_handle() {
  return engine_ ? engine_->native_handle() : nullptr;
}

auto TlsSocket::MaybeSendOutput() -> error_code {
  auto buf_result = engine_->PeekOutputBuf();
  CHECK(buf_result);

  if (!buf_result->empty()) {
    io::Result<size_t> write_result = next_sock_->WriteSome(*buf_result);
    if (!write_result) {
      return write_result.error();
    }
    CHECK_GT(*write_result, 0u);
    engine_->ConsumeOutputBuf(*write_result);
  }

  return error_code{};
}

auto TlsSocket::HandleRead() -> error_code {
  auto mut_buf = engine_->PeekInputBuf();
  io::Result<size_t> esz = next_sock_->Recv(mut_buf);
  if (!esz) {
    return esz.error();
  }
  engine_->CommitInput(*esz);

  return error_code{};
}

}  // namespace tls
}  // namespace util
