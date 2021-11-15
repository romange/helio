// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/http/https_client.h"

#include "base/logging.h"
#include "util/asio/io_context.h"

#ifdef BOOST_ASIO_SEPARATE_COMPILATION
#include <boost/asio/ssl/impl/src.hpp>
#endif

// libssl1.0.x has performance locking bugs that practically prevent scalable communication
// with ssl library. It seems that libssl1.1 fixes that. libssl-dev in 18.04 installs libssl1.1.
#if OPENSSL_VERSION_NUMBER < 0x10100000L
#warning Please update your libssl to libssl1.1 - install libssl-dev
#endif

DEFINE_bool(https_client_disable_ssl, false, "");

namespace util {
namespace http {

using namespace boost;
using namespace std;
namespace h2 = beast::http;

namespace {
constexpr const char kPort[] = "443";

::boost::system::error_code SslConnect(SslStream* stream, unsigned ms) {
  system::error_code ec;
  for (unsigned i = 0; i < 2; ++i) {
    ec = stream->next_layer().ClientWaitToConnect(ms);
    if (ec) {
      VLOG(1) << "Error " << i << ": " << ec << "/" << ec.message() << " for socket "
              << stream->next_layer().native_handle();

      continue;
    }

    stream->handshake(asio::ssl::stream_base::client, ec);
    if (!ec) {
      auto* cipher = SSL_get_current_cipher(stream->native_handle());
      VLOG(1) << "SSL handshake success " << i << ", chosen " << SSL_CIPHER_get_name(cipher) << "/"
              << SSL_CIPHER_get_version(cipher);
    }
    return ec;
  }

  return ec;
}

}  // namespace

SslContextResult CreateClientSslContext(std::string_view cert_string) {
  system::error_code ec;
  asio::ssl::context cntx{asio::ssl::context::tlsv12_client};
  cntx.set_options(boost::asio::ssl::context::default_workarounds |
                   boost::asio::ssl::context::no_compression | boost::asio::ssl::context::no_sslv2 |
                   boost::asio::ssl::context::no_sslv3 | boost::asio::ssl::context::no_tlsv1 |
                   boost::asio::ssl::context::no_tlsv1_1);
  cntx.set_verify_mode(asio::ssl::verify_peer, ec);
  if (ec) {
    return SslContextResult(ec);
  }
  cntx.add_certificate_authority(asio::buffer(cert_string.data(), cert_string.size()), ec);
  if (ec) {
    return SslContextResult(ec);
  }
  SSL_CTX* ssl_cntx = cntx.native_handle();

  long flags = SSL_CTX_get_options(ssl_cntx);
  flags |= SSL_OP_CIPHER_SERVER_PREFERENCE;
  SSL_CTX_set_options(ssl_cntx, flags);

  constexpr char kCiphers[] = "ECDHE-ECDSA-AES128-GCM-SHA256:ECDHE-RSA-AES128-GCM-SHA256";
  CHECK_EQ(1, SSL_CTX_set_cipher_list(ssl_cntx, kCiphers));
  CHECK_EQ(1, SSL_CTX_set_ecdh_auto(ssl_cntx, 1));

  return SslContextResult(std::move(cntx));
}

HttpsClient::HttpsClient(std::string_view host, IoContext* context,
                         ::boost::asio::ssl::context* ssl_ctx)
    : io_context_(*context), ssl_cntx_(*ssl_ctx), host_name_(host) {
}

auto HttpsClient::Connect(unsigned msec) -> error_code {
  CHECK(io_context_.InContextThread());

  reconnect_msec_ = msec;

  return InitSslClient();
}

auto HttpsClient::InitSslClient() -> error_code {
  VLOG(2) << "Https::InitSslClient " << reconnect_needed_;

  error_code ec;
  if (!reconnect_needed_)
    return ec;
  if (FLAGS_https_client_disable_ssl) {
    socket_.reset(new FiberSyncSocket{host_name_, "80", &io_context_});
    socket_->set_keep_alive(true);
    ec = socket_->ClientWaitToConnect(reconnect_msec_);
    if (ec) {
      VLOG(1) << "Error " << ": " << ec << "/" << ec.message() << " for socket "
              << socket_->native_handle();
    }
  } else {
    client_.reset(new SslStream(FiberSyncSocket{host_name_, kPort, &io_context_}, ssl_cntx_));
    client_->next_layer().set_keep_alive(true);

    if (SSL_set_tlsext_host_name(client_->native_handle(), host_name_.c_str()) != 1) {
      char buf[128];
      ERR_error_string_n(::ERR_peek_last_error(), buf, sizeof(buf));

      LOG(FATAL) << "Could not set hostname: " << buf;
    }

    ec = SslConnect(client_.get(), reconnect_msec_);
    if (!ec) {
      reconnect_needed_ = false;
    } else {
      VLOG(1) << "Error connecting " << ec << ", socket " << client_->next_layer().native_handle();
    }
  }
  return ec;
}

auto HttpsClient::DrainResponse(h2::response_parser<h2::buffer_body>* parser) -> error_code {
  if (parser->is_done())
    return error_code{};

  constexpr size_t kBufSize = 1 << 16;
  std::unique_ptr<uint8_t[]> buf(new uint8_t[kBufSize]);
  auto& body = parser->get().body();
  error_code ec;
  size_t sz = 0;

  // Drain pending response completely to allow reusing the current connection.
  VLOG(1) << "parser: " << parser->got_some();
  while (!parser->is_done()) {
    body.data = buf.get();
    body.size = kBufSize;
    size_t raw_bytes;

    if (client_)
      raw_bytes = h2::read(*client_, tmp_buffer_, *parser, ec);
    else
      raw_bytes = h2::read(*socket_, tmp_buffer_, *parser, ec);

    if (ec && ec != h2::error::need_buffer) {
      VLOG(1) << "Error " << ec << "/" << ec.message();
      return ec;
    }
    sz += raw_bytes;

    VLOG(1) << "DrainResp: " << raw_bytes << "/" << body.size;
  }
  VLOG_IF(1, sz > 0) << "Drained " << sz << " bytes";

  return error_code{};
}

bool HttpsClient::HandleWriteError(const error_code& ec) {
  if (!ec)
    return true;

  LOG(WARNING) << "Write returned error " << ec << "/" << ec.message();
  reconnect_needed_ = true;

  return false;
}

}  // namespace http
}  // namespace util
