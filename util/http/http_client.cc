// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/http/http_client.h"

#include <absl/strings/numbers.h>
#include <openssl/err.h>

#include <boost/asio/connect.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>

#include "base/logging.h"
#include "util/fiber_socket_base.h"
#include "util/fibers/dns_resolve.h"
#include "util/fibers/proactor_base.h"
#include "util/tls/tls_engine.h"
#include "util/tls/tls_socket.h"

namespace util {
namespace http {

using namespace std;

namespace h2 = boost::beast::http;
namespace ip = boost::asio::ip;
using boost_error = boost::system::error_code;
using util::tls::TlsSocket;

namespace {

// This can be used for debugging
int VerifyCallback(int ok, X509_STORE_CTX* ctx) {
  // since this is a client side, we don't do much here,
  VLOG(1) << "verify_callback: " << std::boolalpha << bool(ok);
  return ok;
}

}  // namespace

Client::Client(ProactorBase* proactor) : proactor_(proactor) {
}

Client::~Client() {
}

std::error_code Client::Connect(string_view host, string_view service) {
  uint32_t port;
  CHECK(absl::SimpleAtoi(service, &port));
  CHECK_LT(port, 1u << 16);

  port_ = port;
  host_ = host;
  return Reconnect();
}

std::error_code Client::Reconnect() {
  VLOG(1) << "Reconnecting to " << host_ << ":" << port_;

  if (socket_) {
    error_code ec = socket_->Close();
    LOG_IF(WARNING, !ec) << "Socket close failed: " << ec.message();
    socket_.reset();
  }

  char ip[INET_ADDRSTRLEN];
  error_code ec = fb2::DnsResolve(host_.data(), 2000, ip, proactor_);
  if (ec) {
    return ec;
  }

  boost_error berr;
  auto address = ip::make_address(ip, berr);
  if (berr)
    return berr;

  FiberSocketBase* sock = proactor_->CreateSocket();
  if (on_connect_cb_) {
    on_connect_cb_(sock->native_handle());
  }
  socket_.reset(sock);
  FiberSocketBase::endpoint_type ep{address, port_};
  return socket_->Connect(ep);
}

#if 0
auto Client::Send(Verb verb, string_view url, string_view body, Response* response) -> BoostError {
  // Set the URL
  h2::request<h2::string_body> req{verb, boost::string_view(url.data(), url.size()), 11};

  req.body().assign(body.begin(), body.end());
  req.prepare_payload();

  boost_error ec;

  AsioStreamAdapter<> adapter(*socket_);

  // Send the HTTP request to the remote host.
  h2::write(adapter, req, ec);
  if (ec) {
    VLOG(1) << "Error " << ec;
    return ec;
  }

  // This buffer is used for reading and must be persisted
  boost::beast::flat_buffer buffer;

  h2::read(adapter, buffer, *response, ec);
  VLOG(2) << "Resp: " << *response;

  return ec;
}
#endif

void Client::Shutdown() {
  if (socket_) {
    std::error_code ec = socket_->Shutdown(SHUT_RDWR);
    LOG_IF(WARNING, !ec) << "Socket Shutdown failed: " << ec.message();
    socket_.reset();
  }
}

bool Client::IsConnected() const {
  return socket_ && socket_->IsOpen();
}

///////////////////////////////////////////////////////////////////////////////

SSL_CTX* TlsClient::CreateSslContext() {
  SSL_CTX* ctx = SSL_CTX_new(TLS_client_method());
  if (ctx) {
    // Use the default locations for certificates. This means that any trusted
    // remote host by this local host, will be trusted as well.
    // see https://www.openssl.org/docs/man3.0/man1/openssl-verification-options.html
    SSL_CTX_set_min_proto_version(ctx, TLS1_2_VERSION);

    if (tls::SslProbeSetDefaultCALocation(ctx) != 0) {
      if (SSL_CTX_set_default_verify_paths(ctx) != 1) {
        LOG(WARNING) << "failed to set default verify path on client context for TLS connection";
        FreeContext(ctx);
        return nullptr;
      }
    }
    SSL_CTX_set_options(ctx, SSL_OP_DONT_INSERT_EMPTY_FRAGMENTS);
    SSL_CTX_set_verify(ctx, SSL_VERIFY_PEER, VerifyCallback);
    SSL_CTX_set_verify_depth(ctx, 4);
    // see https://www.openssl.org/docs/man1.1.1/man3/SSL_CTX_set_security_level.html
    // this default is for Security level set to 112 bits of security
    SSL_CTX_set_security_level(ctx, 2);
    SSL_CTX_dane_enable(ctx);  // see https://www.internetsociety.org/resources/deploy360/dane/
  }
  return ctx;
}

std::error_code TlsClient::Connect(string_view host, string_view service, SSL_CTX* context) {
  DCHECK(context) << " NULL SSL context";
  // Four phases:
  // 1. TCP connection
  // 2. Setting SSL level verification for the remote host
  // 3. Using the connected TCP for SSL (handshake).
  // 4. Setting the base class to use the "new" TLS socket from here on end

  VLOG(1) << "Connecting to " << host << ":" << service;

  std::error_code ec = Client::Connect(host, service);
  if (!ec) {
    std::unique_ptr<TlsSocket> tls_socket(std::make_unique<TlsSocket>(socket_.release()));
    tls_socket->InitSSL(context);

    const std::string& hn = Client::host();
    const char* host = hn.c_str();
    SSL* ssl_handle = tls_socket->ssl_handle();
    // add SNI
    SSL_set_tlsext_host_name(ssl_handle, host);
    // verify server cert using server hostname
    SSL_dane_enable(ssl_handle, host);
    ec = tls_socket->Connect(FiberSocketBase::endpoint_type{});
    if (!ec) {
      socket_.reset(tls_socket.release());
    }
  }
  return ec;
}

void TlsClient::FreeContext(SSL_CTX* ctx) {
  if (ctx) {
    SSL_CTX_free(ctx);
  }
}

}  // namespace http
}  // namespace util
