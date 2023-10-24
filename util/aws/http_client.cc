// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/aws/http_client.h"

#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/standard/StandardHttpResponse.h>

#include <boost/beast/http/message.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>
#include <boost/interprocess/streams/bufferstream.hpp>

#include "base/logging.h"
#include "util/asio_stream_adapter.h"
#include "util/fibers/dns_resolve.h"
#include "util/http/http_client.h"
#include "util/tls/tls_socket.h"

namespace util {
namespace aws {

namespace h2 = boost::beast::http;

namespace {

constexpr unsigned kHttpVersion1_1 = 11;

h2::verb BoostMethod(Aws::Http::HttpMethod method) {
  switch (method) {
    case Aws::Http::HttpMethod::HTTP_GET:
      return h2::verb::get;
    case Aws::Http::HttpMethod::HTTP_POST:
      return h2::verb::post;
    case Aws::Http::HttpMethod::HTTP_DELETE:
      return h2::verb::delete_;
    case Aws::Http::HttpMethod::HTTP_PUT:
      return h2::verb::put;
    case Aws::Http::HttpMethod::HTTP_HEAD:
      return h2::verb::head;
    case Aws::Http::HttpMethod::HTTP_PATCH:
      return h2::verb::patch;
    default:
      LOG(ERROR) << "aws: http client: invalid http method: " << static_cast<int>(method);
      return h2::verb::unknown;
  }
}

}  // namespace

HttpClient::HttpClient(const Aws::Client::ClientConfiguration& client_conf)
    : client_conf_{client_conf} {
  ctx_ = util::http::TlsClient::CreateSslContext();
}

HttpClient::~HttpClient() {
  SSL_CTX_free(ctx_);
}

std::shared_ptr<Aws::Http::HttpResponse> HttpClient::MakeRequest(
    const std::shared_ptr<Aws::Http::HttpRequest>& request,
    Aws::Utils::RateLimits::RateLimiterInterface* readLimiter,
    Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter) const {
  VLOG(1) << "aws: http client: request; method="
          << Aws::Http::HttpMethodMapper::GetNameForHttpMethod(request->GetMethod())
          << "; url=" << request->GetUri().GetURIString()
          << "; scheme=" << Aws::Http::SchemeMapper::ToString(request->GetUri().GetScheme());
  for (const auto& h : request->GetHeaders()) {
    DVLOG(2) << "aws: http client: request; header=" << h.first << "=" << h.second;
  }

  ProactorBase* proactor = ProactorBase::me();
  CHECK(proactor) << "aws: http client: must run in a proactor thread";

  h2::request<h2::string_body> boost_req{BoostMethod(request->GetMethod()),
                                         request->GetUri().GetURIString(), kHttpVersion1_1};
  for (const auto& h : request->GetHeaders()) {
    boost_req.set(h.first, h.second);
  }

  // If the body is a known type with an underlying buffer we can access
  // directly without copying, we don't include a body in the boost request but
  // instead write the buffer directly to the socket.
  //
  // So h2::write will only write the header (where content-length etc have
  // already been set by the AWS SDK), then we write the body directly.
  //
  // This is a bit of a hack, though it saves us doing expensive copies,
  // especially when we're uploading 10MB file parts.
  boost::interprocess::bufferstream* buf_body = nullptr;
  if (request->GetContentBody()) {
    buf_body = dynamic_cast<boost::interprocess::bufferstream*>(request->GetContentBody().get());
    // Only copy if we don't know the type.
    if (!buf_body) {
      int content_size;
      absl::SimpleAtoi(request->GetContentLength(), &content_size);
      std::string s(content_size, '0');
      request->GetContentBody()->read(s.data(), s.size());
      boost_req.body() = s;
    }
  }

  std::shared_ptr<Aws::Http::HttpResponse> response =
      std::make_shared<Aws::Http::Standard::StandardHttpResponse>(request);

  // To keep the HTTP client simple and thread safe, reconect on each request.
  // Given we have large request/responses (8MB) the cost of reconnecting
  // is negligible.
  io::Result<std::unique_ptr<FiberSocketBase>> connect_res =
      Connect(request->GetUri().GetAuthority(), request->GetUri().GetPort(), proactor);
  if (!connect_res) {
    response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
    response->SetClientErrorMessage("Failed to connect to host");
    return response;
  }

  std::unique_ptr<FiberSocketBase> conn = std::move(*connect_res);
  if (request->GetUri().GetScheme() == Aws::Http::Scheme::HTTPS) {
    std::unique_ptr<util::tls::TlsSocket> tls_conn(
        std::make_unique<util::tls::TlsSocket>(conn.release()));
    tls_conn->InitSSL(ctx_);

    const std::string& hn = request->GetUri().GetAuthority();
    const char* host = hn.c_str();
    SSL* ssl_handle = tls_conn->ssl_handle();
    // Add SNI.
    SSL_set_tlsext_host_name(ssl_handle, host);
    // Verify server cert using server hostname.
    SSL_dane_enable(ssl_handle, host);
    std::error_code ec = tls_conn->Connect(FiberSocketBase::endpoint_type{});
    if (ec) {
      LOG(WARNING) << "aws: http clent: tls connect failed; error=" << ec;
      response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
      tls_conn->Close();
      return response;
    }
    conn.reset(tls_conn.release());
  }

  boost::system::error_code bec;
  AsioStreamAdapter<> adapter(*conn);
  h2::write(adapter, boost_req, bec);
  if (bec) {
    LOG(WARNING) << "aws: http client: failed to send request; method="
                 << Aws::Http::HttpMethodMapper::GetNameForHttpMethod(request->GetMethod())
                 << "; url=" << request->GetUri().GetURIString() << "; error=" << bec;
    response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
    conn->Close();
    return response;
  }

  std::error_code ec;
  // As described above, if we have a known type write the body directly
  // without copying (the headers are written in h2::write.
  if (buf_body) {
    auto [buf, size] = buf_body->buffer();
    ec = conn->Write(io::Bytes{reinterpret_cast<const uint8_t*>(buf), size});
    if (ec) {
      LOG(WARNING) << "aws: http client: failed to send request body; method="
                   << Aws::Http::HttpMethodMapper::GetNameForHttpMethod(request->GetMethod())
                   << "; url=" << request->GetUri().GetURIString() << "; error=" << ec;
      response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
      conn->Close();
      return response;
    }
  }

  h2::response<h2::string_body> boost_resp;
  boost::beast::flat_buffer buf;
  h2::read(adapter, buf, boost_resp, bec);
  if (bec) {
    LOG(WARNING) << "aws: http client: failed to read response; method="
                 << Aws::Http::HttpMethodMapper::GetNameForHttpMethod(request->GetMethod())
                 << "; url=" << request->GetUri().GetURIString() << "; error=" << bec;
    response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
    conn->Close();
    return response;
  }

  response->SetResponseCode(static_cast<Aws::Http::HttpResponseCode>(boost_resp.result_int()));
  for (const auto& h : boost_resp.base()) {
    response->AddHeader(std::string(h.name_string()), std::string(h.value()));
  }
  // Copy the response. This can be improved in the future.
  response->GetResponseBody().write(boost_resp.body().data(), boost_resp.body().size());

  VLOG(1) << "aws: http client: response; status=" << boost_resp.result_int();
  for (const auto& h : response->GetHeaders()) {
    DVLOG(2) << "aws: http client: response; header=" << h.first << "=" << h.second;
  }

  conn->Close();

  return response;
}

void HttpClient::DisableRequestProcessing() {
  // Unused so we don't need to implement.
}

void HttpClient::EnableRequestProcessing() {
  // Unused so we don't need to implement.
}

bool HttpClient::IsRequestProcessingEnabled() const {
  // Unused so we don't need to implement.
  return true;
}

void HttpClient::RetryRequestSleep(std::chrono::milliseconds sleep_time) {
  // Override to only block the fiber, not the thread.
  ThisFiber::SleepFor(sleep_time);
}

io::Result<std::unique_ptr<FiberSocketBase>> HttpClient::Connect(const std::string& host,
                                                                 uint16_t port,
                                                                 ProactorBase* proactor) const {
  VLOG(1) << "aws: http client: connecting; host=" << host << "; port=" << port;

  io::Result<boost::asio::ip::address> addr = Resolve(host, proactor);
  if (!addr) {
    return nonstd::make_unexpected(addr.error());
  }

  std::unique_ptr<FiberSocketBase> socket;
  socket.reset(proactor->CreateSocket());
  FiberSocketBase::endpoint_type ep{*addr, port};
  std::error_code ec = socket->Connect(ep);
  if (ec) {
    LOG(WARNING) << "aws: http client: failed to connect; host=" << host << "; error=" << ec;
    socket->Close();
    return nonstd::make_unexpected(ec);
  }

  VLOG(1) << "aws: http client: connected; host=" << host << "; port=" << port;

  if (client_conf_.enableTcpKeepAlive) {
    ec = EnableKeepAlive(socket->native_handle());
    if (ec) {
      // Log the error but we still continue with the request.
      LOG(ERROR) << "aws: http client: failed to enable tcp keep alive; error=" << ec;
    } else {
      DVLOG(2) << "aws: http client: enabled tcp keep alive";
    }
  }

  return socket;
}

io::Result<boost::asio::ip::address> HttpClient::Resolve(const std::string& host,
                                                         ProactorBase* proactor) const {
  VLOG(1) << "aws: http client: resolving host; host=" << host;

  char ip[INET_ADDRSTRLEN];
  std::error_code ec = fb2::DnsResolve(host.data(), client_conf_.connectTimeoutMs, ip, proactor);
  if (ec) {
    LOG(WARNING) << "aws: http client: failed to resolve host; host=" << host << "; error=" << ec;
    return nonstd::make_unexpected(ec);
  }

  boost::system::error_code bec;
  boost::asio::ip::address address = boost::asio::ip::make_address(ip, bec);
  if (bec) {
    LOG(ERROR) << "aws: http client: resolved invalid address; error=" << ec;
    return nonstd::make_unexpected(ec);
  }

  VLOG(1) << "aws: http client: resolved host; host=" << host << "; ip=" << address;

  return address;
}

std::error_code HttpClient::EnableKeepAlive(int fd) const {
  int val = 1;
  if (setsockopt(fd, SOL_SOCKET, SO_KEEPALIVE, &val, sizeof(val)) < 0) {
    return std::error_code(errno, std::system_category());
  }

  val = client_conf_.tcpKeepAliveIntervalMs / 1000;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPINTVL, &val, sizeof(val)) < 0) {
    return std::error_code(errno, std::system_category());
  }

  val = client_conf_.tcpKeepAliveIntervalMs / 1000;
#ifdef __APPLE__
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPALIVE, &val, sizeof(val)) < 0) {
    return std::error_code(errno, std::system_category());
  }
#else
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPIDLE, &val, sizeof(val)) < 0) {
    return std::error_code(errno, std::system_category());
  }
#endif

  val = 3;
  if (setsockopt(fd, IPPROTO_TCP, TCP_KEEPCNT, &val, sizeof(val)) < 0) {
    return std::error_code(errno, std::system_category());
  }

  return std::error_code{};
}

}  // namespace aws
}  // namespace util
