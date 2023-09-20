// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/aws/http_client.h"

#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/core/http/standard/StandardHttpResponse.h>

#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>

#include "base/logging.h"
#include "util/asio_stream_adapter.h"
#include "util/fibers/dns_resolve.h"

namespace util {
namespace aws {

namespace h2 = boost::beast::http;

namespace {

// TODO(andydunstall): Check what versions boost supports
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
    : client_conf_{client_conf}, disable_request_processing_{false} {
  // TODO(andydunstall): Handle client conf
  proactor_ = ProactorBase::me();
  CHECK(proactor_) << "must run in a proactor thread";
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

  h2::request<h2::string_body> boost_req{BoostMethod(request->GetMethod()),
                                         request->GetUri().GetURIString(), kHttpVersion1_1};
  for (const auto& h : request->GetHeaders()) {
    boost_req.set(h.first, h.second);
  }
  // TODO(andydunstall) We can avoid this copy by adding a custom body type
  // that reads directly from the body std::iostream.
  if (request->GetContentBody()) {
    int content_size;
    absl::SimpleAtoi(request->GetContentLength(), &content_size);
    std::string s(content_size, '0');
    request->GetContentBody()->read(s.data(), s.size());
    boost_req.body() = s;
  }

  std::shared_ptr<Aws::Http::HttpResponse> response =
      Aws::MakeShared<Aws::Http::Standard::StandardHttpResponse>("helio", request);

  // TODO(andydunstall): Cache connections
  io::Result<std::unique_ptr<FiberSocketBase>> connect_res =
      Connect(request->GetUri().GetAuthority(), request->GetUri().GetPort());
  if (!connect_res) {
    response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
    return response;
  }
  std::unique_ptr<FiberSocketBase> conn = std::move(*connect_res);

  boost::system::error_code ec;
  AsioStreamAdapter<> adapter(*conn);
  h2::write(adapter, boost_req, ec);
  if (ec) {
    LOG(WARNING) << "aws: http client: failed to send request; method="
                 << Aws::Http::HttpMethodMapper::GetNameForHttpMethod(request->GetMethod())
                 << "; url=" << request->GetUri().GetURIString() << "; error=" << ec;
    response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
    return response;
  }
  h2::response<h2::string_body> boost_resp;
  h2::read(adapter, buf_, boost_resp, ec);
  if (ec) {
    LOG(WARNING) << "aws: http client: failed to read response; method="
                 << Aws::Http::HttpMethodMapper::GetNameForHttpMethod(request->GetMethod())
                 << "; url=" << request->GetUri().GetURIString() << "; error=" << ec;
    response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
    return response;
  }

  response->SetResponseCode(static_cast<Aws::Http::HttpResponseCode>(boost_resp.result_int()));
  for (const auto& h : boost_resp.base()) {
    response->AddHeader(h.name_string(), h.value());
  }
  // TODO(andydunstall) We can avoid this copy by adding a custom body type
  // that writes directly from the body std::iostream.
  response->GetResponseBody().write(boost_resp.body().data(), boost_resp.body().size());

  VLOG(1) << "aws: http client: response; status=" << boost_resp.result_int();
  for (const auto& h : response->GetHeaders()) {
    DVLOG(2) << "aws: http client: response; header=" << h.first << "=" << h.second;
  }

  conn->Close();

  return response;
}

void HttpClient::DisableRequestProcessing() {
  disable_request_processing_ = true;
  request_processing_signal_.notify_all();
}

void HttpClient::EnableRequestProcessing() {
  disable_request_processing_ = false;
}

bool HttpClient::IsRequestProcessingEnabled() const {
  return disable_request_processing_.load() == false;
}

void HttpClient::RetryRequestSleep(std::chrono::milliseconds sleep_time) {
  std::unique_lock<fb2::Mutex> lk(request_processing_signal_lock_);
  request_processing_signal_.wait_for(
      lk, sleep_time, [this]() { return disable_request_processing_.load() == true; });
}

io::Result<std::unique_ptr<FiberSocketBase>> HttpClient::Connect(const std::string& host,
                                                                 uint16_t port) const {
  VLOG(1) << "aws: http client: connecting; host=" << host << "; port=" << port;

  io::Result<boost::asio::ip::address> addr = Resolve(host);
  if (!addr) {
    return nonstd::make_unexpected(addr.error());
  }

  std::unique_ptr<FiberSocketBase> socket;
  socket.reset(proactor_->CreateSocket());
  FiberSocketBase::endpoint_type ep{*addr, port};
  // TODO(andydunstall): Add connect timeout (client_conf_.connectTimeoutMs)
  std::error_code ec = socket->Connect(ep);
  if (ec) {
    LOG(WARNING) << "aws: http client: failed to connect; host=" << host << "; error=" << ec;
    socket->Close();
    return nonstd::make_unexpected(ec);
  }

  VLOG(1) << "aws: http client: connected; host=" << host << "; port=" << port;

  return socket;
}

io::Result<boost::asio::ip::address> HttpClient::Resolve(const std::string& host) const {
  VLOG(1) << "aws: http client: resolving host; host=" << host;

  char ip[INET_ADDRSTRLEN];
  std::error_code ec = fb2::DnsResolve(host.data(), client_conf_.connectTimeoutMs, ip, proactor_);
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

}  // namespace aws
}  // namespace util
