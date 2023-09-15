#include "util/cloud/aws/http_client.h"

#include <aws/core/http/HttpRequest.h>
#include <aws/core/http/standard/StandardHttpResponse.h>

#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/message.hpp>
#include <boost/beast/http/string_body.hpp>

#include "base/logging.h"
#include "util/fibers/proactor_base.h"

namespace util {
namespace cloud {
namespace aws {

namespace h2 = boost::beast::http;

using namespace std::chrono_literals;

namespace {

h2::verb AwsMethodToBoost(Aws::Http::HttpMethod method) {
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

HttpClient::HttpClient() {
  ProactorBase* proactor = ProactorBase::me();
  CHECK(proactor) << "must run in a proactor thread";
  http_client_ = std::make_unique<http::Client>(proactor);
}

std::shared_ptr<Aws::Http::HttpResponse> HttpClient::MakeRequest(
    const std::shared_ptr<Aws::Http::HttpRequest>& request,
    Aws::Utils::RateLimits::RateLimiterInterface* readLimiter,
    Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter) const {
  // TODO(andydunstall) Why is S3 using HTTPS when HTTP was configured
  if (request->GetUri().GetScheme() != Aws::Http::Scheme::HTTP) {
    LOG(WARNING) << "only http is supported";
    // TODO(andydunstall) For now just update to http
    request->GetUri().SetScheme(Aws::Http::Scheme::HTTP);
    if (request->GetUri().GetPort() == 443) {
      request->GetUri().SetPort(80);
    }
  }

  // TODO(andydunstall): Check rate limitter

  const h2::verb method = AwsMethodToBoost(request->GetMethod());

  DVLOG(2) << "aws: http client: request; method=" << h2::to_string(method)
           << "; url=" << request->GetUri().GetURIString();
  for (const auto& h : request->GetHeaders()) {
    DVLOG(2) << "aws: http client: request; header=" << h.first << "=" << h.second;
  }

  // TODO(andydunstall): Verify HTTP version
  // TODO(andydunstall): Support non-empty body
  h2::request<h2::empty_body> req{method, request->GetUri().GetURIString(), 11};
  for (const auto& h : request->GetHeaders()) {
    req.set(h.first, h.second);
  }

  std::shared_ptr<Aws::Http::HttpResponse> response =
      Aws::MakeShared<Aws::Http::Standard::StandardHttpResponse>("helio", request);

  // TODO(andydunstall): HTTP client should automatically connect if its not
  // already connected. It MUST also support connecting to multiple addresses
  // from the same client (such as when redirected)

  // TODO(andydunstall): Hard code 80 for now
  auto ec = http_client_->Connect(request->GetUri().GetAuthority(), "80");
  if (ec) {
    LOG(WARNING) << "aws: http client: failed to connect: " << request->GetUri().GetAuthority()
                 << ": " << ec;

    // TODO(andydunstall): Handle appropriately
    response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
    return response;
  }

  h2::response<h2::string_body> resp;
  ec = http_client_->Send(req, &resp);
  if (ec) {
    LOG(WARNING) << "aws: http client: failed to send request: " << ec;
    response->SetClientErrorType(Aws::Client::CoreErrors::NETWORK_CONNECTION);
    return response;
  }

  response->SetResponseCode(static_cast<Aws::Http::HttpResponseCode>(resp.result_int()));
  for (const auto& h : resp.base()) {
    response->AddHeader(h.name_string(), h.value());
  }
  response->GetResponseBody().write(resp.body().data(), resp.body().size());

  DVLOG(2) << "aws: http client: response; status=" << resp.result_int();
  for (const auto& h : response->GetHeaders()) {
    DVLOG(2) << "aws: http client: response; header=" << h.first << "=" << h.second;
  }

  // TODO(andydunstall)

  return response;
}

void HttpClient::DisableRequestProcessing() {
  // TODO(andydunstall)
}

void HttpClient::EnableRequestProcessing() {
  // TODO(andydunstall)
}

bool HttpClient::IsRequestProcessingEnabled() const {
  // TODO(andydunstall)
  return true;
}

bool HttpClient::ContinueRequest(const Aws::Http::HttpRequest&) const {
  // TODO(andydunstall)
  return true;
}

void HttpClient::RetryRequestSleep(std::chrono::milliseconds sleepTime) {
  // TODO(andydunstall) Add an arbitrary sleep for now
  ThisFiber::SleepFor(5s);
}

}  // namespace aws
}  // namespace cloud
}  // namespace util
