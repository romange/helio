#include "util/cloud/aws/http_client.h"

namespace util {
namespace cloud {
namespace aws {

std::shared_ptr<Aws::Http::HttpResponse> HttpClient::MakeRequest(
    const std::shared_ptr<Aws::Http::HttpRequest>& request,
    Aws::Utils::RateLimits::RateLimiterInterface* readLimiter,
    Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter) const {
  // TODO(andydunstall)
  return nullptr;
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
  // TODO(andydunstall)
}

}  // namespace aws
}  // namespace cloud
}  // namespace util
