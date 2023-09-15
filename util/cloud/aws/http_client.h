// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <aws/core/http/HttpClient.h>

#include <memory>

#include "util/http/http_client.h"

namespace util {
namespace cloud {
namespace aws {

class HttpClient : public Aws::Http::HttpClient {
 public:
  HttpClient();

  std::shared_ptr<Aws::Http::HttpResponse> MakeRequest(
      const std::shared_ptr<Aws::Http::HttpRequest>& request,
      Aws::Utils::RateLimits::RateLimiterInterface* readLimiter = nullptr,
      Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter = nullptr) const override;

  void DisableRequestProcessing() override;

  void EnableRequestProcessing() override;

  bool IsRequestProcessingEnabled() const override;

  bool ContinueRequest(const Aws::Http::HttpRequest&) const override;

  void RetryRequestSleep(std::chrono::milliseconds sleepTime) override;

 private:
  std::unique_ptr<http::Client> http_client_;
};

}  // namespace aws
}  // namespace cloud
}  // namespace util
