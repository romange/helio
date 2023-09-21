// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <aws/core/client/ClientConfiguration.h>
#include <aws/core/http/HttpClient.h>

#include <boost/beast/core/flat_buffer.hpp>

#include "util/fibers/proactor_base.h"

namespace util {
namespace aws {

// HTTP client manages connecting to and sending HTTP requests.
//
// This is stateless so can be accessed by multiple threads.
class HttpClient : public Aws::Http::HttpClient {
 public:
  HttpClient(const Aws::Client::ClientConfiguration& client_conf);

  // Sends the given HTTP request to the server and returns a response.
  //
  // Requests will only be retried if the request is idempotent and it gets a
  // network error on a connection that was previously healthy. HTTP requests
  // are considered idempotent it they have methods GET, HEAD, OPTIONS, or
  // TRACE.
  std::shared_ptr<Aws::Http::HttpResponse> MakeRequest(
      const std::shared_ptr<Aws::Http::HttpRequest>& request,
      Aws::Utils::RateLimits::RateLimiterInterface* readLimiter = nullptr,
      Aws::Utils::RateLimits::RateLimiterInterface* writeLimiter = nullptr) const override;

  void DisableRequestProcessing() override;

  void EnableRequestProcessing() override;

  bool IsRequestProcessingEnabled() const override;

  void RetryRequestSleep(std::chrono::milliseconds sleep_time) override;

 private:
  io::Result<std::unique_ptr<FiberSocketBase>> Connect(const std::string& host, uint16_t port,
                                                       ProactorBase* proactor) const;

  io::Result<boost::asio::ip::address> Resolve(const std::string& host,
                                               ProactorBase* proactor) const;

  Aws::Client::ClientConfiguration client_conf_;
};

}  // namespace aws
}  // namespace util
