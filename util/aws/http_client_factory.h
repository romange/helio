// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <aws/core/http/HttpClientFactory.h>

#include <memory>

namespace util {
namespace aws {

class HttpClientFactory : public Aws::Http::HttpClientFactory {
 public:
  std::shared_ptr<Aws::Http::HttpClient> CreateHttpClient(
      const Aws::Client::ClientConfiguration& client_conf) const override;

  std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(
      const Aws::String& uri, Aws::Http::HttpMethod method,
      const Aws::IOStreamFactory& stream_factory) const override;

  std::shared_ptr<Aws::Http::HttpRequest> CreateHttpRequest(
      const Aws::Http::URI& uri, Aws::Http::HttpMethod method,
      const Aws::IOStreamFactory& stream_factory) const override;
};

}  // namespace aws
}  // namespace util
