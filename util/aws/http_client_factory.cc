// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/aws/http_client_factory.h"

namespace util {
namespace aws {

std::shared_ptr<Aws::Http::HttpClient> HttpClientFactory::CreateHttpClient(
    const Aws::Client::ClientConfiguration& clientConfiguration) const {
  return nullptr;
}

std::shared_ptr<Aws::Http::HttpRequest> HttpClientFactory::CreateHttpRequest(
    const Aws::String& uri, Aws::Http::HttpMethod method,
    const Aws::IOStreamFactory& streamFactory) const {
  return nullptr;
}

std::shared_ptr<Aws::Http::HttpRequest> HttpClientFactory::CreateHttpRequest(
    const Aws::Http::URI& uri, Aws::Http::HttpMethod method,
    const Aws::IOStreamFactory& streamFactory) const {
  return nullptr;
}

}  // namespace aws
}  // namespace util
