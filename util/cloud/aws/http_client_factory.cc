// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/aws/http_client_factory.h"

namespace util {
namespace cloud {
namespace aws {

std::shared_ptr<Aws::Http::HttpClient> HttpClientFactory::CreateHttpClient(
    const Aws::Client::ClientConfiguration& clientConfiguration) const {
  // TODO(andydunstall)
  return nullptr;
}

std::shared_ptr<Aws::Http::HttpRequest> HttpClientFactory::CreateHttpRequest(
    const Aws::String& uri, Aws::Http::HttpMethod method,
    const Aws::IOStreamFactory& streamFactory) const {
  // TODO(andydunstall)
  return nullptr;
}

std::shared_ptr<Aws::Http::HttpRequest> HttpClientFactory::CreateHttpRequest(
    const Aws::Http::URI& uri, Aws::Http::HttpMethod method,
    const Aws::IOStreamFactory& streamFactory) const {
  // TODO(andydunstall)
  return nullptr;
}

}  // namespace aws
}  // namespace cloud
}  // namespace util
