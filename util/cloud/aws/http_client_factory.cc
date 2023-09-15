// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/aws/http_client_factory.h"

#include "util/cloud/aws/http_client.h"

namespace util {
namespace cloud {
namespace aws {

std::shared_ptr<Aws::Http::HttpClient> HttpClientFactory::CreateHttpClient(
    const Aws::Client::ClientConfiguration& clientConfiguration) const {
  return Aws::MakeShared<HttpClient>("helio");
}

std::shared_ptr<Aws::Http::HttpRequest> HttpClientFactory::CreateHttpRequest(
    const Aws::String& uri, Aws::Http::HttpMethod method,
    const Aws::IOStreamFactory& streamFactory) const {
  return CreateHttpRequest(Aws::Http::URI(uri), method, streamFactory);
}

std::shared_ptr<Aws::Http::HttpRequest> HttpClientFactory::CreateHttpRequest(
    const Aws::Http::URI& uri, Aws::Http::HttpMethod method,
    const Aws::IOStreamFactory& streamFactory) const {
  auto request = Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>("helio", uri, method);
  request->SetResponseStreamFactory(streamFactory);
  return request;
}

}  // namespace aws
}  // namespace cloud
}  // namespace util
