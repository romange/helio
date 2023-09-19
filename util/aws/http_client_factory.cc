// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/aws/http_client_factory.h"

#include <aws/core/http/standard/StandardHttpRequest.h>

#include "util/aws/http_client.h"

namespace util {
namespace aws {

std::shared_ptr<Aws::Http::HttpClient> HttpClientFactory::CreateHttpClient(
    const Aws::Client::ClientConfiguration& client_conf) const {
  return Aws::MakeShared<HttpClient>("helio", client_conf);
}

std::shared_ptr<Aws::Http::HttpRequest> HttpClientFactory::CreateHttpRequest(
    const Aws::String& uri, Aws::Http::HttpMethod method,
    const Aws::IOStreamFactory& stream_factory) const {
  return CreateHttpRequest(Aws::Http::URI(uri), method, stream_factory);
}

std::shared_ptr<Aws::Http::HttpRequest> HttpClientFactory::CreateHttpRequest(
    const Aws::Http::URI& uri, Aws::Http::HttpMethod method,
    const Aws::IOStreamFactory& stream_factory) const {
  auto request = Aws::MakeShared<Aws::Http::Standard::StandardHttpRequest>("helio", uri, method);
  request->SetResponseStreamFactory(stream_factory);
  return request;
}

}  // namespace aws
}  // namespace util
