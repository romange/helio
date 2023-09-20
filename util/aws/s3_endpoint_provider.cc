// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/aws/s3_endpoint_provider.h"

#include "base/logging.h"

namespace util {
namespace aws {

S3EndpointProvider::S3EndpointProvider(const std::string& endpoint) : endpoint_{endpoint} {
}

Aws::Endpoint::ResolveEndpointOutcome S3EndpointProvider::ResolveEndpoint(
    const Aws::Endpoint::EndpointParameters& endpoint_params) const {
  // If no custom endpoint is configured, use the aws default.
  if (endpoint_ == "") {
    Aws::Endpoint::ResolveEndpointOutcome outcome =
        Aws::S3::S3EndpointProvider::ResolveEndpoint(endpoint_params);
    if (outcome.IsSuccess()) {
      // TODO(andydunstall): We currently only support HTTP.
      Aws::Http::URI uri = outcome.GetResult().GetURI();
      uri.SetScheme(Aws::Http::Scheme::HTTP);
      outcome.GetResult().SetURI(uri);
    }
    return outcome;
  }

  // If a custom endpoint is configured, construct the URL. Note this misses
  // lots of functionality of Aws::S3::S3EndpointProvider, though we are
  // currently only using the custom endpoint for testing.

  std::string bucket;
  for (auto p : endpoint_params) {
    if (p.GetName() == "Bucket") {
      CHECK(p.GetString(bucket) == Aws::Endpoint::EndpointParameter::GetSetResult::SUCCESS);
    }
  }

  Aws::Endpoint::AWSEndpoint endpoint;
  // TODO(andydunstall): We currently only support HTTP.
  if (bucket != "") {
    endpoint.SetURL("http://" + endpoint_ + "/" + bucket);
  } else {
    endpoint.SetURL("http://" + endpoint_);
  }
  return Aws::Endpoint::ResolveEndpointOutcome(std::move(endpoint));
}

}  // namespace aws
}  // namespace util
