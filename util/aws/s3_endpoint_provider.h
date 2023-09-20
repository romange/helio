// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <aws/s3/S3Client.h>

namespace util {
namespace aws {

// S3 endpoint provider.
//
// We override the default to support configuring custom endpoints, which the
// C++ SDK doesn't yet support.
class S3EndpointProvider : public Aws::S3::S3EndpointProvider {
 public:
  // Configure a non-empty endpoint string to configure a custom endpoint.
  S3EndpointProvider(const std::string& endpoint = "");

  Aws::Endpoint::ResolveEndpointOutcome ResolveEndpoint(
      const Aws::Endpoint::EndpointParameters& endpoint_params) const override;

 private:
  std::string endpoint_;
};

}  // namespace aws
}  // namespace util
