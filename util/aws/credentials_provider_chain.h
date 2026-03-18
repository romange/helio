// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <aws/core/auth/AWSCredentialsProviderChain.h>

namespace util {
namespace aws {

// Loads a chain of providers:
// 1. Environment variables
// 2. Local configuration file
// 3. Web identity (IRSA - IAM Roles for Service Accounts)
// 4. Container credentials (ECS and EKS Pod Identity)
// 5. EC2 metadata (unless the AWS_EC2_METADATA_DISABLED environment variable
// is set to 'true')
//
// Note we avoid using the default credentials chain to avoid blocking the
// thread, such as we don't support the process credential provider.
class CredentialsProviderChain : public Aws::Auth::AWSCredentialsProviderChain {
 public:
  CredentialsProviderChain();

  virtual Aws::Auth::AWSCredentials GetAWSCredentials() override;

 private:
  std::vector<std::pair<std::string, std::shared_ptr<AWSCredentialsProvider>>> providers_;
};

}  // namespace aws
}  // namespace util
