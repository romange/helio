// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/aws/credentials_provider_chain.h"

#include <aws/core/platform/Environment.h>

#include "base/logging.h"

namespace util {
namespace aws {

CredentialsProviderChain::CredentialsProviderChain() {
  providers_.push_back(std::make_pair(
      "environment", std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>()));
  providers_.push_back(
      std::make_pair("profile config file",
                     std::make_shared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>()));

  const auto ec2_metadata_disabled = Aws::Environment::GetEnv("AWS_EC2_METADATA_DISABLED");
  if (Aws::Utils::StringUtils::ToLower(ec2_metadata_disabled.c_str()) != "true") {
    providers_.push_back(std::make_pair(
        "ec2 metadata", std::make_shared<Aws::Auth::InstanceProfileCredentialsProvider>()));
  } else {
    LOG(INFO) << "aws: disabled EC2 metadata";
  }
}

Aws::Auth::AWSCredentials CredentialsProviderChain::GetAWSCredentials() {
  for (const auto& provider : providers_) {
    Aws::Auth::AWSCredentials credentials = provider.second->GetAWSCredentials();
    if (!credentials.GetAWSAccessKeyId().empty() && !credentials.GetAWSSecretKey().empty()) {
      LOG_FIRST_N(INFO, 1) << "aws: loaded credentials; provider=" << provider.first;
      return credentials;
    }
  }

  LOG(ERROR) << "aws: failed to load credentials; no credential provider found";
  return Aws::Auth::AWSCredentials{};
}

}  // namespace aws
}  // namespace util
