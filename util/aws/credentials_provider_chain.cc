// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/aws/credentials_provider_chain.h"

#include <aws/core/platform/Environment.h>

#include "base/logging.h"

namespace util {
namespace aws {

CredentialsProviderChain::CredentialsProviderChain() {
  AddProvider(std::make_shared<Aws::Auth::EnvironmentAWSCredentialsProvider>());
  AddProvider(std::make_shared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>());

  const auto ec2_metadata_disabled = Aws::Environment::GetEnv("AWS_EC2_METADATA_DISABLED");
  if (Aws::Utils::StringUtils::ToLower(ec2_metadata_disabled.c_str()) != "true") {
    AddProvider(std::make_shared<Aws::Auth::InstanceProfileCredentialsProvider>());
  } else {
    LOG(INFO) << "aws: disabled EC2 metadata";
  }
}

}  // namespace aws
}  // namespace util
