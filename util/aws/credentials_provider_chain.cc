// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/aws/credentials_provider_chain.h"

#include <aws/core/platform/Environment.h>

#include "base/logging.h"

namespace util {
namespace aws {

CredentialsProviderChain::CredentialsProviderChain() {
  AddProvider(Aws::MakeShared<Aws::Auth::EnvironmentAWSCredentialsProvider>("helio"));
  AddProvider(Aws::MakeShared<Aws::Auth::ProfileConfigFileAWSCredentialsProvider>("helio"));

  const auto ec2_metadata_disabled = Aws::Environment::GetEnv("AWS_EC2_METADATA_DISABLED");
  if (Aws::Utils::StringUtils::ToLower(ec2_metadata_disabled.c_str()) != "true") {
    AddProvider(Aws::MakeShared<Aws::Auth::InstanceProfileCredentialsProvider>("helio"));
  } else {
    LOG(INFO) << "aws: disabled EC2 metadata";
  }
}

}  // namespace aws
}  // namespace util
