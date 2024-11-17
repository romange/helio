// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <string>
#include <system_error>

#include "util/cloud/utils.h"

namespace util {
namespace cloud::azure {

class Credentials : public CredentialsProvider {
 public:
  std::error_code Init(unsigned) final;

  const std::string& account_name() const {
    return account_name_;
  }
  const std::string& account_key() const {
    return account_key_;
  }

  std::string GetEndpoint() const;

  void Sign(detail::HttpRequestBase* req) const final;
  std::error_code RefreshToken() final;

 private:
  std::string account_name_;
  std::string account_key_;
};

}  // namespace cloud::azure
}  // namespace util