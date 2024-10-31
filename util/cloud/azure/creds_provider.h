// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <string>
#include <system_error>

namespace util {
namespace cloud::azure {

class CredsProvider {
 public:
  std::error_code Init();

  const std::string& account_name() const { return account_name_; }
  const std::string& account_key() const { return account_key_; }

  using ContainerItem = std::string_view;
  std::error_code ListContainers(std::function<void(ContainerItem)>);

 private:
  std::string account_name_;
  std::string account_key_;
};

};  // namespace cloud::azure
}  // namespace util