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

  const std::string& account_name() const {
    return account_name_;
  }
  const std::string& account_key() const {
    return account_key_;
  }

 private:
  std::string account_name_;
  std::string account_key_;
};

class Storage {
 public:
  Storage(CredsProvider* creds) : creds_(creds) {
  }

  using ContainerItem = std::string_view;
  using ObjectItem = std::string_view;

  std::error_code ListContainers(std::function<void(const ContainerItem&)> cb);
  std::error_code List(std::string_view container, std::function<void(const ObjectItem&)> cb);

 private:
  CredsProvider* creds_;
};

};  // namespace cloud::azure
}  // namespace util