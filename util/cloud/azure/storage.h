// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <functional>
#include <string_view>
#include <system_error>

namespace util {
namespace cloud::azure {

class Credentials;

class Storage {
 public:
  Storage(Credentials* creds) : creds_(creds) {
  }

  using ContainerItem = std::string_view;
  using ObjectItem = std::string_view;

  std::error_code ListContainers(std::function<void(const ContainerItem&)> cb);
  std::error_code List(std::string_view container, unsigned max_results,
                       std::function<void(const ObjectItem&)> cb);

 private:
  Credentials* creds_;
};

} // namespace cloud::azure
}  // namespace util