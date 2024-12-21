// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <functional>
#include <string_view>
#include <system_error>

#include "io/file.h"
#include "util/cloud/utils.h"

typedef struct ssl_ctx_st SSL_CTX;

namespace util {
namespace cloud::azure {

class Credentials;

class Storage {
 public:
  Storage(Credentials* creds) : creds_(creds) {
  }

  using ContainerItem = std::string_view;

  using ListItem = StorageListItem;
  std::error_code ListContainers(std::function<void(const ContainerItem&)> cb);
  std::error_code List(std::string_view container, std::string_view prefix, bool recursive,
                       unsigned max_results, std::function<void(const ListItem&)> cb);

 private:
  Credentials* creds_;
};

struct ReadFileOptions {
  Credentials* creds_provider = nullptr;
  SSL_CTX* ssl_cntx;
};

using WriteFileOptions = ReadFileOptions;

io::Result<io::ReadonlyFile*> OpenReadFile(const std::string& container,
                                           const std::string& key,
                                           const ReadFileOptions& opts);

io::Result<io::WriteFile*> OpenWriteFile(const std::string& container, const std::string& key,
                                         const WriteFileOptions& opts);

}  // namespace cloud::azure
}  // namespace util