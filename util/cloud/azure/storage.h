// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <functional>
#include <string_view>
#include <system_error>

#include "absl/base/attributes.h"
#include "io/file.h"
#include "util/cloud/azure/creds_provider.h"
#include "util/cloud/utils.h"

typedef struct ssl_ctx_st SSL_CTX;

namespace util {
namespace cloud::azure {

class Storage {
 public:
  explicit Storage(Credentials* creds) : creds_(creds) {
  }

  using ContainerItem = std::string_view;

  using ListItem = StorageListItem;
  std::error_code ListContainers(std::function<void(const ContainerItem&)> cb);

  // Deprecated wrapper, kept only to be able to backport helio to older Dragonfly branches.
  ABSL_MUST_USE_RESULT std::error_code List(std::string_view container, std::string_view prefix,
                                            bool recursive, unsigned max_results,
                                            std::function<void(const ListItem&)> cb);

  // Lists blobs under `container` matching `prefix`. Performs a single request.
  //
  // `max_results` is the page size (maxresults).
  //
  // `continuation_token` must not be null: on input it is the resume marker (empty means
  // start from the beginning), on output it holds the marker for the next page, or is
  // cleared if this was the last page. Callers drive pagination by looping until the
  // marker comes back empty.
  ABSL_MUST_USE_RESULT std::error_code List(std::string_view container, std::string_view prefix,
                                             bool recursive, unsigned max_results,
                                             std::function<void(const ListItem&)> cb,
                                             std::string* continuation_token);

 private:
  Credentials* creds_;
};

struct ReadFileOptions {
  Credentials* creds_provider = nullptr;
  SSL_CTX* ssl_cntx = nullptr;
};

using WriteFileOptions = ReadFileOptions;

io::Result<io::ReadonlyFile*> OpenReadFile(const std::string& container, const std::string& key,
                                           const ReadFileOptions& opts);

io::Result<io::WriteFile*> OpenWriteFile(const std::string& container, const std::string& key,
                                         const WriteFileOptions& opts);

}  // namespace cloud::azure
}  // namespace util