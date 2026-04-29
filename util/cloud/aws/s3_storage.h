// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <functional>
#include <memory>
#include <string_view>
#include <system_error>

#include "absl/base/attributes.h"
#include "io/file.h"
#include "util/cloud/aws/aws_creds_provider.h"
#include "util/cloud/utils.h"
#include "util/http/https_client_pool.h"

typedef struct ssl_ctx_st SSL_CTX;

namespace util {

namespace fb2 {
class ProactorBase;
}  // namespace fb2

namespace cloud::aws {

class S3Storage {
 public:
  S3Storage(AwsCredsProvider* creds, SSL_CTX* ssl_cntx, fb2::ProactorBase* pb);
  ~S3Storage();

  using BucketItem = std::string_view;
  using ListItem = StorageListItem;

  std::error_code ListBuckets(std::function<void(const BucketItem&)> cb);

  // Deprecated wrapper, kept only to be able to backport helio to older Dragonfly branches.
  ABSL_MUST_USE_RESULT std::error_code List(std::string_view bucket, std::string_view prefix,
                                            bool recursive, unsigned max_results,
                                            std::function<void(const ListItem&)> cb) {
    std::string continuation_token;
    return List(bucket, prefix, recursive, max_results, std::move(cb), &continuation_token);
  }

  // Lists objects under `bucket` matching `prefix`. Performs a single request.
  //
  // `max_results` is the page size (max-keys).
  //
  // `continuation_token` must not be null: on input it is the resume token (empty means
  // start from the beginning), on output it holds the token for the next page, or is
  // cleared if this was the last page. Callers drive pagination by looping until the
  // token comes back empty.
  ABSL_MUST_USE_RESULT std::error_code List(std::string_view bucket, std::string_view prefix,
                                            bool recursive, unsigned max_results,
                                            std::function<void(const ListItem&)> cb,
                                            std::string* continuation_token);

 private:
  std::string BucketEndpoint(std::string_view bucket) const;

  AwsCredsProvider* creds_;
  SSL_CTX* ssl_cntx_;
  fb2::ProactorBase* pb_;
  std::unique_ptr<http::ClientPool> pool_;
};

struct ReadFileOptions {
  AwsCredsProvider* creds_provider = nullptr;
  SSL_CTX* ssl_cntx = nullptr;
};

using WriteFileOptions = ReadFileOptions;

io::Result<io::ReadonlyFile*> OpenReadFile(const std::string& bucket, const std::string& key,
                                           const ReadFileOptions& opts);

io::Result<io::WriteFile*> OpenWriteFile(const std::string& bucket, const std::string& key,
                                         const WriteFileOptions& opts);

}  // namespace cloud::aws
}  // namespace util