// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <io/io.h>

#include <vector>

#include "util/cloud/gcp/gcp_creds_provider.h"
#include "util/http/http_client.h"

typedef struct ssl_ctx_st SSL_CTX;

namespace util {
namespace cloud {

class GCS {
 public:
  using BucketItem = std::string_view;
  struct ObjectItem {
    size_t size;
    std::string_view key;
    bool is_prefix;
  };

  using ListBucketCb = std::function<void(BucketItem)>;
  using ListObjectCb = std::function<void(const ObjectItem&)>;

  GCS(GCPCredsProvider* creds_provider, SSL_CTX* ssl_cntx, fb2::ProactorBase* pb);
  ~GCS();

  std::error_code Connect(unsigned msec);

  std::error_code ListBuckets(ListBucketCb cb);
  std::error_code List(std::string_view bucket, std::string_view prefix, bool recursive,
                       ListObjectCb cb);
 private:
  GCPCredsProvider& creds_provider_;
  SSL_CTX* ssl_ctx_;
  std::unique_ptr<http::TlsClient> client_;
};

}  // namespace cloud
}  // namespace util