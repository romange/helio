// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <io/io.h>

#include <vector>

#include "util/cloud/gcp/gcp_creds_provider.h"
#include "util/http/http_client.h"
#include "util/http/https_client_pool.h"

typedef struct ssl_ctx_st SSL_CTX;

namespace util {
namespace cloud {

class GCS {
 public:
  using BucketItem = std::string_view;
  using ObjectItem = StorageListItem;

  using ListBucketCb = std::function<void(BucketItem)>;
  using ListObjectCb = std::function<void(const ObjectItem&)>;

  GCS(GCPCredsProvider* creds_provider, SSL_CTX* ssl_cntx, fb2::ProactorBase* pb);
  ~GCS();

  std::error_code ListBuckets(ListBucketCb cb);
  std::error_code List(std::string_view bucket, std::string_view prefix, bool recursive,
                       ListObjectCb cb);

  http::ClientPool* GetConnectionPool() {
    return client_pool_.get();
  }

  static std::unique_ptr<http::ClientPool> CreateApiConnectionPool(SSL_CTX* ssl_ctx,
                                                                   fb2::ProactorBase* pb);

 private:
  GCPCredsProvider& creds_provider_;
  SSL_CTX* ssl_ctx_;
  std::unique_ptr<http::ClientPool> client_pool_;
};

}  // namespace cloud
}  // namespace util