// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <io/io.h>

#include <vector>

#include "util/http/http_client.h"
#include "base/RWSpinLock.h"

typedef struct ssl_ctx_st SSL_CTX;

namespace util {

namespace fb2 {
class ProactorBase;
}  // namespace fb2

namespace cloud {

class GCPCredsProvider {
  GCPCredsProvider(const GCPCredsProvider&) = delete;
  GCPCredsProvider& operator=(const GCPCredsProvider&) = delete;

 public:
  GCPCredsProvider() = default;

  std::error_code Init(unsigned connect_ms, fb2::ProactorBase* pb);

  const std::string& project_id() const {
    return project_id_;
  }

  const std::string& client_id() const {
    return client_id_;
  }

  // Thread-safe method to access the token.
  std::string access_token() const {
    folly::RWSpinLock::ReadHolder lock(lock_);
    return access_token_;
  }

  time_t expire_time() const {
    return expire_time_.load(std::memory_order_acquire);
  }

  // Thread-safe method issues refresh of the token.
  // Right now will do the refresh unconditonally.
  // TODO: to use expire_time_ to skip the refresh if expire time is far away.
  std::error_code RefreshToken(fb2::ProactorBase* pb);

 private:
  bool use_instance_metadata_ = false;
  unsigned connect_ms_ = 0;

  fb2::ProactorBase* pb_ = nullptr;
  std::string account_id_;
  std::string project_id_;

  std::string client_id_, client_secret_, refresh_token_;

  mutable folly::RWSpinLock lock_;  // protects access_token_
  std::string access_token_;
  std::atomic<time_t> expire_time_ = 0;  // seconds since epoch
};

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