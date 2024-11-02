// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include "base/RWSpinLock.h"
#include "util/cloud/utils.h"

namespace util {

namespace fb2 {
class ProactorBase;
}  // namespace fb2

namespace cloud {

class GCPCredsProvider : public CredentialsProvider {
  GCPCredsProvider(const GCPCredsProvider&) = delete;
  GCPCredsProvider& operator=(const GCPCredsProvider&) = delete;

 public:
  GCPCredsProvider() = default;

  std::error_code Init(unsigned connect_ms) final;

  void Sign(detail::HttpRequestBase* req) const final;

  // Thread-safe method issues refresh of the token.
  // Right now will do the refresh unconditonally.
  // TODO: to use expire_time_ to skip the refresh if expire time is far away.
  std::error_code RefreshToken();

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

 private:
  bool use_instance_metadata_ = false;
  unsigned connect_ms_ = 0;

  std::string account_id_;
  std::string project_id_;

  std::string client_id_, client_secret_, refresh_token_;

  mutable folly::RWSpinLock lock_;  // protects access_token_
  std::string access_token_;
  std::atomic<time_t> expire_time_ = 0;  // seconds since epoch
};

}  // namespace cloud
}  // namespace util