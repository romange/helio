// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <atomic>
#include <ctime>
#include <string>
#include <system_error>

#include "base/RWSpinLock.h"
#include "util/cloud/utils.h"

namespace util {
namespace cloud::azure {

// Endpoint and account identity resolved from a BlobEndpoint URL or account name.
struct AccountInfo {
  std::string service_endpoint;
  std::string account_name;
  std::string path_prefix;  // non-empty for path-style endpoints (e.g. "/devstoreaccount1")
  bool is_https = true;
};

class Credentials : public CredentialsProvider {
 public:
  enum class AuthMode { kNone, kSharedKey, kSas, kBearer };

  std::error_code Init(unsigned) final;

  const std::string& account_name() const {
    return account_info_.account_name;
  }
  const std::string& account_key() const {
    return account_key_;
  }

  std::string ServiceEndpoint() const;

  bool IsHttps() const {
    return account_info_.is_https;
  }

  // Returns the path prefix from the BlobEndpoint URL (e.g. "/devstoreaccount1" for Azurite).
  // Empty for real Azure endpoints.
  const std::string& GetPathPrefix() const {
    return account_info_.path_prefix;
  }

  void Sign(detail::HttpRequestBase* req) const final;
  std::error_code RefreshToken() final;

 private:
  enum class CredSource { kNone, kConnectionString, kEnv, kManagedIdentity };

  std::error_code TryConnectionString();
  std::error_code TryEnvSharedKey();
  std::error_code TryManagedIdentity();

  // Helpers to commit credential state. These (and Init/TryXxx) are the only writers;
  // Sign() reads these fields without a lock, so they must not change after Init() returns
  // (except via RefreshToken, which only updates access_token_ under lock_).
  void SetCredentials(CredSource src, AccountInfo info, AuthMode mode, std::string key_or_sas);
  void SetSharedKey(CredSource src, AccountInfo info, std::string key);
  void SetSas(CredSource src, AccountInfo info, std::string sas);
  void SetBearer(AccountInfo info, std::string token, unsigned ttl);

  static std::string NormalizeSasQuery(std::string_view query);

  // Immutable after Init() — read by Sign() without locking.
  CredSource source_ = CredSource::kNone;
  AuthMode auth_mode_ = AuthMode::kNone;
  AccountInfo account_info_;
  std::string account_key_;
  std::string sas_query_;

  // Mutable at runtime — protected by lock_ / atomic.
  mutable folly::RWSpinLock lock_;
  std::string access_token_;  // guarded by lock_
  std::atomic<time_t> expire_time_ = 0;

  std::string managed_identity_client_id_;
  unsigned connect_ms_ = 0;
};

}  // namespace cloud::azure
}  // namespace util
