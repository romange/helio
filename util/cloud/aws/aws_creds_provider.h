// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include "base/RWSpinLock.h"
#include "util/cloud/utils.h"

typedef struct ssl_ctx_st SSL_CTX;

namespace util {
namespace cloud::aws {

struct AwsCredentials {
  std::string access_key_id;
  std::string secret_access_key;
  std::string session_token;  // empty for static credentials
  time_t expiry = 0;          // 0 = no expiry; refreshed 30s early

  bool empty() const {
    return access_key_id.empty() || secret_access_key.empty();
  }

  bool IsExpired() const {
    return expiry > 0 && time(nullptr) + 30 >= expiry;
  }
};

// Implements CredentialsProvider for AWS.
//
// Credential chain (same order as current util/aws/ SDK chain):
//   1. Environment vars:  AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY / AWS_SESSION_TOKEN
//   2. Profile file:      ~/.aws/credentials  (AWS_SHARED_CREDENTIALS_FILE / AWS_PROFILE)
//   3. Web identity IRSA: AWS_ROLE_ARN + AWS_WEB_IDENTITY_TOKEN_FILE -> STS
//   4. Container creds:   AWS_CONTAINER_CREDENTIALS_RELATIVE_URI or _FULL_URI
//   5. EC2 IMDSv2:        http://169.254.169.254/
//
// Sign() computes AWS Signature Version 4 on every call.
// RefreshToken() re-fetches from whichever source originally succeeded.
// Thread-safe: RWSpinLock guards creds_.
class AwsCredsProvider : public CredentialsProvider {
  AwsCredsProvider(const AwsCredsProvider&) = delete;
  AwsCredsProvider& operator=(const AwsCredsProvider&) = delete;

 public:
  // `region` empty -> defaults to AWS_REGION / AWS_DEFAULT_REGION / "us-east-1".
  // `endpoint_override` empty -> falls back to AWS_S3_ENDPOINT, then
  //   "s3.{region}.amazonaws.com". Accepts a bare "host[:port]" or a full URL
  //   (e.g. "https://s3.dualstack.us-west-2.amazonaws.com"); scheme and path
  //   are stripped.
  explicit AwsCredsProvider(std::string region = {}, std::string endpoint_override = {});
  ~AwsCredsProvider();

  unsigned connect_ms() const { return connect_ms_; }

  std::error_code Init(unsigned connect_ms) final;

  // Returns the resolved S3 endpoint host[:port]. Precedence: ctor override >
  // AWS_S3_ENDPOINT env var > "s3.{region}.amazonaws.com".
  std::string ServiceEndpoint() const final;

  // Computes AWS SigV4 and sets x-amz-date, x-amz-security-token, Authorization headers.
  void Sign(detail::HttpRequestBase* req) const final;

  std::error_code RefreshToken() final;

  const std::string& region() const {
    return region_;
  }

 private:
  SSL_CTX* GetSslCtx();  // creates ssl_ctx_ on first call

  std::error_code TryEnvironment();
  std::error_code TryProfileFile();
  std::error_code TryWebIdentity();
  std::error_code TryContainerCreds();
  std::error_code TryIMDS();

  std::string region_;
  std::string endpoint_override_;
  unsigned connect_ms_ = 2000;

  enum class CredSource { kNone, kEnv, kProfile, kWebIdentity, kContainer, kIMDS };
  CredSource source_ = CredSource::kNone;

  // Saved for RefreshToken():
  std::string role_arn_, web_identity_token_file_;
  std::string container_uri_;
  std::string imds_role_;

  SSL_CTX* ssl_ctx_ = nullptr;
  mutable folly::RWSpinLock lock_;
  AwsCredentials creds_;
};

}  // namespace cloud::aws
}  // namespace util
