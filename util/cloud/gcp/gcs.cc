// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/gcp/gcs.h"

namespace util {
namespace cloud {

namespace  {
constexpr char kDomain[] = "www.googleapis.com";
}  // namespace


GCS::GCS(SSL_CTX* ssl_cntx, fb2::ProactorBase* pb) {
  client_.reset(new http::TlsClient(pb));
}

GCS::~GCS() {
}

std::error_code GCS::Connect(unsigned msec) {
  client_->set_connect_timeout_ms(msec);

  return client_->Connect(kDomain, "443");
}

auto GCS::ListBuckets() -> ListBucketResult {
  return {};
}

}  // namespace cloud
}  // namespace util