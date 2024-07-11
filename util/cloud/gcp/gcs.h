// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <io/io.h>

#include <vector>

#include "util/http/http_client.h"

typedef struct ssl_ctx_st SSL_CTX;

namespace util {

namespace fb2 {
class ProactorBase;
}  // namespace fb2

namespace cloud {

class GCS {
 public:
  using ListBucketResult = io::Result<std::vector<std::string>>;

  GCS(SSL_CTX* ssl_cntx, fb2::ProactorBase* pb);
  ~GCS();

  std::error_code Connect(unsigned msec);

  ListBucketResult ListBuckets();

 private:
  SSL_CTX* ssl_ctx_;
  std::unique_ptr<http::Client> client_;
};

}  // namespace cloud
}  // namespace util