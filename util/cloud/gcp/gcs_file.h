// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include "io/file.h"
#include "util/http/https_client_pool.h"
#include "util/cloud/gcp/gcp_creds_provider.h"

namespace util {

namespace cloud {

// File handle that writes to GCS.
//
// This uses multipart uploads, where it will buffer upto the configured part
// size before uploading.
class GcsWriteFile : public io::WriteFile {
 public:
  static constexpr size_t kDefaultPartSize = 1ULL << 23;  // 8MB.

  // Writes bytes to the GCS object. This will either buffer internally or
  // write a part to GCS.
  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) override;

  // Closes the object and completes the multipart upload. Therefore the object
  // will not be uploaded unless Close is called.
  std::error_code Close() override;

  static io::Result<GcsWriteFile*> Open(const std::string& bucket, const std::string& key,
                                       GCPCredsProvider* creds_provider,
                                       http::ClientPool* pool, size_t part_size = kDefaultPartSize);

 private:
  GcsWriteFile(const std::string& key,  const std::string& upload_id,
               size_t part_size, http::ClientPool* pool);

  std::string upload_id_;
};

}  // namespace cloud
}  // namespace util