// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <aws/s3/S3Client.h>

#include "io/file.h"
#include "io/io.h"

namespace util {
namespace aws {

constexpr size_t kDefaultPartSize = 1ULL << 23;  // 8MB.

// File handle that writes to S3.
//
// This uses multipart uploads, where it will buffer upto the configured part
// size before uploading.
class S3WriteFile : public io::WriteFile {
 public:
  // Writes bytes to the S3 object. This will either buffer internally or
  // write a part to S3.
  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) override;

  // Closes the object and completes the multipart upload. Therefore the object
  // will not be uploaded unless Close is called.
  std::error_code Close() override;

  static io::Result<S3WriteFile> Open(const std::string& bucket, const std::string& key,
                                      std::shared_ptr<Aws::S3::S3Client> client,
                                      size_t part_size = kDefaultPartSize);

 private:
  S3WriteFile(const std::string& bucket, const std::string& key, const std::string& upload_id,
              std::shared_ptr<Aws::S3::S3Client> client, size_t part_size = kDefaultPartSize);

  // Uploads the data buffered in buf_. Note this must not be called until
  // there are at least 5MB bytes in buf_, unless it is the last upload.
  std::error_code Flush();

  std::string bucket_;

  std::string key_;

  std::string upload_id_;

  // Etags of the uploaded parts.
  std::vector<std::string> parts_;

  // A buffer containing the pending bytes waiting to be uploaded. Only offset_
  // bytes have been written.
  std::vector<uint8_t> buf_;

  // Offset of the consumed bytes in buf_.
  size_t offset_ = 0;

  std::shared_ptr<Aws::S3::S3Client> client_;
};

}  // namespace aws
}  // namespace util
