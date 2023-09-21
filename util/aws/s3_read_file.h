// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <aws/s3/S3Client.h>

#include "io/file.h"
#include "io/io.h"

namespace util {
namespace aws {

constexpr size_t kDefaultChunkSize = 10 * (1 << 20);

class S3ReadFile final : public io::ReadonlyFile {
 public:
  S3ReadFile(const std::string& bucket, const std::string& key,
             std::shared_ptr<Aws::S3::S3Client> client, size_t chunk_size = kDefaultChunkSize);

  io::Result<size_t> Read(size_t offset, const iovec* v, uint32_t len) override;

  std::error_code Close() override;

  size_t Size() const override;

  int Handle() const override;

 private:
  std::error_code DownloadChunk();

  std::string NextByteRange() const;

  std::string bucket_;

  std::string key_;

  std::shared_ptr<Aws::S3::S3Client> client_;

  std::vector<uint8_t> buf_;

  size_t start_ = 0;

  size_t end_ = 0;

  size_t read_ = 0;

  size_t file_size_ = 0;
};

}  // namespace aws
}  // namespace util
