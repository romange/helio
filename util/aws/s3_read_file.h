// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#pragma once

#include <aws/s3/S3Client.h>
#include <aws/s3/model/GetObjectResult.h>

#include "io/file.h"
#include "io/io.h"

namespace util {
namespace aws {

// 8MB is the limit for the boost response parser.
constexpr size_t kDefaultChunkSize = 8 * (1 << 20);

// Reads files from S3.
//
// This downloads chunks of the file with the given chunk size, then Read
// consumes from the buffered chunk. Once a chunk has been read, it downloads
// another.
class S3ReadFile final : public io::ReadonlyFile {
 public:
  S3ReadFile(const std::string& bucket, const std::string& key,
             std::shared_ptr<Aws::S3::S3Client> client, size_t chunk_size = kDefaultChunkSize);

  io::Result<size_t> Read(size_t offset, const iovec* v, uint32_t len) override;

  std::error_code Close() override;

  size_t Size() const override;

  int Handle() const override;

 private:
  io::Result<size_t> Read(iovec v);

  std::error_code DownloadChunk();

  std::string NextByteRange() const;

  std::error_code ParseFileSize(const Aws::S3::Model::GetObjectResult& result);

  std::string bucket_;

  std::string key_;

  // Buffers the last downloaded chunk. Note may not fill buf_ if we're reading
  // the last chunk, so uses start_idx_ and end_idx_ to indicate the remaining
  // bytes to be consumed.
  std::vector<uint8_t> buf_;

  size_t start_idx_ = 0;

  size_t end_idx_ = 0;

  size_t file_read_ = 0;

  // Size of the target file to read. Set on first download.
  size_t file_size_ = 0;

  std::shared_ptr<Aws::S3::S3Client> client_;
};

}  // namespace aws
}  // namespace util
