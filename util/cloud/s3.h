// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <system_error>

#include "io/io.h"
#include "io/file.h"
#include "util/cloud/aws.h"
#include "util/http/http_client.h"

namespace util {
namespace cloud {

using ListBucketsResult = io::Result<std::vector<std::string>>;

// Inner result value is 'marker' to start next page from.
// Empty if no more pages left.
using ListObjectsResult = io::Result<std::string>;

// List all S3 buckets. Refresh AWS token if needed.
ListBucketsResult ListS3Buckets(AWS* aws, http::Client* http_client);

class S3Bucket {
 public:
  S3Bucket(const S3Bucket&) = delete;

  S3Bucket(const AWS& aws, std::string_view bucket);
  S3Bucket(const AWS& aws, std::string_view endpoint, std::string_view bucket);

  std::error_code Connect(uint32_t ms);

  //! Called with (size, key_name) pairs.
  using ListObjectCb = std::function<void(size_t, std::string_view)>;

  // Iterate over bucket objects for given path, starting from a marker (default none).
  // Up to max_keys entries are returned, possible maximum is 1000.
  // Returns key to start next query from is result is truncated.
  ListObjectsResult ListObjects(std::string_view path, ListObjectCb cb,
                                std::string_view marker = "", unsigned max_keys = 1000);

  // Iterate over all bucket objects for the given path.
  std::error_code ListAllObjects(std::string_view path, ListObjectCb cb);

  io::Result<io::ReadonlyFile*> OpenReadFile(std::string_view path,
      const io::ReadonlyFile::Options& opts = io::ReadonlyFile::Options{});

 private:
  std::string GetHost() const;
  std::error_code ConnectInternal();

  AWS aws_;
  std::string endpoint_;
  std::string bucket_;
  std::unique_ptr<http::Client> http_client_;
};

}  // namespace cloud

}  // namespace util
