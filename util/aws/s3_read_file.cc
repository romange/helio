// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/aws/s3_read_file.h"

#include <absl/strings/numbers.h>
#include <absl/strings/str_format.h>
#include <absl/strings/str_split.h>
#include <aws/s3/model/GetObjectRequest.h>

#include "base/logging.h"

namespace util {
namespace aws {

S3ReadFile::S3ReadFile(const std::string& bucket, const std::string& key,
                       std::shared_ptr<Aws::S3::S3Client> client, size_t chunk_size)
    : bucket_{bucket}, key_{key}, buf_(chunk_size), client_{client} {
}

io::Result<size_t> S3ReadFile::Read(size_t offset, const iovec* v, uint32_t len) {
  size_t read_n = 0;
  for (uint32_t i = 0; i != len; i++) {
    io::Result<size_t> n = Read(v[i]);
    if (!n) {
      return n;
    }
    read_n += *n;
  }

  VLOG(2) << "aws: s3 read file: read=" << read_n << "; file_read=" << file_read_
          << "; file_size=" << file_size_;

  if (read_n == 0) {
    VLOG(2) << "aws: s3 read file: read complete; file_size=" << file_size_;
  }

  return read_n;
}

std::error_code S3ReadFile::Close() {
  return std::error_code{};
}

size_t S3ReadFile::Size() const {
  return file_size_;
}

int S3ReadFile::Handle() const {
  return -1;
}

io::Result<size_t> S3ReadFile::Read(iovec v) {
  size_t read_n = 0;
  while (true) {
    // If we're read the whole file we're done.
    if (file_size_ > 0 && file_read_ == file_size_) {
      return read_n;
    }

    // Take as many bytes from the downloaded chunk as possible.
    size_t n = v.iov_len - read_n;
    if (n == 0) {
      return read_n;
    }
    if (n > end_idx_ - start_idx_) {
      n = end_idx_ - start_idx_;
    }
    uint8_t* b = reinterpret_cast<uint8_t*>(v.iov_base);
    memcpy(b + read_n, buf_.data() + start_idx_, n);
    start_idx_ += n;
    read_n += n;
    file_read_ += n;

    // If we don't have sufficient bytes to fill v and haven't read the whole
    // file, read again.
    if (read_n < v.iov_len) {
      std::error_code ec = DownloadChunk();
      if (ec) {
        return nonstd::make_unexpected(ec);
      }
    }
  }
}

std::error_code S3ReadFile::DownloadChunk() {
  // If we're read the whole file we're done.
  if (file_size_ > 0 && file_read_ == file_size_) {
    return std::error_code{};
  }

  Aws::S3::Model::GetObjectRequest request;
  request.SetBucket(bucket_);
  request.SetKey(key_);
  request.SetRange(NextByteRange());
  Aws::S3::Model::GetObjectOutcome outcome = client_->GetObject(request);
  if (outcome.IsSuccess()) {
    VLOG(2) << "aws: s3 read file: downloaded chunk: length="
            << outcome.GetResult().GetContentLength();

    outcome.GetResult().GetBody().read(reinterpret_cast<char*>(buf_.data()),
                                       outcome.GetResult().GetContentLength());
    start_idx_ = 0;
    end_idx_ = outcome.GetResult().GetContentLength();

    // If this is the first download, read the file size.
    if (file_size_ == 0) {
      std::error_code ec = ParseFileSize(outcome.GetResult());
      if (ec) {
        return ec;
      }
    }
    return std::error_code{};
  } else {
    LOG(ERROR) << "aws: s3 write file: failed to read chunk: "
               << outcome.GetError().GetExceptionName();
    return std::make_error_code(std::errc::io_error);
  }
}

std::string S3ReadFile::NextByteRange() const {
  return absl::StrFormat("bytes=%d-%d", file_read_, file_read_ + buf_.size() - 1);
}

std::error_code S3ReadFile::ParseFileSize(const Aws::S3::Model::GetObjectResult& result) {
  if (result.GetContentRange() == "") {
    file_size_ = result.GetContentLength();
  } else {
    std::vector<std::string_view> parts = absl::StrSplit(result.GetContentRange(), "/");
    if (parts.size() < 2) {
      LOG(ERROR) << "aws: s3 read file: failed to parse file size: range="
                 << result.GetContentRange();
      return std::make_error_code(std::errc::io_error);
    }
    if (!absl::SimpleAtoi(parts[1], &file_size_)) {
      LOG(ERROR) << "aws: s3 read file: failed to parse file size: range="
                 << result.GetContentRange();
      return std::make_error_code(std::errc::io_error);
    }
  }

  return std::error_code{};
}

}  // namespace aws
}  // namespace util
