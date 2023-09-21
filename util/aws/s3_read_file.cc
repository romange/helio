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

// TODO(andydunstall): Currently just using a very simple and inefficient
// read. Need to re-implement properly.

S3ReadFile::S3ReadFile(const std::string& bucket, const std::string& key,
                       std::shared_ptr<Aws::S3::S3Client> client, size_t chunk_size)
    : bucket_{bucket}, key_{key}, buf_(chunk_size), client_{client} {
}

io::Result<size_t> S3ReadFile::Read(size_t offset, const iovec* v, uint32_t len) {
  if (file_size_ > 0 && read_ == file_size_) {
    VLOG(2) << "aws: s3 read file: read complete; file_size=" << file_size_;
    return 0;
  }

  size_t read_n = 0;
  for (uint32_t i = 0; i != len; i++) {
    if (start_ == end_) {
      std::error_code ec = DownloadChunk();
      if (ec) {
        return nonstd::make_unexpected(ec);
      }
    }

    size_t n = v[i].iov_len;
    if (n > end_ - start_) {
      n = end_ - start_;
    }
    memcpy(v[i].iov_base, buf_.data() + start_, n);
    start_ += n;
    read_n += n;
    read_ += n;
  }

  VLOG(2) << "aws: s3 read file: read=" << read_n;

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

std::error_code S3ReadFile::DownloadChunk() {
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
    start_ = 0;
    end_ = outcome.GetResult().GetContentLength();

    if (file_size_ == 0) {
      if (outcome.GetResult().GetContentRange() == "") {
        file_size_ = outcome.GetResult().GetContentLength();
      } else {
        std::vector<std::string_view> parts =
            absl::StrSplit(outcome.GetResult().GetContentRange(), "/");
        absl::SimpleAtoi(parts[1], &file_size_);
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
  return absl::StrFormat("bytes=%d-%d", read_, read_ + buf_.size() - 1);
}

}  // namespace aws
}  // namespace util
