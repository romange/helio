// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/aws/s3_write_file.h"

#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include "base/logging.h"

namespace util {
namespace aws {

io::Result<size_t> S3WriteFile::WriteSome(const iovec* v, uint32_t len) {
  // Fill the pending buffer until we reach the part size, then flush and
  // keep writing.
  size_t total = 0;
  for (size_t i = 0; i < len; ++i) {
    const uint8_t* buf = reinterpret_cast<const uint8_t*>(v[i].iov_base);
    const size_t len = v[i].iov_len;

    size_t written = 0;
    while (written < len) {
      size_t n = len - written;
      if (n > buf_.size() - offset_) {
        // Limit to avoid exceeding the buffer size.
        n = buf_.size() - offset_;
      }
      memcpy(buf_.data() + offset_, buf + written, n);
      written += n;
      total += n;
      offset_ += n;

      if (buf_.size() == offset_) {
        std::error_code ec = Flush();
        if (ec) {
          return nonstd::make_unexpected(ec);
        }
      }
    }
  }

  return total;
}

// Closes the object and completes the multipart upload. Therefore the object
// will not be uploaded unless Close is called.
std::error_code S3WriteFile::Close() {
  std::error_code ec = Flush();
  if (ec) {
    return ec;
  }

  Aws::S3::Model::CompletedMultipartUpload completed_upload;
  for (size_t i = 0; i != parts_.size(); i++) {
    Aws::S3::Model::CompletedPart part;
    part.SetPartNumber(i + 1);
    part.SetETag(parts_[i]);
    completed_upload.AddParts(part);
  }

  Aws::S3::Model::CompleteMultipartUploadRequest request;
  request.SetBucket(bucket_);
  request.SetKey(key_);
  request.SetUploadId(upload_id_);
  request.SetMultipartUpload(completed_upload);

  Aws::S3::Model::CompleteMultipartUploadOutcome outcome =
      client_->CompleteMultipartUpload(request);
  if (outcome.IsSuccess()) {
    VLOG(2) << "aws: s3 write file: completed multipart upload; parts=" << parts_.size();
  } else {
    LOG(ERROR) << "aws: s3 write file: failed to complete multipart upload: "
               << outcome.GetError().GetExceptionName();
    return std::make_error_code(std::errc::io_error);
  }

  return std::error_code{};
}

io::Result<S3WriteFile> S3WriteFile::Open(const std::string& bucket, const std::string& key,
                                          std::shared_ptr<Aws::S3::S3Client> client,
                                          size_t part_size) {
  Aws::S3::Model::CreateMultipartUploadRequest request;
  request.SetBucket(bucket);
  request.SetKey(key);
  Aws::Utils::Outcome<Aws::S3::Model::CreateMultipartUploadResult, Aws::S3::S3Error> outcome =
      client->CreateMultipartUpload(request);
  if (outcome.IsSuccess()) {
    VLOG(2) << "aws: s3 write file: created multipart upload; upload_id="
            << outcome.GetResult().GetUploadId();
    return S3WriteFile{bucket, key, outcome.GetResult().GetUploadId(), client, part_size};
  } else {
    LOG(ERROR) << "aws: s3 write file: failed to create multipart upload: "
               << outcome.GetError().GetExceptionName();
    return nonstd::make_unexpected(std::make_error_code(std::errc::io_error));
  }
}

S3WriteFile::S3WriteFile(const std::string& bucket, const std::string& key,
                         const std::string& upload_id, std::shared_ptr<Aws::S3::S3Client> client,
                         size_t part_size)
    : io::WriteFile{""}, bucket_{bucket}, key_{key}, upload_id_{upload_id},
      buf_(part_size), client_{client} {
}

std::error_code S3WriteFile::Flush() {
  if (offset_ == 0) {
    return std::error_code{};
  }

  Aws::S3::Model::UploadPartRequest request;
  request.SetBucket(bucket_);
  request.SetKey(key_);
  request.SetPartNumber(parts_.size() + 1);
  request.SetUploadId(upload_id_);

  // TODO(andydunstall): Look at avoiding this copy. We're copying to an IO
  // stream, then back to a buffer in HTTP client.
  std::shared_ptr<Aws::IOStream> object_stream = Aws::MakeShared<Aws::StringStream>("helio");
  object_stream->write((const char*)buf_.data(), offset_);
  request.SetBody(object_stream);

  Aws::S3::Model::UploadPartOutcome outcome = client_->UploadPart(request);
  if (outcome.IsSuccess()) {
    VLOG(2) << "aws: s3 write file: upload part; part_number=" << parts_.size() + 1;

    parts_.push_back(outcome.GetResult().GetETag());
    offset_ = 0;
    return std::error_code{};
  } else {
    LOG(ERROR) << "aws: s3 write file: failed to upload part: "
               << outcome.GetError().GetExceptionName();
    return std::make_error_code(std::errc::io_error);
  }
  return std::error_code{};
}

}  // namespace aws
}  // namespace util
