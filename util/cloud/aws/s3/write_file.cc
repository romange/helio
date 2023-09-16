#include "util/cloud/aws/s3/write_file.h"

#include <aws/s3/model/CompleteMultipartUploadRequest.h>
#include <aws/s3/model/CompletedMultipartUpload.h>
#include <aws/s3/model/CreateMultipartUploadRequest.h>
#include <aws/s3/model/UploadPartRequest.h>

#include "base/logging.h"

namespace util {
namespace cloud {
namespace aws {
namespace s3 {

constexpr size_t kPartSize = 10 * (1 << 20);

WriteFile::WriteFile(const std::string& bucket, const std::string& key,
                     const std::string& upload_id, Aws::S3::S3Client* client)
    : io::WriteFile{""}, bucket_{bucket}, key_{key}, upload_id_{upload_id},
      buf_(kPartSize), client_{client} {
}

io::Result<size_t> WriteFile::WriteSome(const iovec* v, uint32_t len) {
  size_t total = 0;
  for (size_t i = 0; i < len; ++i) {
    const char* buf = reinterpret_cast<const char*>(v[i].iov_base);
    const size_t len = v[i].iov_len;

    size_t written = 0;
    while (written < len) {
      size_t n = len - written;
      if (n > buf_.size() - offset_) {
        n = buf_.size() - offset_;
      }
      memcpy(buf_.data() + offset_, buf + written, n);
      written += n;
      total += n;
      offset_ += n;

      if (buf_.size() == offset_) {
        std::error_code ec = Upload();
        if (ec) {
          return nonstd::make_unexpected(ec);
        }
      }
    }
  }

  return total;
}

std::error_code WriteFile::Close() {
  std::error_code ec = Upload();
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
    LOG(INFO) << "completed multipart upload";
  } else {
    LOG(ERROR) << "failed to complete multipart upload: " << outcome.GetError().GetExceptionName();
    return std::make_error_code(std::errc::io_error);
  }

  return std::error_code{};
}

std::error_code WriteFile::Upload() {
  if (offset_ == 0) {
    return std::error_code{};
  }

  Aws::S3::Model::UploadPartRequest request;
  request.SetBucket(bucket_);
  request.SetKey(key_);
  // TODO(andydunstall)
  request.SetPartNumber(parts_.size() + 1);
  request.SetUploadId(upload_id_);

  // TODO(andydunstall): Look at avoiding this copy
  std::shared_ptr<Aws::IOStream> object_stream = Aws::MakeShared<Aws::StringStream>("helio");
  object_stream->write((const char*)buf_.data(), offset_);
  request.SetBody(object_stream);

  Aws::S3::Model::UploadPartOutcome outcome = client_->UploadPart(request);
  if (outcome.IsSuccess()) {
    LOG(INFO) << "upload part; part_number=" << parts_.size() + 1;

    parts_.push_back(outcome.GetResult().GetETag());
    offset_ = 0;
    return std::error_code{};
  } else {
    // TODO(andydunstall): Return error
    LOG(ERROR) << "failed to upload part: " << outcome.GetError().GetExceptionName();
    return std::make_error_code(std::errc::io_error);
  }
}

io::Result<std::unique_ptr<io::WriteFile>> WriteFile::Open(const std::string& bucket,
                                                           const std::string& key,
                                                           Aws::S3::S3Client* client) {
  Aws::S3::Model::CreateMultipartUploadRequest request;
  request.SetBucket(bucket);
  request.SetKey(key);
  Aws::Utils::Outcome<Aws::S3::Model::CreateMultipartUploadResult, Aws::S3::S3Error> outcome =
      client->CreateMultipartUpload(request);
  if (outcome.IsSuccess()) {
    VLOG(2) << "created multipart upload; upload_id=" << outcome.GetResult().GetUploadId();
    return std::make_unique<WriteFile>(bucket, key, outcome.GetResult().GetUploadId(), client);
  } else {
    LOG(ERROR) << "failed to create multipart upload: " << outcome.GetError().GetExceptionName();
    return nonstd::make_unexpected(std::make_error_code(std::errc::io_error));
  }
}

}  // namespace s3
}  // namespace aws
}  // namespace cloud
}  // namespace util
