#pragma once

#include <aws/s3/S3Client.h>

#include <memory>
#include <vector>

#include "io/file.h"
#include "io/io.h"

namespace util {
namespace cloud {
namespace aws {
namespace s3 {

// Writes the given file to an object in S3.
//
// This multipart uploads, so we upload 8MB of the object at a time. This
// avoids having to buffer the whole object and quickly recovers from
// networking issues as we only have to retry a small portion of the object.
class WriteFile : public io::WriteFile {
 public:
  WriteFile(const std::string& bucket, const std::string& key, const std::string& upload_id,
            Aws::S3::S3Client* client);

  // Writes bytes to the S3 object. This will either buffer internally or
  // write a part to S3.
  io::Result<size_t> WriteSome(const iovec* v, uint32_t len) override;

  // Closes the object and completes the multipart upload. Therefore the object
  // will not be uploaded unless Close is called.
  std::error_code Close() override;

  static io::Result<std::unique_ptr<io::WriteFile>> Open(const std::string& bucket,
                                                         const std::string& key,
                                                         Aws::S3::S3Client* client);

 private:
  std::error_code Upload();

  std::string bucket_;

  std::string key_;

  std::string upload_id_;

  // Etags of the uploaded parts.
  std::vector<std::string> parts_;

  size_t offset_ = 0;

  std::vector<uint8_t> buf_;

  Aws::S3::S3Client* client_;
};

}  // namespace s3
}  // namespace aws
}  // namespace cloud
}  // namespace util
