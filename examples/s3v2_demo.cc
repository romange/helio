// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

#include <memory>

#include "base/init.h"
#include "base/logging.h"
#include "util/cloud/aws/aws.h"
#include "util/cloud/aws/s3/write_file.h"
#include "util/fibers/pool.h"

ABSL_FLAG(bool, epoll, false, "Whether to use epoll instead of io_uring");

void ListBuckets() {
  Aws::S3::S3ClientConfiguration s3_conf{};
  // Out HTTP client currently only supports HTTP.
  s3_conf.scheme = Aws::Http::Scheme::HTTP;
  // HTTP 1.1 is the latest version our HTTP client supports.
  s3_conf.version = Aws::Http::Version::HTTP_VERSION_1_1;

  // TODO(andydunstall) For now only support the environment provider.
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider =
      Aws::MakeShared<Aws::Auth::EnvironmentAWSCredentialsProvider>("helio");
  Aws::S3::S3Client s3{credentials_provider, Aws::MakeShared<Aws::S3::S3EndpointProvider>("helio"),
                       s3_conf};

  Aws::S3::Model::ListBucketsOutcome outcome = s3.ListBuckets();
  if (outcome.IsSuccess()) {
    std::cout << "buckets: " << std::endl;
    for (const Aws::S3::Model::Bucket& bucket : outcome.GetResult().GetBuckets()) {
      std::cout << bucket.GetName() << std::endl;
    }
  } else {
    LOG(ERROR) << "failed to list buckets: " << outcome.GetError().GetExceptionName();
  }
}

void ListObjects() {
  Aws::S3::S3ClientConfiguration s3_conf{};
  // Out HTTP client currently only supports HTTP.
  s3_conf.scheme = Aws::Http::Scheme::HTTP;
  // HTTP 1.1 is the latest version our HTTP client supports.
  s3_conf.version = Aws::Http::Version::HTTP_VERSION_1_1;

  // TODO(andydunstall) For now only support the environment provider.
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider =
      Aws::MakeShared<Aws::Auth::EnvironmentAWSCredentialsProvider>("helio");
  Aws::S3::S3Client s3{credentials_provider, Aws::MakeShared<Aws::S3::S3EndpointProvider>("helio"),
                       s3_conf};

  Aws::S3::Model::ListObjectsV2Request list_objects_request;
  list_objects_request.SetBucket("dev-andy-dfcloud-backups-us-east-1");

  Aws::S3::Model::ListObjectsV2Outcome outcome = s3.ListObjectsV2(list_objects_request);
  if (outcome.IsSuccess()) {
    std::cout << "objects: " << std::endl;
    for (const auto& object : outcome.GetResult().GetContents()) {
      std::cout << object.GetKey() << std::endl;
    }
  } else {
    LOG(ERROR) << "failed to list objects: " << outcome.GetError().GetExceptionName();
  }
}

void PutObject(const std::string& bucket, const std::string& key, const std::string& body) {
  Aws::S3::S3ClientConfiguration s3_conf{};
  // Out HTTP client currently only supports HTTP.
  s3_conf.scheme = Aws::Http::Scheme::HTTP;
  // HTTP 1.1 is the latest version our HTTP client supports.
  s3_conf.version = Aws::Http::Version::HTTP_VERSION_1_1;

  // TODO(andydunstall) For now only support the environment provider.
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider =
      Aws::MakeShared<Aws::Auth::EnvironmentAWSCredentialsProvider>("helio");
  Aws::S3::S3Client s3{credentials_provider, Aws::MakeShared<Aws::S3::S3EndpointProvider>("helio"),
                       s3_conf};

  Aws::S3::Model::PutObjectRequest put_object_request;
  put_object_request.SetBucket(bucket);
  put_object_request.SetKey(key);
  std::shared_ptr<Aws::IOStream> object_stream = Aws::MakeShared<Aws::StringStream>("helio");
  *object_stream << body;
  put_object_request.SetBody(object_stream);

  Aws::S3::Model::PutObjectOutcome outcome = s3.PutObject(put_object_request);
  if (outcome.IsSuccess()) {
    LOG(INFO) << "OK";
  } else {
    LOG(ERROR) << "failed to put object: " << outcome.GetError().GetExceptionName();
  }
}

void Upload() {
  Aws::S3::S3ClientConfiguration s3_conf{};
  // Out HTTP client currently only supports HTTP.
  s3_conf.scheme = Aws::Http::Scheme::HTTP;
  // HTTP 1.1 is the latest version our HTTP client supports.
  s3_conf.version = Aws::Http::Version::HTTP_VERSION_1_1;

  // TODO(andydunstall) For now only support the environment provider.
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider =
      Aws::MakeShared<Aws::Auth::EnvironmentAWSCredentialsProvider>("helio");
  Aws::S3::S3Client s3{credentials_provider, Aws::MakeShared<Aws::S3::S3EndpointProvider>("helio"),
                       s3_conf};

  io::Result<std::unique_ptr<io::WriteFile>> open_res =
      util::cloud::aws::s3::WriteFile::Open("dev-andy-dfcloud-backups-us-east-1", "myfile", &s3);
  if (open_res) {
    std::unique_ptr<io::WriteFile> file = *open_res;

    std::unique_ptr<uint8_t[]> buf(new uint8_t[1024]);
    memset(buf.get(), 'R', 1024);
    for (size_t i = 0; i < 15000; ++i) {
      std::error_code ec = file->Write(io::Bytes(buf.get(), 1024));
      CHECK(!ec);
    }
    std::error_code ec = file->Close();
    CHECK(!ec);
  } else {
    LOG(ERROR) << "failed to open write file: " << open_res.error();
  }
}

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  std::unique_ptr<util::ProactorPool> pp;

#ifdef __linux__
  if (absl::GetFlag(FLAGS_epoll)) {
    pp.reset(util::fb2::Pool::Epoll());
  } else {
    pp.reset(util::fb2::Pool::IOUring(256));
  }
#else
  pp.reset(util::fb2::Pool::Epoll());
#endif

  pp->Run();

  pp->GetNextProactor()->Await([&] {
    util::cloud::aws::Init();

    // ListBuckets();
    // ListObjects();
    // PutObject("dev-andy-dfcloud-backups-us-east-1", "myobject", "mybody");
    Upload();

    util::cloud::aws::Shutdown();
  });
}
