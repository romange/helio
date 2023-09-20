// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>

#include "base/flags.h"
#include "base/init.h"
#include "base/logging.h"
#include "util/aws/aws.h"
#include "util/aws/credentials_provider_chain.h"
#include "util/aws/s3_endpoint_provider.h"
#include "util/fibers/pool.h"

ABSL_FLAG(std::string, cmd, "list-buckets", "Command to run");
ABSL_FLAG(std::string, bucket, "", "S3 bucket name");
ABSL_FLAG(std::string, endpoint, "", "S3 endpoint");
ABSL_FLAG(bool, epoll, false, "Whether to use epoll instead of io_uring");

Aws::S3::S3Client OpenS3Client() {
  Aws::S3::S3ClientConfiguration s3_conf{};
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider =
      Aws::MakeShared<util::aws::CredentialsProviderChain>("helio");
  std::shared_ptr<Aws::S3::S3EndpointProviderBase> endpoint_provider =
      Aws::MakeShared<util::aws::S3EndpointProvider>("helio", absl::GetFlag(FLAGS_endpoint));
  return Aws::S3::S3Client{credentials_provider, endpoint_provider, s3_conf};
}

void ListBuckets() {
  Aws::S3::S3Client s3 = OpenS3Client();
  Aws::S3::Model::ListBucketsOutcome outcome = s3.ListBuckets();
  if (outcome.IsSuccess()) {
    std::cout << "buckets:" << std::endl;
    for (const Aws::S3::Model::Bucket& bucket : outcome.GetResult().GetBuckets()) {
      std::cout << "* " << bucket.GetName() << std::endl;
    }
  } else {
    LOG(ERROR) << "failed to list buckets: " << outcome.GetError().GetExceptionName();
  }
}

void ListObjects(const std::string& bucket) {
  if (bucket == "") {
    LOG(ERROR) << "missing bucket name";
    return;
  }

  Aws::S3::S3Client s3 = OpenS3Client();
  Aws::S3::Model::ListObjectsV2Request request;
  request.SetBucket(bucket);

  Aws::S3::Model::ListObjectsV2Outcome outcome = s3.ListObjectsV2(request);
  if (outcome.IsSuccess()) {
    std::cout << "objects in " << bucket << ":" << std::endl;
    for (const auto& object : outcome.GetResult().GetContents()) {
      std::cout << "* " << object.GetKey() << std::endl;
    }
  } else {
    LOG(ERROR) << "failed to list objects: " << outcome.GetError().GetExceptionName();
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
    util::aws::Init();
    std::string cmd = absl::GetFlag(FLAGS_cmd);
    LOG(INFO) << "s3v2_demo; cmd=" << cmd;

    if (cmd == "list-buckets") {
      ListBuckets();
    } else if (cmd == "list-objects") {
      ListObjects(absl::GetFlag(FLAGS_bucket));
    }

    util::aws::Shutdown();
  });
}
