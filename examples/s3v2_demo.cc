// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>

#include <memory>

#include "base/init.h"
#include "base/logging.h"
#include "util/cloud/aws/aws.h"
#include "util/fibers/pool.h"

ABSL_FLAG(bool, epoll, false, "Whether to use epoll instead of io_uring");

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

  util::cloud::aws::Init();

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

  util::cloud::aws::Shutdown();
}
