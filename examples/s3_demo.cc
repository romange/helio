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
#include "util/aws/s3_read_file.h"
#include "util/aws/s3_write_file.h"
#include "util/fibers/pool.h"

ABSL_FLAG(std::string, cmd, "list-buckets", "Command to run");
ABSL_FLAG(std::string, bucket, "", "S3 bucket name");
ABSL_FLAG(std::string, key, "", "S3 file key");
ABSL_FLAG(std::string, endpoint, "", "S3 endpoint");
ABSL_FLAG(size_t, upload_size, 100 << 20, "Upload file size");
ABSL_FLAG(size_t, chunk_size, 1024, "File chunk size");
ABSL_FLAG(bool, https, false, "Whether to use HTTPS");
ABSL_FLAG(bool, epoll, false, "Whether to use epoll instead of io_uring");

std::shared_ptr<Aws::S3::S3Client> OpenS3Client() {
  Aws::S3::S3ClientConfiguration s3_conf{};
  s3_conf.payloadSigningPolicy = Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::ForceNever;
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider =
      std::make_shared<util::aws::CredentialsProviderChain>();
  std::shared_ptr<Aws::S3::S3EndpointProviderBase> endpoint_provider =
      std::make_shared<util::aws::S3EndpointProvider>(absl::GetFlag(FLAGS_endpoint),
                                                      absl::GetFlag(FLAGS_https));
  return std::make_shared<Aws::S3::S3Client>(credentials_provider, endpoint_provider, s3_conf);
}

void ListBuckets() {
  std::shared_ptr<Aws::S3::S3Client> s3 = OpenS3Client();
  Aws::S3::Model::ListBucketsOutcome outcome = s3->ListBuckets();
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

  std::shared_ptr<Aws::S3::S3Client> s3 = OpenS3Client();

  std::string continuation_token;
  std::vector<std::string> keys;
  do {
    Aws::S3::Model::ListObjectsV2Request request;
    request.SetBucket(bucket);
    if (continuation_token != "") {
      request.SetContinuationToken(continuation_token);
    }

    Aws::S3::Model::ListObjectsV2Outcome outcome = s3->ListObjectsV2(request);
    if (outcome.IsSuccess()) {
      continuation_token = outcome.GetResult().GetNextContinuationToken();
      for (const auto& object : outcome.GetResult().GetContents()) {
        keys.push_back(object.GetKey());
      }
    } else {
      LOG(ERROR) << "failed to list objects: " << outcome.GetError().GetExceptionName();
      return;
    }
  } while (continuation_token != "");

  std::cout << "objects in " << bucket << ":" << std::endl;
  for (const std::string& key : keys) {
    std::cout << "* " << key << std::endl;
  }
}

void Upload(const std::string& bucket, const std::string& key, size_t upload_size,
            size_t chunk_size) {
  if (bucket == "") {
    LOG(ERROR) << "missing bucket name";
    return;
  }

  if (key == "") {
    LOG(ERROR) << "missing key name";
    return;
  }

  std::shared_ptr<Aws::S3::S3Client> s3 = OpenS3Client();
  io::Result<util::aws::S3WriteFile> file = util::aws::S3WriteFile::Open(bucket, key, s3);
  if (!file) {
    LOG(ERROR) << "failed to open s3 write file: " << file.error();
    return;
  }

  size_t chunks = upload_size / chunk_size;

  LOG(INFO) << "uploading s3 file; chunks=" << chunks << "; chunk_size=" << chunk_size;

  std::vector<uint8_t> buf(chunk_size, 0xff);
  for (size_t i = 0; i != chunks; i++) {
    std::error_code ec = file->Write(io::Bytes(buf.data(), buf.size()));
    if (ec) {
      LOG(ERROR) << "failed to write to s3: " << file.error();
      return;
    }
  }
  std::error_code ec = file->Close();
  if (ec) {
    LOG(ERROR) << "failed to close s3 write file: " << file.error();
    return;
  }
}

void Download(const std::string& bucket, const std::string& key) {
  if (bucket == "") {
    LOG(ERROR) << "missing bucket name";
    return;
  }

  if (key == "") {
    LOG(ERROR) << "missing key name";
    return;
  }

  std::shared_ptr<Aws::S3::S3Client> s3 = OpenS3Client();
  std::unique_ptr<io::ReadonlyFile> file = std::make_unique<util::aws::S3ReadFile>(bucket, key, s3);

  LOG(INFO) << "downloading s3 file";

  std::vector<uint8_t> buf(1024, 0);
  size_t read_n = 0;
  while (true) {
    io::Result<size_t> n = file->Read(read_n, io::MutableBytes(buf.data(), buf.size()));
    if (!n) {
      LOG(ERROR) << "failed to read from s3: " << n.error();
      return;
    }
    if (*n == 0) {
      LOG(INFO) << "finished download; read_n=" << read_n;
      return;
    }
    read_n += *n;
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
    } else if (cmd == "upload") {
      Upload(absl::GetFlag(FLAGS_bucket), absl::GetFlag(FLAGS_key),
             absl::GetFlag(FLAGS_upload_size), absl::GetFlag(FLAGS_chunk_size));
    } else if (cmd == "download") {
      Download(absl::GetFlag(FLAGS_bucket), absl::GetFlag(FLAGS_key));
    } else {
      LOG(ERROR) << "unknown command: " << cmd;
    }

    util::aws::Shutdown();
  });

  pp->Stop();
  return 0;
}
