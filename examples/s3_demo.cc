// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#ifdef WITH_AWS
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/PutObjectRequest.h>
#endif

#include "base/flags.h"
#include "base/init.h"
#include "base/logging.h"
#ifdef WITH_AWS
#include "util/aws/aws.h"
#include "util/aws/credentials_provider_chain.h"
#include "util/aws/s3_endpoint_provider.h"
#include "util/aws/s3_read_file.h"
#include "util/aws/s3_write_file.h"
#endif
#include <absl/cleanup/cleanup.h>

#include "util/cloud/aws/aws_creds_provider.h"
#include "util/cloud/aws/s3_storage.h"
#include "util/fibers/pool.h"
#include "util/http/http_client.h"

// Returns nullptr (plain HTTP) when AWS_S3_ENDPOINT is an http:// URL, otherwise a TLS context.
SSL_CTX* MakeS3SslCtx() {
  const char* ep = getenv("AWS_S3_ENDPOINT");
  if (ep && strncmp(ep, "http://", 7) == 0)
    return nullptr;
  return util::http::TlsClient::CreateSslContext();
}

ABSL_FLAG(std::string, cmd, "list-buckets", "Command to run");
ABSL_FLAG(std::string, bucket, "", "S3 bucket name");
ABSL_FLAG(std::string, key, "", "S3 file key");
ABSL_FLAG(std::string, prefix, "", "Object key prefix for list-objects");
ABSL_FLAG(std::string, endpoint, "", "S3 endpoint");
ABSL_FLAG(size_t, upload_size, 100 << 20, "Upload file size");
ABSL_FLAG(size_t, chunk_size, 1024, "File chunk size");
ABSL_FLAG(bool, https, false, "Whether to use HTTPS");
ABSL_FLAG(bool, epoll, false, "Whether to use epoll instead of io_uring");
ABSL_FLAG(unsigned, connect_ms, 2000, "AWS connection timeout in milliseconds");
ABSL_FLAG(unsigned, list_max_results, 1000, "Max keys per List page");

#ifdef WITH_AWS
std::shared_ptr<Aws::S3::S3Client> OpenS3Client() {
  Aws::S3::S3ClientConfiguration s3_conf{};
  s3_conf.checksumConfig.responseChecksumValidation =
      Aws::Client::ResponseChecksumValidation::WHEN_REQUIRED;

  s3_conf.payloadSigningPolicy = Aws::Client::AWSAuthV4Signer::PayloadSigningPolicy::Never;
  std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials_provider =
      std::make_shared<util::aws::CredentialsProviderChain>();
  std::shared_ptr<Aws::S3::S3EndpointProviderBase> endpoint_provider =
      std::make_shared<util::aws::S3EndpointProvider>(absl::GetFlag(FLAGS_endpoint),
                                                      absl::GetFlag(FLAGS_https));
  return std::make_shared<Aws::S3::S3Client>(credentials_provider, endpoint_provider, s3_conf);
}
#endif  // WITH_AWS

bool InitAws(util::cloud::aws::AwsCredsProvider* creds, SSL_CTX** ssl_ctx) {
  std::error_code ec = creds->Init(absl::GetFlag(FLAGS_connect_ms));
  if (ec) {
    LOG(ERROR) << "failed to init credentials: " << ec.message();
    return false;
  }
  *ssl_ctx = MakeS3SslCtx();
  return true;
}

void ListBuckets() {
  util::cloud::aws::AwsCredsProvider creds_provider;
  SSL_CTX* ssl_ctx;
  if (!InitAws(&creds_provider, &ssl_ctx)) return;
  absl::Cleanup free_ctx([ssl_ctx] { if (ssl_ctx) util::http::TlsClient::FreeContext(ssl_ctx); });

  util::cloud::aws::S3Storage storage(&creds_provider, ssl_ctx,
                                      util::fb2::ProactorBase::me());
  auto ec = storage.ListBuckets([](std::string_view name) {
    std::cout << "* " << name << std::endl;
  });
  if (ec) {
    LOG(ERROR) << "failed to list buckets: " << ec.message();
  }
}

void ListObjects(const std::string& bucket, const std::string& prefix) {
  if (bucket.empty()) {
    LOG(ERROR) << "missing bucket name";
    return;
  }

  util::cloud::aws::AwsCredsProvider creds_provider;
  SSL_CTX* ssl_ctx;
  if (!InitAws(&creds_provider, &ssl_ctx)) return;
  absl::Cleanup free_ctx([ssl_ctx] { if (ssl_ctx) util::http::TlsClient::FreeContext(ssl_ctx); });

  bool recursive = prefix.empty();
  util::cloud::aws::S3Storage storage(&creds_provider, ssl_ctx,
                                      util::fb2::ProactorBase::me());
  auto ec = storage.List(bucket, prefix, recursive,
                         absl::GetFlag(FLAGS_list_max_results),
                         [](const util::cloud::aws::S3Storage::ListItem& item) {
                           std::cout << "* " << item.key << std::endl;
                         });
  if (ec) {
    LOG(ERROR) << "failed to list objects: " << ec.message();
  }
}

void GetObject(const std::string& bucket, const std::string& key) {
  if (bucket.empty() || key.empty()) {
    LOG(ERROR) << "missing bucket or key";
    return;
  }

  util::cloud::aws::AwsCredsProvider creds_provider;
  SSL_CTX* ssl_ctx;
  if (!InitAws(&creds_provider, &ssl_ctx)) return;
  absl::Cleanup free_ctx([ssl_ctx] { if (ssl_ctx) util::http::TlsClient::FreeContext(ssl_ctx); });

  util::cloud::aws::ReadFileOptions opts{&creds_provider, ssl_ctx};
  auto res = util::cloud::aws::OpenReadFile(bucket, key, opts);
  if (!res) {
    LOG(ERROR) << "failed to open s3 file: " << res.error().message();
    return;
  }

  std::unique_ptr<io::ReadonlyFile> file(*res);
  LOG(INFO) << "get-object: size=" << file->Size();

  std::vector<uint8_t> buf(1 << 16);
  size_t read_n = 0;
  while (read_n < file->Size()) {
    iovec iov{buf.data(), std::min(buf.size(), file->Size() - read_n)};
    io::Result<size_t> n = file->Read(read_n, &iov, 1);
    if (!n) {
      LOG(ERROR) << "get-object read error: " << n.error().message();
      break;
    }
    read_n += *n;
  }
  LOG(INFO) << "get-object done; bytes=" << read_n;
}

#ifdef WITH_AWS
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
      LOG(ERROR) << "failed to write to s3: " << ec.message();
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
#endif  // WITH_AWS

void PutObject(const std::string& bucket, const std::string& key) {
  if (bucket.empty() || key.empty()) {
    LOG(ERROR) << "missing bucket or key";
    return;
  }

  util::cloud::aws::AwsCredsProvider creds_provider;
  SSL_CTX* ssl_ctx;
  if (!InitAws(&creds_provider, &ssl_ctx)) return;
  absl::Cleanup free_ctx([ssl_ctx] { if (ssl_ctx) util::http::TlsClient::FreeContext(ssl_ctx); });

  util::cloud::aws::WriteFileOptions opts{&creds_provider, ssl_ctx};
  auto res = util::cloud::aws::OpenWriteFile(bucket, key, opts);
  if (!res) {
    LOG(ERROR) << "failed to open s3 write file: " << res.error().message();
    return;
  }

  std::unique_ptr<io::WriteFile> file(*res);
  size_t total = absl::GetFlag(FLAGS_upload_size);
  std::vector<uint8_t> buf(absl::GetFlag(FLAGS_chunk_size), 0xab);

  size_t written = 0;
  while (written < total) {
    size_t chunk = std::min(buf.size(), total - written);
    iovec iov{buf.data(), chunk};
    auto n = file->WriteSome(&iov, 1);
    if (!n) {
      LOG(ERROR) << "put-object write error: " << n.error().message();
      return;
    }
    written += *n;
  }
  if (auto ec = file->Close(); ec) {
    LOG(ERROR) << "put-object close error: " << ec.message();
    return;
  }
  LOG(INFO) << "put-object done; bytes=" << written;
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
    std::string cmd = absl::GetFlag(FLAGS_cmd);
    LOG(INFO) << "s3_demo; cmd=" << cmd;

    if (cmd == "list-buckets") {
      ListBuckets();
    } else if (cmd == "list-objects") {
      ListObjects(absl::GetFlag(FLAGS_bucket), absl::GetFlag(FLAGS_prefix));
    } else if (cmd == "get-object") {
      GetObject(absl::GetFlag(FLAGS_bucket), absl::GetFlag(FLAGS_key));
    } else if (cmd == "put-object") {
      PutObject(absl::GetFlag(FLAGS_bucket), absl::GetFlag(FLAGS_key));
#ifdef WITH_AWS
    } else if (cmd == "upload" || cmd == "download") {
      util::aws::Init();
      if (cmd == "upload") {
        Upload(absl::GetFlag(FLAGS_bucket), absl::GetFlag(FLAGS_key),
               absl::GetFlag(FLAGS_upload_size), absl::GetFlag(FLAGS_chunk_size));
      } else {
        Download(absl::GetFlag(FLAGS_bucket), absl::GetFlag(FLAGS_key));
      }
      util::aws::Shutdown();
#endif  // WITH_AWS
    } else {
      LOG(ERROR) << "unknown command: " << cmd;
    }
  });

  pp->Stop();
  return 0;
}
