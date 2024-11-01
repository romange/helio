// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include <absl/strings/str_cat.h>

#include "base/flags.h"
#include "base/init.h"
#include "base/logging.h"
#include "io/file_util.h"
#include "util/cloud/azure/creds_provider.h"
#include "util/cloud/gcp/gcs.h"
#include "util/cloud/gcp/gcs_file.h"
#include "util/fibers/pool.h"

using namespace std;
using namespace util;

using absl::GetFlag;

ABSL_FLAG(string, bucket, "", "");
ABSL_FLAG(string, prefix, "", "");
ABSL_FLAG(uint32_t, write, 0, "If write > 0, then write this many files to GCS");
ABSL_FLAG(uint32_t, read, 0, "If read > 0, then read this many files from GCS");
ABSL_FLAG(uint32_t, connect_ms, 2000, "");
ABSL_FLAG(bool, epoll, false, "Whether to use epoll instead of io_uring");
ABSL_FLAG(bool, azure, false, "Whether to use Azure instead of GCS");

static io::Result<string> ReadToString(io::ReadonlyFile* file) {
  string res_str;
  while (true) {
    constexpr size_t kBufSize = 1U << 20;
    size_t offset = res_str.size();
    res_str.resize(offset + kBufSize);
    io::MutableBytes mb{reinterpret_cast<uint8_t*>(res_str.data() + offset), kBufSize};
    io::Result<size_t> res = file->Read(offset, mb);
    if (!res) {
      return nonstd::make_unexpected(res.error());
    }
    size_t read_sz = *res;
    if (read_sz < kBufSize) {
      res_str.resize(offset + read_sz);
      break;
    }
  }
  return res_str;
}

void Run(SSL_CTX* ctx) {
  fb2::ProactorBase* pb = fb2::ProactorBase::me();
  cloud::GCPCredsProvider provider;
  unsigned connect_ms = GetFlag(FLAGS_connect_ms);
  error_code ec = provider.Init(connect_ms, pb);
  CHECK(!ec) << "Could not load credentials " << ec.message();

  cloud::GCS gcs(&provider, ctx, pb);

  string prefix = GetFlag(FLAGS_prefix);
  string bucket = GetFlag(FLAGS_bucket);

  if (!bucket.empty()) {
    auto conn_pool = gcs.GetConnectionPool();
    if (GetFlag(FLAGS_write) > 0) {
      auto src = io::ReadFileToString("/proc/self/exe");
      CHECK(src);
      LOG(INFO) << "Writing " << src->size() << " bytes to " << prefix;
      for (unsigned i = 0; i < GetFlag(FLAGS_write); ++i) {
        string dest_key = absl::StrCat(prefix, "_", i);
        cloud::GcsWriteFileOptions opts;
        opts.creds_provider = &provider;
        opts.pool = conn_pool;
        io::Result<io::WriteFile*> dest_res = cloud::OpenWriteGcsFile(bucket, dest_key, opts);
        CHECK(dest_res) << "Could not open " << dest_key << " " << dest_res.error().message();
        unique_ptr<io::WriteFile> dest(*dest_res);
        error_code ec = dest->Write(*src);
        CHECK(!ec);
        ec = dest->Close();
        CHECK(!ec);
        CONSOLE_INFO << "Written " << dest_key;
      }
    } else if (GetFlag(FLAGS_read) > 0) {
      for (unsigned i = 0; i < GetFlag(FLAGS_read); ++i) {
        string dest_key = prefix;
        cloud::GcsReadFileOptions opts;
        opts.creds_provider = &provider;
        opts.pool = conn_pool;
        io::Result<io::ReadonlyFile*> dest_res = cloud::OpenReadGcsFile(bucket, dest_key, opts);
        CHECK(dest_res) << "Could not open " << dest_key << " " << dest_res.error().message();
        unique_ptr<io::ReadonlyFile> dest(*dest_res);
        io::Result<string> dest_str = ReadToString(dest.get());
        if (dest_str) {
          CONSOLE_INFO << "Read " << dest_str->size() << " bytes from " << dest_key;
        } else {
          LOG(ERROR) << "Error reading " << dest_key << " " << dest_str.error().message();
        }
      }
    } else {
      auto cb = [](cloud::GCS::ObjectItem item) {
        cout << "Object: " << item.key << ", size: " << item.size << endl;
      };
      ec = gcs.List(GetFlag(FLAGS_bucket), prefix, false, cb);
    }
  } else {
    auto cb = [](std::string_view bname) { CONSOLE_INFO << bname; };

    ec = gcs.ListBuckets(cb);
  }
  CHECK(!ec) << ec.message();
}

void RunAzure(SSL_CTX* ctx) {
  util::cloud::azure::CredsProvider provider;
  util::cloud::azure::Storage storage(&provider);

  error_code ec = provider.Init();
  CHECK(!ec) << "Could not load credentials " << ec.message();
  auto bucket = GetFlag(FLAGS_bucket);
  if (bucket.empty()) {
    ec = storage.ListContainers([](std::string_view item) { CONSOLE_INFO << item << endl; });
    CHECK(!ec) << ec.message();
    return;
  }

  storage.List(bucket, [](const util::cloud::azure::Storage::ObjectItem& item) {
    CONSOLE_INFO << "Object: " << item << endl;
  });
}

int main(int argc, char** argv) {
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

  SSL_CTX* ctx = util::http::TlsClient::CreateSslContext();
  bool azure = GetFlag(FLAGS_azure);
  if (azure) {
    pp->GetNextProactor()->Await([&] { RunAzure(ctx); });
  } else {
    pp->GetNextProactor()->Await([ctx] { Run(ctx); });
  }
  util::http::TlsClient::FreeContext(ctx);

  return 0;
}
