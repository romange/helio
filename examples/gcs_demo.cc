// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include <absl/strings/str_cat.h>
#include "base/flags.h"
#include "base/init.h"
#include "base/logging.h"
#include "io/file_util.h"
#include "util/cloud/gcp/gcs.h"
#include "util/cloud/gcp/gcs_file.h"
#include "util/fibers/pool.h"

using namespace std;
using namespace util;

using absl::GetFlag;

ABSL_FLAG(string, bucket, "", "");
ABSL_FLAG(string, prefix, "", "");
ABSL_FLAG(uint32_t, write, 0, "");
ABSL_FLAG(uint32_t, connect_ms, 2000, "");
ABSL_FLAG(bool, epoll, false, "Whether to use epoll instead of io_uring");

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
      for (unsigned i = 0; i < GetFlag(FLAGS_write); ++i) {
        string dest_key = absl::StrCat(prefix, "_", i);
        io::Result<io::WriteFile*> dest_res =
            cloud::OpenWriteGcsFile(bucket, dest_key, &provider, conn_pool);
        CHECK(dest_res) << "Could not open " << dest_key << " " << dest_res.error().message();
        unique_ptr<io::WriteFile> dest(*dest_res);
        error_code ec = dest->Write(*src);
        CHECK(!ec);
        ec = dest->Close();
        CHECK(!ec);
        CONSOLE_INFO << "Written " << dest_key;
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
  pp->GetNextProactor()->Await([ctx] { Run(ctx); });
  util::http::TlsClient::FreeContext(ctx);

  return 0;
}
