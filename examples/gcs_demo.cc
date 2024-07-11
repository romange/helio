// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "base/flags.h"
#include "base/init.h"
#include "base/logging.h"
#include "util/cloud/gcp/gcs.h"
#include "util/fibers/pool.h"

using namespace std;
using namespace boost;
using namespace util;

using absl::GetFlag;

ABSL_FLAG(string, bucket, "", "");
ABSL_FLAG(string, access_token, "", "");
ABSL_FLAG(uint32_t, connect_ms, 2000, "");
ABSL_FLAG(bool, epoll, false, "Whether to use epoll instead of io_uring");


void Run(SSL_CTX* ctx) {
  fb2::ProactorBase* pb = fb2::ProactorBase::me();
  cloud::GCS gcs(ctx, pb);
  error_code ec = gcs.Connect(GetFlag(FLAGS_connect_ms));
  CHECK(!ec) << "Could not connect " << ec;
  auto res = gcs.ListBuckets();
  CHECK(res) << res.error();
  for (auto v : *res) {
    CONSOLE_INFO << v;
  }
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

  SSL_CTX* ctx =  util::http::TlsClient::CreateSslContext();
  pp->GetNextProactor()->Await([ctx] {
    Run(ctx);
  });
  util::http::TlsClient::FreeContext(ctx);


  return 0;
}
