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
ABSL_FLAG(uint32_t, connect_ms, 2000, "");
ABSL_FLAG(bool, epoll, false, "Whether to use epoll instead of io_uring");


void Run(SSL_CTX* ctx) {
  fb2::ProactorBase* pb = fb2::ProactorBase::me();
  cloud::GCPCredsProvider provider;
  unsigned connect_ms = GetFlag(FLAGS_connect_ms);
  error_code ec = provider.Init(connect_ms, pb);
  CHECK(!ec) << "Could not load credentials " << ec.message();

  cloud::GCS gcs(&provider, ctx, pb);
  ec = gcs.Connect(connect_ms);
  CHECK(!ec) << "Could not connect " << ec;
  auto cb = [](std::string_view bname) {
    CONSOLE_INFO << bname;
  };

  ec = gcs.ListBuckets(cb);
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

  SSL_CTX* ctx =  util::http::TlsClient::CreateSslContext();
  pp->GetNextProactor()->Await([ctx] {
    Run(ctx);
  });
  util::http::TlsClient::FreeContext(ctx);


  return 0;
}
