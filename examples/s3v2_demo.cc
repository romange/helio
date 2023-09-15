// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

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

  util::cloud::aws::Shutdown();
}
