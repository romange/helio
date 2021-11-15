// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/http/http_testing.h"
#include "base/logging.h"

namespace util {

using namespace boost;
using namespace std;


void HttpBaseTest::SetUp() {
  pool_.reset(new IoContextPool);
  pool_->Run();

  server_.reset(new AcceptServer(pool_.get()));
  port_ = server_->AddListener(0, &listener_);
  server_->Run();
}

void HttpBaseTest::TearDown() {
  LOG(INFO) << "HttpBaseTest::TearDown";

  server_.reset();
  VLOG(1) << "After server reset";
  pool_->Stop();
}

}  // namespace util
