// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/gtest.h"

#include "util/asio/accept_server.h"
#include "util/asio/io_context_pool.h"
#include "util/http/http_conn_handler.h"

namespace util {

class HttpBaseTest : public testing::Test {
 protected:
  void SetUp() override;

  void TearDown() override;

  std::unique_ptr<AcceptServer> server_;
  std::unique_ptr<IoContextPool> pool_;
  http::Listener<> listener_;
  uint16_t port_ = 0;
};

}  // namespace util

