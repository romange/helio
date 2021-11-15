// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/read.hpp>

#include <rapidjson/document.h>
#include <rapidjson/error/en.h>

#include "base/gtest.h"
#include "base/logging.h"

#include "util/asio/accept_server.h"
#include "util/asio/asio_utils.h"
#include "util/asio/io_context_pool.h"
#include "util/http/http_client.h"
#include "util/http/http_testing.h"
#include "util/http/beast_rj_utils.h"

namespace util {
namespace http {
using namespace boost;
using namespace std;
namespace rj = rapidjson;

class HttpTest : public HttpBaseTest {
 protected:
};

using namespace asio::ip;
namespace h2 = beast::http;

TEST_F(HttpTest, Client) {
  IoContext& io_context = pool_->GetNextContext();

  Client client(&io_context);

  system::error_code ec = client.Connect("localhost", std::to_string(port_));
  ASSERT_FALSE(ec) << ec << " " << ec.message();

  ASSERT_TRUE(client.IsConnected());

  Client::Response res;
  ec = client.Send(h2::verb::get, "/", &res);
  ASSERT_FALSE(ec) << ec;

  // Write the message to standard out
  VLOG(1) << res;

  server_->Stop();
  server_->Wait();  // To wait till server stops.

  ec = client.Send(h2::verb::get, "/", &res);
  EXPECT_TRUE(ec);
  this_thread::sleep_for(10ms);

  EXPECT_FALSE(client.IsConnected());
}


void AddToMB(const char* str, beast::multi_buffer* dest) {
  size_t sz = strlen(str);
  size_t req_sz = sz * 2 + 10;
  auto seq = dest->prepare(req_sz);
  auto it = seq.begin();
  CHECK_EQ(req_sz, (*it).size());

  memcpy((*it).data(), str, sz);
  dest->commit(sz);
}

TEST_F(HttpTest, JsonParse) {
  const char kPart1[] = R"({ "key1" : "val1", "key2)";
  const char kPart2[] = R"(" : "val2", "key)";
  const char kPart3[] = R"(3" : "val3" })";
  beast::multi_buffer mb;
  AddToMB(kPart1, &mb);
  AddToMB(kPart2, &mb);
  AddToMB(kPart3, &mb);

  RjBufSequenceStream is(mb.data());

  rj::Document doc;
  doc.ParseStream<rj::kParseDefaultFlags>(is);
  ASSERT_FALSE(doc.HasParseError()) << rj::GetParseError_En(doc.GetParseError());
  EXPECT_STREQ("val1", doc["key1"].GetString());
  EXPECT_STREQ("val2", doc["key2"].GetString());
  EXPECT_STREQ("val3", doc["key3"].GetString());
}

}  // namespace http
}  // namespace util

