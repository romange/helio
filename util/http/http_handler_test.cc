// Copyright 2026, Helio authors. All rights reserved.
// See LICENSE for licensing terms.

#include "util/http/http_handler.h"

#include <absl/flags/flag.h>

#include <boost/beast/http/read.hpp>

#include "base/gtest.h"
#include "base/logging.h"
#include "util/accept_server.h"
#include "util/fibers/pool.h"

ABSL_FLAG(std::string, http_auth_test_value, "initial", "Flag used by HTTP authorization tests");

namespace util {
namespace {
namespace h2 = boost::beast::http;

class HttpHandlerTest : public testing::Test {
 protected:
  void SetUp() override {
    pool_.reset(fb2::Pool::Epoll(1));
    pool_->Run();
    server_ = std::make_unique<AcceptServer>(pool_.get());
    listener_ = new HttpListener<>;
    port_ = server_->AddListener(0, listener_);
    absl::SetFlag(&FLAGS_http_auth_test_value, "initial");
  }

  void TearDown() override {
    server_->Stop(true);
    pool_->Stop();
  }

  h2::response<h2::string_body> Get(std::string_view target, bool credentials = false) {
    h2::response<h2::string_body> response;
    pool_->at(0)->Await([&] {
      std::unique_ptr<FiberSocketBase> socket(pool_->at(0)->CreateSocket());
      auto address = boost::asio::ip::make_address("127.0.0.1");
      CHECK(!socket->Connect({address, port_}));
      AsioStreamAdapter<> stream(*socket);
      HttpListenerBase::RequestType request(h2::verb::get,
                                            boost::string_view(target.data(), target.size()), 11);
      if (credentials) {
        request.set(h2::field::authorization, "Basic ZGVmYXVsdDpzZWNyZXQ=");
      }
      h2::write(stream, request);
      boost::beast::flat_buffer buffer;
      h2::read(stream, buffer, response);
      std::ignore = socket->Close();
    });
    return response;
  }

  std::unique_ptr<ProactorPool> pool_;
  std::unique_ptr<AcceptServer> server_;
  HttpListener<>* listener_ = nullptr;
  uint16_t port_ = 0;
};

TEST_F(HttpHandlerTest, QueryPolicyPreventsFlagMutation) {
  listener_->SetAuthFunctor(
      [](std::string_view path, const http::QueryArgs& args, std::string_view, std::string_view) {
        if (path == "/flagz") {
          for (const auto& [key, value] : args) {
            if (key == "flag" && !value.empty()) {
              return false;
            }
          }
        }
        return true;
      });
  server_->Run();

  for (auto target :
       {"/flagz?flag=http_auth_test_value&value=changed", "/flagz?flag=http_auth_test_value",
        "/flagz?flag=&flag=http_auth_test_value&value=changed",
        "/flagz?flag=http_auth_test_value&value=%63hanged"}) {
    EXPECT_EQ(Get(target).result(), h2::status::unauthorized);
    EXPECT_EQ(absl::GetFlag(FLAGS_http_auth_test_value), "initial");
  }
  EXPECT_EQ(Get("/flagz").result(), h2::status::ok);
}

TEST_F(HttpHandlerTest, QueryAndCredentialsMatchHandler) {
  const http::QueryArgs* authorized_args = nullptr;
  listener_->SetAuthFunctor([&](std::string_view path, const http::QueryArgs& args,
                                std::string_view username, std::string_view password) {
    EXPECT_EQ(path, "/echo");
    EXPECT_EQ(username, "default");
    EXPECT_EQ(password, "secret");
    EXPECT_EQ(args, (http::QueryArgs{{"value", "one"}, {"value", "%74wo"}, {"empty", ""}}));
    authorized_args = &args;
    return true;
  });
  listener_->RegisterCb("/echo", [&](const http::QueryArgs& args, HttpContext* context) {
    EXPECT_EQ(&args, authorized_args);
    context->Invoke(http::MakeStringResponse());
  });
  server_->Run();
  EXPECT_EQ(Get("/echo?value=one&value=%74wo&empty", true).result(), h2::status::ok);
}

TEST_F(HttpHandlerTest, LegacyCallback) {
  listener_->SetAuthFunctor(
      [](std::string_view path, std::string_view username, std::string_view password) {
        return path == "/" && username == "default" && password == "secret";
      });
  server_->Run();
  EXPECT_EQ(Get("/").result(), h2::status::unauthorized);
  EXPECT_EQ(Get("/", true).result(), h2::status::ok);
}

}  // namespace
}  // namespace util
