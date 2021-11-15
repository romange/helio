// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/http/https_client_pool.h"

#include "base/logging.h"
#include "util/http/https_client.h"

namespace util {

namespace http {

void HttpsClientPool::HandleGuard::operator()(HttpsClient* client) {
  CHECK(client);

  CHECK_GT(pool_->existing_handles_, 0);

  if (client->status()) {
    VLOG(1) << "Deleting client " << client->native_handle() << " due to " << client->status();
    --pool_->existing_handles_;
    delete client;
  } else {
    CHECK(pool_);
    pool_->available_handles_.emplace_back(client);
  }
}

HttpsClientPool::HttpsClientPool(const std::string& domain, ::boost::asio::ssl::context* ssl_ctx,
                                 IoContext* io_cntx)
    : ssl_cntx_(*ssl_ctx), io_cntx_(*io_cntx), domain_(domain) {}

HttpsClientPool::~HttpsClientPool() {
  for (auto* ptr : available_handles_) {
    delete ptr;
  }
}

auto HttpsClientPool::GetHandle() -> ClientHandle {
  while (!available_handles_.empty()) {
    // Pulling the oldest handles first.
    std::unique_ptr<HttpsClient> ptr{std::move(available_handles_.front())};

    available_handles_.pop_front();

    if (ptr->status()) {
      continue;  // we just throw a connection with error status.
    }

    VLOG(1) << "Reusing https client " << ptr->native_handle();

    // pass it further with custom deleter.
    return ClientHandle(ptr.release(), HandleGuard{this});
  }

  // available_handles_ are empty - create a new connection.
  VLOG(1) << "Creating a new https client";

  std::unique_ptr<HttpsClient> client(new HttpsClient{domain_, &io_cntx_, &ssl_cntx_});
  client->set_retry_count(retry_cnt_);

  auto ec = client->Connect(connect_msec_);

  LOG_IF(WARNING, ec) << "HttpsClientPool: Could not connect " << ec;
  ++existing_handles_;

  return ClientHandle{client.release(), HandleGuard{this}};
}

}  // namespace http
}  // namespace util
