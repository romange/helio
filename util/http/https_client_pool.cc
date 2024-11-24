// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/http/https_client_pool.h"

#include "base/logging.h"
#include "util/http/http_client.h"

namespace util {

namespace http {

void ClientPool::HandleGuard::operator()(Client* client) {
  DVLOG(1) << "HandleGuard dtor " << client << " pool_ " << pool_;
  CHECK(client);

  CHECK_GT(pool_->existing_handles_, 0);

  if (client->IsConnected()) {
    CHECK(pool_);
    pool_->available_handles_.emplace_back(client);
  } else {
    VLOG(1) << "Deleting client ";
    --pool_->existing_handles_;
    delete client;
  }
}

ClientPool::ClientPool(const std::string& domain, SSL_CTX* ssl_ctx, fb2::ProactorBase* pb)
    : domain_(domain), ssl_cntx_(ssl_ctx), proactor_(*pb) {
  CHECK(pb);
}

ClientPool::~ClientPool() {
  DVLOG(1) << "ClientPool dtor " << this;
  DCHECK_EQ(unsigned(existing_handles_), available_handles_.size());
  for (auto* ptr : available_handles_) {
    delete ptr;
  }
}

auto ClientPool::GetHandle() -> io::Result<ClientHandle> {
  while (!available_handles_.empty()) {
    // Pulling the oldest handles first.
    std::unique_ptr<Client> ptr{std::move(available_handles_.front())};

    available_handles_.pop_front();

    if (!ptr->IsConnected()) {
      continue;  // we just throw a connection with error status.
    }

    VLOG(1) << "Reusing https client " << ptr->native_handle();

    // pass it further with custom deleter.
    return ClientHandle(ptr.release(), HandleGuard{this});
  }

  // available_handles_ are empty - create a new connection.
  VLOG(1) << "Creating a new https client";

  // TODO: create tls/Non-tls clients based on whether ssl_cntx_ is null.
  std::unique_ptr<TlsClient> client(new TlsClient{&proactor_});
  client->set_retry_count(retry_cnt_);
  if (on_connect_) {
    client->AssignOnConnect(on_connect_);
  }
  auto ec = client->Connect(domain_, "443", ssl_cntx_);

  LOG_IF(WARNING, ec) << "ClientPool: Could not connect " << ec;
  if (ec)
    return nonstd::make_unexpected(ec);
  ++existing_handles_;

  return ClientHandle{client.release(), HandleGuard{this}};
}

}  // namespace http
}  // namespace util
