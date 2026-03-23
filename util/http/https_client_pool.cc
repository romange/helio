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
  // Parse host and optional port from domain_ ("host" or "host:port").
  std::string host = domain_;
  std::string port;
  if (auto pos = domain_.rfind(':'); pos != std::string::npos) {
    host = domain_.substr(0, pos);
    port = domain_.substr(pos + 1);
  }

  std::unique_ptr<Client> client;
  std::error_code ec;

  if (ssl_cntx_) {
    if (port.empty()) port = "443";
    VLOG(1) << "Creating a new TLS client to " << host << ":" << port;
    auto* tls = new TlsClient{&proactor_};
    tls->set_retry_count(retry_cnt_);
    tls->set_connect_timeout_ms(connect_msec_);
    if (on_connect_) tls->AssignOnConnect(on_connect_);
    ec = tls->Connect(host, port, ssl_cntx_);
    client.reset(tls);
  } else {
    if (port.empty()) port = "80";
    VLOG(1) << "Creating a new HTTP client to " << host << ":" << port;
    client.reset(new Client{&proactor_});
    client->set_retry_count(retry_cnt_);
    client->set_connect_timeout_ms(connect_msec_);
    if (on_connect_) client->AssignOnConnect(on_connect_);
    ec = client->Connect(host, port);
  }

  LOG_IF(WARNING, ec) << "ClientPool: Could not connect to " << host << ":" << port << " - " << ec;
  if (ec)
    return nonstd::make_unexpected(ec);
  ++existing_handles_;

  return ClientHandle{client.release(), HandleGuard{this}};
}

}  // namespace http
}  // namespace util
