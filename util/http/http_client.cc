// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/http/http_client.h"

#include <boost/asio/connect.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>

#include "base/logging.h"
#include "util/asio_stream_adapter.h"
#include "util/fiber_socket_base.h"
#include "util/proactor_base.h"

namespace util {
namespace http {

using namespace boost;
using namespace std;

namespace h2 = beast::http;

Client::Client(ProactorBase* proactor) : proactor_(proactor) {}

Client::~Client() {}

std::error_code Client::Connect(StringPiece host, StringPiece service) {
  uint32_t port;
  CHECK(absl::SimpleAtoi(service, &port));
  CHECK_LT(port, 1u << 16);


  auto address = asio::ip::make_address(host);
  FiberSocketBase::endpoint_type ep{address, uint16_t(port)};
  socket_.reset(proactor_->CreateSocket());

  return socket_->Connect(ep);
}

std::error_code Client::Send(Verb verb, StringPiece url, StringPiece body,
                                         Response* response) {
  // Set the URL
  h2::request<h2::string_body> req{verb, boost::string_view(url.data(), url.size()), 11};

  // Optional headers
  for (const auto& k_v : headers_) {
    req.set(k_v.first, k_v.second);
  }
  req.body().assign(body.begin(), body.end());
  req.prepare_payload();

  system::error_code ec;

  AsioStreamAdapter<> adapter(*socket_);

  // Send the HTTP request to the remote host.
  h2::write(adapter, req, ec);
  if (ec) {
    VLOG(1) << "Error " << ec;
    return ec;
  }

  // This buffer is used for reading and must be persisted
  beast::flat_buffer buffer;

  h2::read(adapter, buffer, *response, ec);
  VLOG(2) << "Resp: " << *response;

  return ec;
}

void Client::Shutdown() {
  if (socket_) {
    std::error_code ec;
    socket_->Shutdown(SHUT_RDWR);
    socket_.reset();
  }
}

bool Client::IsConnected() const {
  return socket_ && socket_->IsOpen();
}

}  // namespace http
}  // namespace util
