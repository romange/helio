// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/http/http_client.h"

#include <boost/asio/connect.hpp>
#include <boost/beast/core/flat_buffer.hpp>
#include <boost/beast/http/dynamic_body.hpp>
#include <boost/beast/http/read.hpp>
#include <boost/beast/http/string_body.hpp>
#include <boost/beast/http/write.hpp>

#include "base/logging.h"
#include "util/fiber_socket_base.h"
#include "util/proactor_base.h"
#include "util/dns_resolve.h"

namespace util {
namespace http {

using namespace std;

namespace h2 = boost::beast::http;
namespace ip = boost::asio::ip;
using boost_error = boost::system::error_code;

Client::Client(ProactorBase* proactor) : proactor_(proactor) {}

Client::~Client() {}

std::error_code Client::Connect(StringPiece host, StringPiece service) {
  uint32_t port;
  CHECK(absl::SimpleAtoi(service, &port));
  CHECK_LT(port, 1u << 16);

  char ip[INET_ADDRSTRLEN];

  error_code ec = DnsResolve(host.data(), 2000, ip);
  if (ec) {
    return ec;
  }

  boost_error berr;
  auto address = ip::make_address(ip, berr);
  if (berr)
    return berr;

  FiberSocketBase::endpoint_type ep{address, uint16_t(port)};
  socket_.reset(proactor_->CreateSocket());
  host_ = host;

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

  boost_error ec;

  AsioStreamAdapter<> adapter(*socket_);

  // Send the HTTP request to the remote host.
  h2::write(adapter, req, ec);
  if (ec) {
    VLOG(1) << "Error " << ec;
    return ec;
  }

  // This buffer is used for reading and must be persisted
  boost::beast::flat_buffer buffer;

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
