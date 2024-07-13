// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/gcp/gcp_utils.h"

#include <absl/strings/str_cat.h>

#include <boost/beast/http/string_body.hpp>

#include "base/logging.h"
#include "util/cloud/gcp/gcp_creds_provider.h"

namespace util::cloud {
using namespace std;
namespace h2 = boost::beast::http;

namespace {

bool IsUnauthorized(const h2::header<false, h2::fields>& resp) {
  if (resp.result() != h2::status::unauthorized) {
    return false;
  }
  auto it = resp.find("WWW-Authenticate");

  return it != resp.end();
}

inline bool DoesServerPushback(h2::status st) {
  return st == h2::status::too_many_requests ||
         h2::to_status_class(st) == h2::status_class::server_error;
}

constexpr auto kResumeIncomplete = h2::status::permanent_redirect;

bool IsResponseOK(h2::status st) {
  // Partial content can appear because of the previous reconnect.
  // For multipart uploads kResumeIncomplete can be returned.
  return st == h2::status::ok || st == h2::status::partial_content || st == kResumeIncomplete;
}

}  // namespace

const char GCS_API_DOMAIN[] = "storage.googleapis.com";

string AuthHeader(string_view access_token) {
  return absl::StrCat("Bearer ", access_token);
}

namespace detail {

EmptyRequestImpl::EmptyRequestImpl(h2::verb req_verb, std::string_view url,
                                   const string_view access_token)
    : req_{req_verb, boost::beast::string_view{url.data(), url.size()}, 11} {
  req_.set(h2::field::host, GCS_API_DOMAIN);
  req_.set(h2::field::authorization, AuthHeader(access_token));
  // ? req_.keep_alive(true);
}

std::error_code EmptyRequestImpl::Send(http::Client* client) {
  return client->Send(req_);
}

std::error_code DynamicBodyRequestImpl::Send(http::Client* client) {
  return client->Send(req_);
}

}  // namespace detail

RobustSender::RobustSender(unsigned num_iterations, GCPCredsProvider* provider)
    : num_iterations_(num_iterations), provider_(provider) {
}

auto RobustSender::Send(http::Client* client,
                        detail::HttpRequestBase* req) -> io::Result<HeaderParserPtr> {
  error_code ec;
  for (unsigned i = 0; i < num_iterations_; ++i) {  // Iterate for possible token refresh.
    VLOG(1) << "HttpReq " << client->host() << ": " << req->GetHeaders() << ", ["
            << client->native_handle() << "]";

    RETURN_UNEXPECTED(req->Send(client));
    HeaderParserPtr parser(new h2::response_parser<h2::empty_body>());
    RETURN_UNEXPECTED(client->ReadHeader(parser.get()));
    {
      const auto& msg = parser->get();
      VLOG(1) << "RespHeader" << i << ": " << msg;

      if (!parser->keep_alive()) {
        LOG(FATAL) << "TBD: Schedule reconnect due to conn-close header";
      }

      if (IsResponseOK(msg.result())) {
        return parser;
      }
    }

    // We have some kind of error, possibly with body that needs to be drained.
    h2::response_parser<h2::string_body> drainer(std::move(*parser));
    RETURN_UNEXPECTED(client->Recv(&drainer));
    const auto& msg = drainer.get();

    if (DoesServerPushback(msg.result())) {
      LOG(INFO) << "Retrying(" << client->native_handle() << ") with " << msg;

      ThisFiber::SleepFor(100ms);
      continue;
    }

    if (IsUnauthorized(msg)) {
      RETURN_UNEXPECTED(provider_->RefreshToken(client->proactor()));
      req->SetHeader(h2::field::authorization, AuthHeader(provider_->access_token()));

      continue;
    }

    if (msg.result() == h2::status::forbidden) {
      return nonstd::make_unexpected(make_error_code(errc::operation_not_permitted));
    }

    ec = make_error_code(errc::bad_message);
    LOG(DFATAL) << "Unexpected response " << msg << "\n" << msg.body() << "\n";
  }

  return nonstd::make_unexpected(ec);
}

}  // namespace util::cloud