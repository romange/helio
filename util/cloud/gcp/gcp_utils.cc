// Copyright 2024, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/gcp/gcp_utils.h"

#include <absl/strings/str_cat.h>

#include <boost/beast/http/string_body.hpp>

#include "base/logging.h"
#include "util/cloud/gcp/gcp_creds_provider.h"
#include "util/http/http_client.h"

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

EmptyRequestImpl CreateGCPEmptyRequest(boost::beast::http::verb req_verb, std::string_view url,
                                       const std::string_view access_token) {
  EmptyRequestImpl res(req_verb, url);
  res.SetHeader(h2::field::host, GCS_API_DOMAIN);
  res.SetHeader(h2::field::authorization, AuthHeader(access_token));
  return res;
}

}  // namespace detail

RobustSender::RobustSender(http::ClientPool* pool, GCPCredsProvider* provider)
    : pool_(pool), provider_(provider) {
}

error_code RobustSender::Send(unsigned num_iterations, detail::HttpRequestBase* req,
                              SenderResult* result) {
  error_code ec;
  for (unsigned i = 0; i < num_iterations; ++i) {  // Iterate for possible token refresh.
    auto res = pool_->GetHandle();
    if (!res)
      return res.error();

    result->client_handle = std::move(res.value());
    auto* client_handle = result->client_handle.get();
    VLOG(1) << "HttpReq " << client_handle->host() << ": " << req->GetHeaders() << ", ["
            << client_handle->native_handle() << "]";

    RETURN_ERROR(req->Send(client_handle));
    result->eb_parser.reset(new h2::response_parser<h2::empty_body>());

    // no limit. Prevent from this parser to throw an error due to large body.
    // result->eb_parser->body_limit(boost::optional<uint64_t>());
    auto header_err = client_handle->ReadHeader(result->eb_parser.get());

    // Unfortunately earlier versions of boost (1.74-) have a bug that do not support the body_limit
    // directive above. Therefore, we fix it here.
    if (header_err == h2::error::body_limit) {
      header_err.clear();
    }
    RETURN_ERROR(header_err);
    {
      const auto& msg = result->eb_parser->get();
      VLOG(1) << "RespHeader" << i << ": " << msg;

      if (!result->eb_parser->keep_alive()) {
        LOG(FATAL) << "TBD: Schedule reconnect due to conn-close header";
      }

      if (IsResponseOK(msg.result())) {
        return {};
      }
    }

    // We have some kind of error, possibly with body that needs to be drained.
    h2::response_parser<h2::string_body> drainer(std::move(*result->eb_parser));
    RETURN_ERROR(client_handle->Recv(&drainer));
    const auto& msg = drainer.get();

    if (DoesServerPushback(msg.result())) {
      LOG(INFO) << "Retrying(" << client_handle->native_handle() << ") with " << msg;

      ThisFiber::SleepFor(100ms);
      continue;
    }

    if (IsUnauthorized(msg)) {
      VLOG(1) << "Refreshing token";
      RETURN_ERROR(provider_->RefreshToken());
      provider_->Sign(req);
      continue;
    }

    if (msg.result() == h2::status::forbidden) {
      return make_error_code(errc::operation_not_permitted);
    }

    if (msg.result() == h2::status::not_found) {
      return make_error_code(errc::no_such_file_or_directory);
    }

    ec = make_error_code(errc::bad_message);
    LOG(DFATAL) << "Unexpected response " << msg << "\n" << msg.body() << "\n";
  }

  return ec;
}

}  // namespace util::cloud
