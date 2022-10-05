// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <boost/beast/http/empty_body.hpp>
#include <boost/beast/http/fields.hpp>
#include <boost/beast/http/string_body.hpp>
#include <system_error>

#include "util/http/http_client.h"

namespace util {
namespace cloud {

namespace h2 = boost::beast::http;

struct AwsConnectionData {
  std::string access_key, secret_key, session_token;
  std::string region, role_name;
};

class AWS {
 public:
  static const char kEmptySig[];
  static const char kUnsignedPayloadSig[];

  AWS(const std::string& service, const std::string& region = "") : service_(service) {
    connection_data_.region = region;
  }

  std::error_code Init();

  using HttpHeader = ::boost::beast::http::header<true, ::boost::beast::http::fields>;

  // Sign request and send it with client->Send() internally. Try to refresh session token if
  // needed.
  std::error_code SendRequest(std::string_view payload_sig, http::Client* client,
                              h2::request<h2::empty_body>* req,
                              h2::response<h2::string_body>* resp);

  const std::string& region() const {
    return connection_data_.region;
  }

  void UpdateRegion(std::string_view region);

 private:
  // TODO: we should remove domain argument in favor to subdomain (bucket).
  // and build the whole domain it from service and region
  // for example, "<bucket>.s3.eu-west-1.amazonaws.com"
  // See: https://docs.aws.amazon.com/general/latest/gr/s3.html
  //
  void Sign(std::string_view payload_sig, HttpHeader* header) const;

  std::string AuthHeader(std::string_view method, std::string_view headers, std::string_view target,
                         std::string_view payload_sig, std::string_view amz_date) const;
  void SetScopeAndSignKey();

  std::string service_;
  AwsConnectionData connection_data_;

  std::string sign_key_;
  std::string credential_scope_;
  char date_str_[32];
};

}  // namespace cloud

}  // namespace util
