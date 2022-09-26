// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <system_error>

#include <boost/beast/http/fields.hpp>

namespace util {
namespace cloud {

class AWS {
 public:
  static const char kEmptySig[];
  static const char kUnsignedPayloadSig[];

  AWS(const std::string& service, const std::string& region = "")
      : service_(service), region_(region) {
  }

  std::error_code Init();

  using HttpHeader = ::boost::beast::http::header<true, ::boost::beast::http::fields>;

  // TODO: we should remove domain argument in favor to subdomain (bucket).
  // and build the whole domain it from service and region
  // for example, "<bucket>.s3.eu-west-1.amazonaws.com"
  // See: https://docs.aws.amazon.com/general/latest/gr/s3.html
  //
  void Sign(std::string_view payload_sig, HttpHeader* header) const;

  const std::string& region() const {
    return region_;
  }

  void UpdateRegion(std::string_view region);

 private:
  std::string AuthHeader(std::string_view method, std::string_view headers,
                         std::string_view target, std::string_view payload_sig,
                         std::string_view amz_date) const;
  void SetScopeAndSignKey();

  std::string service_, region_, secret_, access_key_, session_token_;

  std::string sign_key_;
  std::string credential_scope_;
  char date_str_[32];
};

}  // namespace cloud

}  // namespace util
