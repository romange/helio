// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <boost/beast/http/serializer.hpp>
#include <boost/beast/http/write.hpp>

#include "util/asio_stream_adapter.h"
#include "util/connection.h"
#include "util/http/http_common.h"
#include "util/listener_interface.h"

namespace util {

class HttpContext {
  template <typename Body> using Response = ::boost::beast::http::response<Body>;
  using error_code = ::boost::system::error_code;

  AsioStreamAdapter<>& asa_;

 public:
  explicit HttpContext(AsioStreamAdapter<>& asa) : asa_(asa) {
  }

  template <typename Body> void Invoke(Response<Body>&& msg) {
    // Determine if we should close the connection after
    // close_ = msg.need_eof();

    // We need the serializer here because the serializer requires
    // a non-const file_body, and the message oriented version of
    // http::write only works with const messages.
    msg.prepare_payload();
    ::boost::beast::http::response_serializer<Body> sr{msg};

    ::boost::system::error_code ec;
    ::boost::beast::http::write(asa_, sr, ec);
  }
};

// Should be one per process. Represents http server interface.
// Currently does not support on the fly updates - requires
// multi-threading support.
class HttpHandler2;

class HttpListenerBase : public ListenerInterface {
  friend class HttpHandler2;

 public:
  using RequestType = ::boost::beast::http::request<::boost::beast::http::string_body>;
  typedef std::function<void(const http::QueryArgs&, HttpContext*)> RequestCb;

  HttpListenerBase();

  // Returns true if a callback was registered.
  bool RegisterCb(std::string_view path, RequestCb cb);

  void set_resource_prefix(const char* prefix) {
    resource_prefix_ = prefix;
  }
  void set_favicon(const char* favicon) {
    favicon_ = favicon;
  }

  void enable_metrics() {
    enable_metrics_ = true;
  }

 private:
  bool HandleRoot(const RequestType& rt, HttpContext* cntx) const;

  struct CbInfo {
    RequestCb cb;
  };
  absl::flat_hash_map<std::string_view, CbInfo> cb_map_;

  const char* favicon_;
  const char* resource_prefix_;
  bool enable_metrics_ = false;
};

class HttpHandler2 : public Connection {
 public:
  using RequestType = ::boost::beast::http::request<::boost::beast::http::string_body>;

  HttpHandler2(const HttpListenerBase* base);

  void HandleRequests() final;

 protected:
  void HandleOne(const RequestType& req, HttpContext* cntx);

 private:
  const HttpListenerBase* base_;
};

// http Listener + handler factory. By default creates HttpHandler.
template <typename Handler = HttpHandler2> class HttpListener : public HttpListenerBase {
 public:
  static_assert(std::is_base_of<HttpHandler2, Handler>::value,
                "Handler must be derived from HttpHandler");

  Connection* NewConnection(ProactorBase*) final {
    return new Handler(this);
  }
};

}  // namespace util
