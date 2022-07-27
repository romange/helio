// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <absl/container/flat_hash_map.h>

#include <boost/beast/core.hpp>  // for flat_buffer.
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
    namespace h2 = ::boost::beast::http;
    msg.prepare_payload();
    h2::response_serializer<Body> sr{msg};

    ::boost::system::error_code ec;
    h2::write(asa_, sr, ec);
  }

  template<typename Serializer> ::boost::system::error_code Write(const Serializer& ser) {
    namespace h2 = ::boost::beast::http;

    ::boost::system::error_code ec;
    h2::write(asa_, ser, ec);
    return ec;
  }
};

// Should be one per process. Represents http server interface.
// Currently does not support on the fly updates - requires
// multi-threading support.
class HttpConnection;

class HttpListenerBase : public ListenerInterface {
  friend class HttpConnection;

 public:
  using RequestType = ::boost::beast::http::request<::boost::beast::http::string_body>;
  typedef std::function<void(const http::QueryArgs&, HttpContext*)> RequestCb;

  HttpListenerBase();

  // Returns true if a callback was registered.
  bool RegisterCb(std::string_view path, RequestCb cb);

  void set_resource_prefix(std::string_view prefix) {
    resource_prefix_ = prefix;
  }

  void set_favicon(std::string_view url) {
    favicon_url_ = url;
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

  std::string favicon_url_;
  std::string resource_prefix_;
  bool enable_metrics_ = false;
};

class HttpConnection : public Connection {
 public:
  using RequestType = ::boost::beast::http::request<::boost::beast::http::string_body>;

  HttpConnection(const HttpListenerBase* base);

  // Parses one or more http request from the buffer.
  // In case that there is a leftover - keeps it so that
  // HandleRequest could continue parsing from the same point via socket.
  // In any case, buf is fully consumed or the error is returned.
  std::error_code ParseFromBuffer(::io::Bytes buf);

  void HandleRequests() final;

 protected:
  void HandleSingleRequest(const RequestType& req, HttpContext* cntx);

 private:

  const HttpListenerBase* owner_;
  ::boost::beast::flat_buffer req_buffer_;
};

// http Listener + handler factory. By default creates HttpHandler.
template <typename Handler = HttpConnection> class HttpListener : public HttpListenerBase {
 public:
  static_assert(std::is_base_of<HttpConnection, Handler>::value,
                "Handler must be derived from HttpHandler");

  Connection* NewConnection(ProactorBase*) final {
    return new Handler(this);
  }
};

}  // namespace util
