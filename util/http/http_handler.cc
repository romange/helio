// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/http/http_handler.h"

#include <absl/flags/reflection.h>
#include <absl/strings/match.h>
#include <absl/strings/str_split.h>

#include <boost/beast/http.hpp>
#include <filesystem>

#include "absl/strings/escaping.h"
#include "base/logging.h"
#include "util/http/http_common.h"
#include "util/metrics/family.h"

namespace util {

using namespace http;
using namespace std;
namespace h2 = boost::beast::http;
using metrics::MetricType;
namespace fs = std::filesystem;

namespace {

using FileResponse = ::boost::beast::http::response<::boost::beast::http::file_body>;

inline std::string_view ToStd(::boost::string_view s) {
  return std::string_view(s.data(), s.size());
}

::boost::system::error_code LoadFileResponse(std::string_view fname, FileResponse* resp) {
  FileResponse::body_type::value_type body;
  boost::system::error_code ec;

  body.open(fname.data(), boost::beast::file_mode::scan, ec);
  if (ec) {
    return ec;
  }

  size_t sz = body.size();
  *resp = FileResponse{std::piecewise_construct, std::make_tuple(std::move(body)),
                       std::make_tuple(h2::status::ok, 11)};

  const char* mime = kHtmlMime;
  if (absl::EndsWith(fname, ".svg")) {
    mime = kSvgMime;
  } else if (absl::EndsWith(fname, ".html")) {
    mime = kHtmlMime;
  } else {
    mime = kTextMime;
  }
  SetMime(mime, resp);
  resp->content_length(sz);
  resp->swap(*resp);

  return ec;
}

void HandleVModule(std::string_view str) {
  vector<std::string_view> parts = absl::StrSplit(str, ",", absl::SkipEmpty());
  for (std::string_view p : parts) {
    size_t sep = p.find('=');
    int32_t level = 0;
    if (sep != std::string_view::npos && absl::SimpleAtoi(p.substr(sep + 1), &level)) {
      string module_expr = string(p.substr(0, sep));
      int prev = google::SetVLOGLevel(module_expr.c_str(), level);
      LOG(INFO) << "Setting module " << module_expr << " to loglevel " << level
                << ", prev: " << prev;
    }
  }
}

QueryParam ParseQuery(std::string_view str) {
  std::pair<std::string_view, std::string_view> res;
  size_t pos = str.find('?');
  res.first = str.substr(0, pos);
  if (pos != std::string_view::npos) {
    res.second = str.substr(pos + 1);
  }
  return res;
}

QueryArgs SplitQuery(std::string_view query) {
  vector<std::string_view> args = absl::StrSplit(query, '&');
  vector<std::pair<std::string_view, std::string_view>> res(args.size());
  for (size_t i = 0; i < args.size(); ++i) {
    size_t pos = args[i].find('=');
    res[i].first = args[i].substr(0, pos);
    res[i].second = (pos == std::string_view::npos) ? std::string_view() : args[i].substr(pos + 1);
  }
  return res;
}

h2::response<h2::string_body> ParseFlagz(const QueryArgs& args) {
  h2::response<h2::string_body> response(h2::status::ok, 11);

  std::string_view flag_name;
  std::string_view value;
  for (const auto& k_v : args) {
    if (k_v.first == "flag") {
      flag_name = k_v.second;
    } else if (k_v.first == "value") {
      value = k_v.second;
    }
  }
  if (!flag_name.empty()) {
    absl::CommandLineFlag* cmd_flag = absl::FindCommandLineFlag(flag_name);
    if (cmd_flag == nullptr) {
      response.body() = "Flag not found \n";
    } else {
      SetMime(kHtmlMime, &response);
      response.body().append("<p>Current value ").append(cmd_flag->CurrentValue()).append("</p>");
      string error;
      if (cmd_flag->ParseFrom(value, &error)) {
        response.body().append("Flag ").append(cmd_flag->CurrentValue());
      } else {
        response.body().append("Flag could not be parsed:").append(error);
      }

      if (flag_name == "vmodule") {
        HandleVModule(value);
      }
    }
  } else if (args.size() == 1) {
    LOG(INFO) << "Printing all flags";
    auto flags = absl::GetAllFlags();
    for (const auto& k_v : flags) {
      response.body()
          .append("--")
          .append(k_v.first)
          .append(": ")
          .append(k_v.second->CurrentValue())
          .append("\n");
      SetMime(kTextMime, &response);
    }
  }
  return response;
}

void FilezHandler(const QueryArgs& args, HttpContext* send) {
  std::string_view file_name;
  for (const auto& k_v : args) {
    if (k_v.first == "file") {
      file_name = k_v.second;
    }
  }

  fs::path canonical_file_name = fs::weakly_canonical(file_name);
  auto relative = fs::relative(canonical_file_name, kProfilesFolder);
  if (relative.empty() || (kProfilesFolder / relative) != canonical_file_name) {
    return send->Invoke(MakeStringResponse(h2::status::unauthorized));
  }

  if (file_name.empty()) {
    http::StringResponse resp = MakeStringResponse(h2::status::unauthorized);
    return send->Invoke(std::move(resp));
  }

  FileResponse fresp;
  string fname(file_name);
  auto ec = LoadFileResponse(fname, &fresp);
  if (ec) {
    StringResponse res = MakeStringResponse(h2::status::not_found);
    SetMime(kTextMime, &res);
    if (ec == boost::system::errc::no_such_file_or_directory)
      res.body() = "The resource '" + fname + "' was not found.";
    else
      res.body() = "Error '" + ec.message() + "'.";
    return send->Invoke(std::move(res));
  }

  return send->Invoke(std::move(fresp));
}

/*# HELP go_gc_duration_seconds A summary of the pause duration of garbage collection cycles.
# TYPE go_gc_duration_seconds summary
go_gc_duration_seconds{quantile="0"} 0
go_gc_duration_seconds{quantile="0.25"} 0
go_gc_duration_seconds{quantile="0.5"} 0
go_gc_duration_seconds{quantile="0.75"} 0
go_gc_duration_seconds{quantile="1"} 0
go_gc_duration_seconds_sum 0
go_gc_duration_seconds_count 0
*/

const char* MetricTypeName(MetricType type) {
  switch (type) {
    case MetricType::COUNTER:
      return "counter";
    case MetricType::GAUGE:
      return "gauge";
    case MetricType::SUMMARY:
      return "summary";
    case MetricType::HISTOGRAM:
      return "histogram";
  }
  return "unknown";
}

void AppendLabelTupple(absl::Span<const metrics::Label> label_names,
                       absl::Span<const string_view> label_values, string* dest) {
  if (label_names.empty())
    return;

  for (size_t i = 0; i < label_names.size(); ++i) {
    absl::StrAppend(dest, label_names[i].name(), "=\"", label_values[i], "\",");
  }
  dest->pop_back();
}

void AppendObservation(const metrics::ObservationDescriptor& od, absl::Span<const double> vals,
                       StringResponse* resp) {
  absl::StrAppend(&resp->body(), "# HELP ", od.metric_name, " ", od.metric_help, "\n");
  absl::StrAppend(&resp->body(), "# TYPE ", od.metric_name, " ", MetricTypeName(od.type), "\n");

  if (od.adjustments.empty()) {
    for (size_t i = 0; i < od.label_values.size(); ++i) {
      absl::StrAppend(&resp->body(), od.metric_name, "{");
      AppendLabelTupple(od.label_names, {od.label_values[i], od.label_names.size()}, &resp->body());
      absl::StrAppend(&resp->body(), "} ", vals[i], "\n");
    }
  }
};

void MetricsHandler(const QueryArgs& args, HttpContext* send) {
  StringResponse res = MakeStringResponse();
  SetMime(kTextMime, &res);
  metrics::Iterate(
      [&res](const auto& od, auto vals) { AppendObservation(od, std::move(vals), &res); });
  return send->Invoke(std::move(res));
}

using ParserType = ::boost::beast::http::parser<true, HttpConnection::RequestType::body_type>;

}  // namespace

HttpListenerBase::HttpListenerBase() {
  favicon_url_ =
      "https://rawcdn.githack.com/romange/helio/master/util/http/"
      "favicon-32x32.png";
  resource_prefix_ = "https://cdn.jsdelivr.net/gh/romange/helio/util/http";
}

bool HttpListenerBase::HandleRoot(const RequestType& request, HttpContext* cntx) const {
  std::string_view target = ToStd(request.target());
  if (target == "/favicon.ico") {
    h2::response<h2::string_body> resp = MakeStringResponse(h2::status::moved_permanently);
    resp.set(h2::field::location, favicon_url_);
    resp.set(h2::field::server, "HELIO");
    resp.keep_alive(request.keep_alive());

    cntx->Invoke(std::move(resp));
    return true;
  }

  std::string_view path, query;
  tie(path, query) = ParseQuery(target);
  auto args = SplitQuery(query);

  if (path == "/") {
    cntx->Invoke(BuildStatusPage(args, resource_prefix_));
    return true;
  }

  if (path == "/flagz") {
    h2::response<h2::string_body> resp(h2::status::ok, request.version());
    cntx->Invoke(ParseFlagz(args));
    return true;
  }

  if (path == "/filez") {
    FilezHandler(args, cntx);
    return true;
  }

  if (path == "/profilez") {
    cntx->Invoke(ProfilezHandler(args));
    return true;
  }

  if (enable_metrics_ && path == "/metrics") {
    MetricsHandler(args, cntx);
    return true;
  }
  return false;
}

bool HttpListenerBase::RegisterCb(std::string_view path, RequestCb cb) {

  auto res = cb_map_.emplace(path, std::move(cb));
  return res.second;
}

bool HttpListenerBase::RegisterCb(std::string_view path, RequestCbExt cb) {
  auto res = cb_map_.emplace(path, std::move(cb));
  return res.second;
}

// Limit the parsing buffer to 4K.
HttpConnection::HttpConnection(const HttpListenerBase* base) : owner_(base), req_buffer_(4096) {
}

error_code HttpConnection::ParseFromBuffer(io::Bytes buf) {
  DCHECK(socket_);

  boost::system::error_code ec;
  RequestType request;

  AsioStreamAdapter<> asa(*socket_);

  HttpContext cntx(asa);
  cntx.set_user_data(user_data_);

  while (!buf.empty()) {
    ParserType parser{std::move(request)};
    parser.eager(true);

    size_t consumed = parser.put(boost::asio::const_buffer{buf.data(), buf.size()}, ec);
    if (ec)
      break;
    buf.remove_prefix(consumed);
    request = parser.release();

    VLOG(1) << "Full Url: " << request.target();
    HandleSingleRequest(std::move(request), &cntx);
  }

  if (ec == h2::error::need_more) {
    if (buf.size() > req_buffer_.max_size()) {
      return make_error_code(errc::value_too_large);
    }

    if (!buf.empty()) {
      auto mb = req_buffer_.prepare(buf.size());
      memcpy(mb.data(), buf.data(), buf.size());
      req_buffer_.commit(buf.size());
    }
    return error_code{};
  }

  return ec;
}

void HttpConnection::HandleRequests() {
  CHECK(socket_->IsOpen());

  ::boost::system::error_code ec;

  AsioStreamAdapter<> asa(*socket_);
  RequestType request;

  HttpContext cntx(asa);
  cntx.set_user_data(user_data_);

  while (true) {
    ParserType parser{std::move(request)};
    parser.eager(true);

    h2::read(asa, req_buffer_, parser, ec);
    if (ec) {
      break;
    }

    request = parser.release();

    VLOG(1) << "Full Url: " << request.target();
    HandleSingleRequest(std::move(request), &cntx);
  }

  VLOG(1) << "HttpConnection exit " << ec.message();
  LOG_IF(INFO, !FiberSocketBase::IsConnClosed(ec)) << "Http error " << ec.message();
}

bool HttpConnection::CheckRequestAuthorization(const RequestType& req, HttpContext* cntx,
                                               std::string_view path) {
  CHECK(owner_);

  if (!owner_->auth_functor_)
    return true;

  string_view username, password;
  string decoded;

  if (const boost::string_view header = req[h2::field::authorization]; !header.empty()) {
    string_view header_stl = ToStd(header);
    if (absl::StartsWith(header_stl, "Basic ")) {
      absl::Base64Unescape(header_stl.substr(6), &decoded);
      auto split_pos = decoded.find(':');
      if (split_pos != string::npos) {
        string_view decoded_sv(decoded);
        username = decoded_sv.substr(0, split_pos);
        password = decoded_sv.substr(split_pos + 1, decoded.size());
      }
    }
  }

  if (owner_->auth_functor_(path, username, password)) {
    return true;
  }

  h2::response<h2::string_body> resp{h2::status::unauthorized, req.version()};
  resp.set(h2::field::content_type, "text/plain");
  resp.set(h2::field::www_authenticate, "Basic realm=\"Restricted Area\"");
  resp.body() = "Please authorize!\r\n";
  cntx->Invoke(std::move(resp));

  return false;
}

void HttpConnection::HandleSingleRequest(RequestType&& req, HttpContext* cntx) {
  CHECK(owner_);

  std::string target{ToStd(req.target())};
  auto [path, query] = ParseQuery(target);

  if (!CheckRequestAuthorization(req, cntx, path))
    return;

  if (owner_->HandleRoot(req, cntx)) {
    return;
  }
  VLOG(2) << "Searching for " << path;

  auto it = owner_->cb_map_.find(path);
  if (it == owner_->cb_map_.end()) {
    h2::response<h2::string_body> resp(h2::status::unauthorized, req.version());
    return cntx->Invoke(std::move(resp));
  }
  auto args = SplitQuery(query);
  if (std::holds_alternative<HttpListenerBase::RequestCb>(it->second)) {
    std::get<HttpListenerBase::RequestCb>(it->second)(args, cntx);
  } else {
    std::get<HttpListenerBase::RequestCbExt>(it->second)(args, std::move(req), cntx);
  }
}

}  // namespace util
