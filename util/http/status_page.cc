// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/http/http_common.h"

#include <absl/strings/str_cat.h>
#include <absl/strings/str_replace.h>
#include <absl/time/clock.h>

#include <cinttypes>  // for PRIu32 and such

#include "base/proc_util.h"
#include "base/varz_node.h"

namespace util {
namespace http {
using namespace std;
using base::VarzListNode;
using boost::beast::http::field;

namespace h2 = ::boost::beast::http;
typedef h2::response<h2::string_body> StringResponse;

namespace {

string GetTimerString(uint64 seconds) {
  char buf[128];
  uint32 hours = seconds / 3600;
  seconds = seconds % 3600;
  uint32 mins = seconds / 60;
  uint32 secs = seconds % 60;
  snprintf(buf, sizeof buf, "%" PRIu32 ":%" PRIu32 ":%" PRIu32, hours, mins, secs);
  return buf;
}

string StatusLine(const string& name, const string& val) {
  string res("<div>");
  res.append(name).append(":<span class='key_text'>").append(val).append("</span></div>\n");
  return res;
}

}  // namespace

StringResponse BuildStatusPage(const QueryArgs& args, string_view resource_prefix) {
  StringResponse response(h2::status::ok, 11);

  bool output_json = false;

  string varz;
  auto start = absl::Now();

  VarzListNode::IterateValues([&varz](const string& nm, const string& val) {
    absl::StrAppend(&varz, "\"", nm, "\": ", val, ",\n");
  });
  absl::StrAppend(&varz, "\"current-time\": ", time(nullptr), ",\n");
  varz.resize(varz.size() - 2);

  for (const auto& k_v : args) {
    if (k_v.first == "o" && k_v.second == "json")
      output_json = true;
  }

  auto delta_usec = absl::ToInt64Microseconds(absl::Now() - start);
  if (output_json) {
    response.set(field::content_type, kJsonMime);
    response.body() = "{" + varz + "}\n";
    return response;
  }

  string a = "<!DOCTYPE html>\n<html><head>\n";
  a += R"(<meta http-equiv='Content-Type' content='text/html; charset=UTF-8'/>
    <link href='https://fonts.googleapis.com/css?family=Roboto:400,300' rel='stylesheet'
     type='text/css'>
    <link rel='stylesheet' href='{s3_path}/status_page.css'>
    <script type="text/javascript" src="{s3_path}/status_page.js"></script>
</head>
<body>
<div><img src='{s3_path}/logo.png' width="160"/></div>)";

  a = absl::StrReplaceAll(a, {{"{s3_path}", resource_prefix}});

  a += "\n<div class='left_panel'></div>\n";
  a += "<div class='styled_border'>\n";
  a += StatusLine("Status", "OK");

  static std::atomic<time_t> start_time_cached{0};
  time_t start_time = start_time_cached.load(std::memory_order_relaxed);
  if (start_time == 0) {
    base::ProcessStats stats = base::ProcessStats::Read();
    start_time = stats.start_time_seconds;
    start_time_cached.store(stats.start_time_seconds, std::memory_order_seq_cst);
  }
  absl::Time atime = absl::FromUnixSeconds(start_time);
  string str_fmt = absl::FormatTime("%Y-%m-%dT%H:%M:%S", atime, absl::UTCTimeZone());
  a += StatusLine("Started on", str_fmt);
  a += StatusLine("Uptime", GetTimerString(time(NULL) - start_time));
  a += StatusLine("Render Latency", absl::StrCat(delta_usec, " us"));

  a += R"(</div>
</body>
<script>
var json_text1 = {)";
  a += varz + R"(};
document.querySelector('.left_panel').innerHTML = JsonToHTML(json_text1);
</script>
</html>)";

  response.set(field::content_type, kHtmlMime);
  response.body() = std::move(a);
  return response;
}

}  // namespace http
}  // namespace util
