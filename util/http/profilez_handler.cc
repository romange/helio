// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <unordered_map>

#include <gperftools/profiler.h>

#include <absl/time/clock.h>
#include <absl/time/time.h>
#include "base/logging.h"
#include "base/proc_util.h"

#include "util/fibers/fibers_ext.h"
#include "util/http/http_common.h"


namespace util {
namespace http {
namespace {
char last_profile_suffix[100] = {0};
}

using namespace std;
using namespace boost;
using beast::http::field;
namespace h2 = beast::http;
typedef h2::response<h2::string_body> StringResponse;

static void HandleCpuProfile(bool enable, StringResponse* response) {
  string profile_name = "/tmp/" + base::ProgramBaseName();
  response->set(h2::field::cache_control, "no-cache, no-store, must-revalidate");
  response->set(h2::field::pragma, "no-cache");
  response->set(field::content_type, kHtmlMime);

  auto& body = response->body();

  if (enable) {
    if (last_profile_suffix[0]) {
      body.append("<p> Yo, already profiling, stupid!</p>\n");
    } else {
      string suffix = absl::FormatTime("_%d%m%Y_%H%M%S.prof", absl::Now(), absl::UTCTimeZone());
      profile_name.append(suffix);
      strcpy(last_profile_suffix, suffix.c_str());
      int res = ProfilerStart(profile_name.c_str());
      LOG(INFO) << "Starting profiling into " << profile_name << " " << res;
      body.append(
          "<h1> Wow, your code is so fast!</h1> \n"
          "<img "
          "src='https://gist.github.com/romange/4760c3eebc407755f856fec8e5b6d4c1/raw/"
          "8da7e4129da2ebef26a0fad7b637364439f33e97/profiler2.gif'>\n");
    }
    return;
  }
  ProfilerStop();
  if (last_profile_suffix[0] == '\0') {
    body.append("<h3>Profiling is off, commander!</h3> \n")
    .append("<img src='https://i.giphy.com/media/l0IykG0AM7911MrCM/giphy.webp'>\n");
    return;
  }
  string cmd("nice -n 15 pprof -noinlines -lines -unit ms --svg ");
  string symbols_name = base::ProgramAbsoluteFileName() + ".debug";
  LOG(INFO) << "Symbols " << symbols_name << ", suffix: " << last_profile_suffix;
  if (access(symbols_name.c_str(), R_OK) != 0) {
    symbols_name = base::ProgramAbsoluteFileName();
  }
  cmd.append(symbols_name).append(" ");

  profile_name.append(last_profile_suffix);
  cmd.append(profile_name).append(" > ");

  string err_log = profile_name + ".err";
  profile_name.append(".svg");

  cmd.append(profile_name).append(" 2> ").append(err_log);

  LOG(INFO) << "Running command: " << cmd;
  last_profile_suffix[0] = '\0';

  int sh_res = base::sh_exec(cmd.c_str());
  if (sh_res != 0) {
    LOG(ERROR) << "Error running sh_exec, status: " << errno << " " << strerror(errno);
  }

  // Redirect browser to show this file.
  string url("filez?file=");
  url.append(profile_name);
  LOG(INFO) << "Redirecting to " << url;
  google::FlushLogFiles(google::INFO);

  response->set(h2::field::location, url);
  response->result(h2::status::moved_permanently);
}


StringResponse ProfilezHandler(const QueryArgs& args) {
  bool enable = false;
  bool heap = false;
  for (const auto& k_v : args) {
    if (k_v.first == "profile") {
      enable = (k_v.second == "on");
    } else if (k_v.first == "heap") {
      heap = true;
      enable = (k_v.second == "on");
    }
  }

  fibers_ext::Done done;
  StringResponse response;
  std::thread([=, &response]() mutable {
    if (!heap) {
      HandleCpuProfile(enable, &response);
    } else {
      // TBD.
    }
    done.Notify();
  }).detach();

  // Instead of joining the thread which is not fiber-friendly,
  // I use done to block the fiber but free the thread to handle other fibers.
  // Still this fiber connection is blocked.
  done.Wait();
  return response;
}

}  // namespace http
}  // namespace util
