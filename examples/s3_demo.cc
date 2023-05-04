// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/strings/str_split.h>
#include <absl/strings/strip.h>
#include <absl/time/time.h>

#include "base/init.h"
#include "util/cloud/aws.h"
#include "util/cloud/s3.h"
#include "util/cloud/s3_file.h"
#include "util/fibers/pool.h"
#include "util/http/http_client.h"

using namespace std;
using namespace util;
using cloud::AWS;

ABSL_FLAG(string, cmd, "ls", "");
ABSL_FLAG(string, region, "us-east-1", "");
ABSL_FLAG(string, path, "", "s3://bucket/path");
ABSL_FLAG(string, endpoint, "", "s3 endpoint");
ABSL_FLAG(uint32_t, num_iters, 1, "Number of iterations");
ABSL_FLAG(uint32_t, delay, 5, "Delay in seconds between each iteration");

namespace h2 = boost::beast::http;

#define CHECK_EC(x)                                                                 \
  do {                                                                              \
    auto __ec$ = (x);                                                               \
    CHECK(!__ec$) << "Error: " << __ec$ << " " << __ec$.message() << " for " << #x; \
  } while (false)

template <typename Body> std::ostream& operator<<(std::ostream& os, const h2::request<Body>& msg) {
  os << msg.method_string() << " " << msg.target() << endl;
  for (const auto& f : msg) {
    os << f.name_string() << " : " << f.value() << endl;
  }
  os << "-------------------------";

  return os;
}

void ListBuckets(AWS* aws, ProactorBase* proactor) {
  string endpoint = absl::GetFlag(FLAGS_endpoint);
  if (endpoint.empty()) {
    endpoint = "s3.amazonaws.com:80";
  }

  vector<string> parts = absl::StrSplit(endpoint, ':');
  CHECK_EQ(parts.size(), 2u);

  http::Client http_client{proactor};

  http_client.set_connect_timeout_ms(2000);
  auto list_res = proactor->Await([&] {
    CHECK_EC(http_client.Connect(parts[0], parts[1]));
    return ListS3Buckets(aws, &http_client);
  });

  if (!list_res) {
    cout << "Error: " << list_res.error() << endl;
    return;
  }

  for (const auto& b : *list_res) {
    cout << b << endl;
  }
};

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  unique_ptr<ProactorPool> pp;
#ifdef USE_FB2
  pp.reset(fb2::Pool::IOUring(256));
#else
  pp.reset(new uring::UringPool);
#endif
  pp->Run();

  AWS aws{"s3", absl::GetFlag(FLAGS_region)};

  pp->GetNextProactor()->Await([&] { CHECK_EC(aws.Init()); });

  string cmd = absl::GetFlag(FLAGS_cmd);
  string path = absl::GetFlag(FLAGS_path);
  string endpoint = absl::GetFlag(FLAGS_endpoint);

  if (path.empty()) {
    CHECK(cmd == "ls");
    ListBuckets(&aws, pp->GetNextProactor());
  } else {
    string_view clean = absl::StripPrefix(path, "s3://");
    string_view obj_path;
    size_t pos = clean.find('/');
    string_view bucket_name = clean.substr(0, pos);
    if (pos != string_view::npos) {
      obj_path = clean.substr(pos + 1);
    }
    cloud::S3Bucket bucket = cloud::S3Bucket::FromEndpoint(aws, endpoint, bucket_name);

    if (cmd == "ls") {
      cloud::S3Bucket::ListObjectCb cb = [](size_t sz, string_view name) { CONSOLE_INFO << name; };

      error_code ec = pp->GetNextProactor()->Await([&] {
        auto ec = bucket.Connect(300);
        if (ec)
          return ec;
        unsigned num_iters = absl::GetFlag(FLAGS_num_iters);
        for (unsigned i = 0; i < num_iters; ++i) {
          ec = bucket.ListAllObjects(obj_path, cb);
          if (ec)
            return ec;

          if (i + 1 < num_iters)
            ThisFiber::SleepFor(chrono::seconds(absl::GetFlag(FLAGS_delay)));
        }
        return ec;
      });

      CHECK(!ec) << ec;
    } else if (cmd == "read") {
      pp->GetNextProactor()->Await([&] {
        auto ec = bucket.Connect(300);
        CHECK(!ec);

        io::Result<io::ReadonlyFile*> res = bucket.OpenReadFile(obj_path);
        if (res) {
          io::ReadonlyFile* file = *res;
          std::unique_ptr<uint8_t[]> buf(new uint8_t[1024]);
          io::SizeOrError sz_res = file->Read(0, io::MutableBytes(buf.get(), 1024));
          if (sz_res) {
            CONSOLE_INFO << "File contents(first 1024) of " << obj_path << ":";
            CONSOLE_INFO << string_view(reinterpret_cast<char*>(buf.get()), *sz_res);
          } else {
            LOG(ERROR) << "Error: " << sz_res.error();
          }
        } else {
          LOG(ERROR) << "Read Error: " << res.error().message();
        }
      });
    } else if (cmd == "write") {
      pp->GetNextProactor()->Await([&] {
        auto ec = bucket.Connect(300);
        CHECK(!ec);

        io::Result<io::WriteFile*> res = bucket.OpenWriteFile(obj_path);
        if (res) {
          unique_ptr<io::WriteFile> file{*res};
          CHECK(file);
          std::unique_ptr<uint8_t[]> buf(new uint8_t[1024]);
          memset(buf.get(), 'R', 1024);
          for (size_t i = 0; i < 10240; ++i) {
            ec = file->Write(io::Bytes(buf.get(), 1024));
            CHECK(!ec);
          }
          ec = file->Close();
          CHECK(!ec);
        } else {
          LOG(ERROR) << "Error: " << res.error();
        }
      });
    } else {
      LOG(ERROR) << "Unknown command " << cmd;
    }
  }

  pp->Stop();

  return 0;
}
