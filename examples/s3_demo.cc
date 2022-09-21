// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <absl/time/time.h>

#include "base/init.h"
#include "util/cloud/aws.h"
#include "util/cloud/s3.h"
#include "util/http/http_client.h"
#include "util/uring/uring_pool.h"

using namespace std;
using namespace util;
using cloud::AWS;

ABSL_FLAG(string, cmd, "ls", "");
ABSL_FLAG(string, region, "us-east-1", "");

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
  http::Client http_client{proactor};

  http_client.set_connect_timeout_ms(2000);
  auto list_res = proactor->Await([&] {
    // "s3.amazonaws.com"
    CHECK_EC(http_client.Connect("52.217.130.8", "80"));

    return ListS3Buckets(*aws, &http_client);
  });

  CHECK(list_res);

  for (const auto& b : *list_res) {
    cout << b << endl;
  }
};

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  unique_ptr<ProactorPool> pp;
  pp.reset(new uring::UringPool);

  pp->Run();

  AWS aws{absl::GetFlag(FLAGS_region), "s3"};
  CHECK_EC(aws.Init());

  ListBuckets(&aws, pp->GetNextProactor());

  pp->Stop();

  return 0;
}
