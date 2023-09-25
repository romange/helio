#include "util/aws/aws.h"

#include "base/logging.h"

int main(int argc, char* argv[]) {
  util::aws::Init();

  LOG(INFO) << "s3v2_demo";

  util::aws::Shutdown();
}
