#include "util/aws/aws.h"

#include "base/init.h"
#include "base/flags.h"
#include "base/logging.h"

ABSL_FLAG(std::string, cmd, "list-buckets", "Command to run");

int main(int argc, char* argv[]) {
  MainInitGuard guard(&argc, &argv);

  util::aws::Init();

  LOG(INFO) << "s3v2_demo; cmd=" << absl::GetFlag(FLAGS_cmd);

  util::aws::Shutdown();
}
