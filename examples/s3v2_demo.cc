#include <aws/core/auth/AWSCredentialsProvider.h>

#include "base/logging.h"

int main() {
  Aws::Auth::EnvironmentAWSCredentialsProvider provider{};
  Aws::Auth::AWSCredentials creds = provider.GetAWSCredentials();
  LOG(INFO) << "access key id " << creds.GetAWSAccessKeyId();
}
