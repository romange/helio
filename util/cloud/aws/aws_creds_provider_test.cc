// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.

#include "util/cloud/aws/aws_creds_provider.h"

#include <cstdlib>

#include "base/gtest.h"

namespace util {
namespace cloud::aws {

using namespace std;

class AwsCredsProviderTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // ServiceEndpoint() consults AWS_S3_ENDPOINT first; clear it so the test is
    // hermetic and exercises the partition-suffix fallback.
    unsetenv("AWS_S3_ENDPOINT");
  }
};

TEST_F(AwsCredsProviderTest, PartitionDnsSuffix) {
  EXPECT_EQ(PartitionDnsSuffix("us-east-1"), "amazonaws.com");
  EXPECT_EQ(PartitionDnsSuffix("eu-west-3"), "amazonaws.com");
  EXPECT_EQ(PartitionDnsSuffix(""), "amazonaws.com");
  // GovCloud stays on the standard suffix.
  EXPECT_EQ(PartitionDnsSuffix("us-gov-west-1"), "amazonaws.com");

  EXPECT_EQ(PartitionDnsSuffix("cn-north-1"), "amazonaws.com.cn");
  EXPECT_EQ(PartitionDnsSuffix("cn-northwest-1"), "amazonaws.com.cn");
}

TEST_F(AwsCredsProviderTest, StsEndpoint) {
  // Standard partitions use the global endpoint.
  EXPECT_EQ(StsEndpoint("us-east-1"), "sts.amazonaws.com");
  EXPECT_EQ(StsEndpoint("ap-southeast-2"), "sts.amazonaws.com");
  EXPECT_EQ(StsEndpoint(""), "sts.amazonaws.com");

  // China has no global STS endpoint; it requires the regional host.
  EXPECT_EQ(StsEndpoint("cn-north-1"), "sts.cn-north-1.amazonaws.com.cn");
  EXPECT_EQ(StsEndpoint("cn-northwest-1"), "sts.cn-northwest-1.amazonaws.com.cn");
}

TEST_F(AwsCredsProviderTest, ServiceEndpointStandard) {
  AwsCredsProvider provider("us-east-1");
  EXPECT_EQ(provider.ServiceEndpoint(), "s3.us-east-1.amazonaws.com");
}

TEST_F(AwsCredsProviderTest, ServiceEndpointChina) {
  AwsCredsProvider provider("cn-north-1");
  EXPECT_EQ(provider.ServiceEndpoint(), "s3.cn-north-1.amazonaws.com.cn");
}

TEST_F(AwsCredsProviderTest, ServiceEndpointOverride) {
  AwsCredsProvider provider("cn-north-1", "https://my-minio.example:9000/");
  EXPECT_EQ(provider.ServiceEndpoint(), "my-minio.example:9000");
}

}  // namespace cloud::aws
}  // namespace util
