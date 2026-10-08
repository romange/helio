// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/cloud/platform_info.h"

#include <absl/cleanup/cleanup.h>
#include <absl/strings/str_cat.h>
#include <unistd.h>

#include <cstdlib>
#include <filesystem>
#include <optional>

#include "base/gtest.h"
#include "io/file.h"
#include "io/file_util.h"

namespace util::cloud {
namespace {

constexpr const char* kEnvVars[] = {
    "RAILWAY_ENVIRONMENT", "AWS_EXECUTION_ENV",  "ECS_CONTAINER_METADATA_URI_V4",
    "K_SERVICE",           "CONTAINER_APP_NAME", "KUBERNETES_SERVICE_HOST",
    "container",           "INVOCATION_ID",
};

class PlatformInfoTest : public testing::Test {
 protected:
  void SetUp() override {
    for (const char* name : kEnvVars) {
      std::optional<std::string> original;
      if (const char* value = getenv(name))
        original = value;
      original_env_.emplace_back(name, std::move(original));
      ASSERT_EQ(unsetenv(name), 0);
    }

    root_ = base::GetTestTempPath(testing::UnitTest::GetInstance()->current_test_info()->name());
    std::error_code ec;
    std::filesystem::remove_all(root_, ec);
    ASSERT_FALSE(ec) << ec;
    std::filesystem::create_directories(root_ + "/proc", ec);
    ASSERT_FALSE(ec) << ec;
    std::filesystem::create_directories(root_ + "/sys/class/dmi/id", ec);
    ASSERT_FALSE(ec) << ec;
    std::filesystem::create_directories(root_ + "/run", ec);
    ASSERT_FALSE(ec) << ec;
  }

  void TearDown() override {
    for (const auto& [name, value] : original_env_) {
      if (value)
        EXPECT_EQ(setenv(name, value->c_str(), 1), 0);
      else
        EXPECT_EQ(unsetenv(name), 0);
    }
  }

  void WriteCpu(std::string_view content) {
    io::WriteStringToFileOrDie(content, root_ + "/proc/cpuinfo");
  }

  void WriteDmi(std::string_view vendor, std::string_view product = "",
                std::string_view asset_tag = "", std::string_view bios = "") {
    std::string directory = root_ + "/sys/class/dmi/id/";
    io::WriteStringToFileOrDie(absl::StrCat(vendor, "\n"), directory + "sys_vendor");
    io::WriteStringToFileOrDie(absl::StrCat(product, "\n"), directory + "product_name");
    io::WriteStringToFileOrDie(absl::StrCat(asset_tag, "\n"), directory + "chassis_asset_tag");
    io::WriteStringToFileOrDie(absl::StrCat(bios, "\n"), directory + "bios_version");
  }

  std::string root_;
  std::vector<std::pair<const char*, std::optional<std::string>>> original_env_;
};

TEST_F(PlatformInfoTest, DmiCloudAndHypervisorClassification) {
  struct Case {
    const char* vendor;
    const char* product;
    const char* asset_tag;
    const char* bios;
    CloudProvider cloud;
    Virtualization virtualization;
  };
  const Case cases[] = {
      {"Amazon EC2", "", "", "", CloudProvider::kAws, Virtualization::kUnknown},
      {"Xen", "", "", "AMAZON EC2", CloudProvider::kAws, Virtualization::kXen},
      {"Google", "", "", "", CloudProvider::kGcp, Virtualization::kUnknown},
      {"", "Google Compute Engine", "", "", CloudProvider::kGcp, Virtualization::kUnknown},
      {"Microsoft Corporation", "", "7783-7084-3265-9085-8269-3286-77", "", CloudProvider::kAzure,
       Virtualization::kHyperV},
      {"Microsoft Corporation", "", "not-azure", "", CloudProvider::kUnknown,
       Virtualization::kHyperV},
      {"", "", "OracleCloud.com", "", CloudProvider::kOracle, Virtualization::kUnknown},
      {"Hetzner", "", "", "", CloudProvider::kHetzner, Virtualization::kUnknown},
      {"", "OpenStack Nova", "", "", CloudProvider::kOpenStack, Virtualization::kUnknown},
      {"", "OpenStack Compute", "", "", CloudProvider::kOpenStack, Virtualization::kUnknown},
      {"VMware, Inc.", "", "", "", CloudProvider::kUnknown, Virtualization::kVmware},
      {"QEMU", "", "", "", CloudProvider::kUnknown, Virtualization::kKvm},
      {"", "KVM", "", "", CloudProvider::kUnknown, Virtualization::kKvm},
      {"Xen", "", "", "", CloudProvider::kUnknown, Virtualization::kXen},
      {"Other", "", "", "", CloudProvider::kUnknown, Virtualization::kUnknown},
  };
  WriteCpu("flags : fpu\n");
  for (const auto& test : cases) {
    SCOPED_TRACE(absl::StrCat(test.vendor, "/", test.product, "/", test.asset_tag, "/", test.bios));
    WriteDmi(test.vendor, test.product, test.asset_tag, test.bios);
    auto info = PlatformInfo::Create(root_);
    EXPECT_EQ(info.cloud, test.cloud);
    EXPECT_EQ(info.virtualization, test.virtualization);
  }
}

TEST_F(PlatformInfoTest, MissingAzureTagDoesNotImplyAzure) {
  WriteDmi("Microsoft Corporation");
  ASSERT_TRUE(io::Delete(root_ + "/sys/class/dmi/id/chassis_asset_tag"));
  auto info = PlatformInfo::Create(root_);
  EXPECT_EQ(info.cloud, CloudProvider::kUnknown);
  EXPECT_EQ(info.virtualization, Virtualization::kHyperV);
  ASSERT_FALSE(info.dmi.chassis_asset_tag);
  EXPECT_EQ(info.dmi.chassis_asset_tag.error(), (std::error_code{ENOENT, std::system_category()}));
}

TEST_F(PlatformInfoTest, MissingVendorDoesNotDiscardOtherDmiEvidence) {
  WriteDmi("", "Google Compute Engine");
  ASSERT_TRUE(io::Delete(root_ + "/sys/class/dmi/id/sys_vendor"));
  auto info = PlatformInfo::Create(root_);
  EXPECT_EQ(info.cloud, CloudProvider::kGcp);
  EXPECT_FALSE(info.dmi.sys_vendor);

  WriteDmi("", "", "OracleCloud.com");
  ASSERT_TRUE(io::Delete(root_ + "/sys/class/dmi/id/sys_vendor"));
  info = PlatformInfo::Create(root_);
  EXPECT_EQ(info.cloud, CloudProvider::kOracle);
  EXPECT_FALSE(info.dmi.sys_vendor);
}

TEST_F(PlatformInfoTest, CloudEnvironmentFallbacks) {
  const std::pair<const char*, CloudProvider> cases[] = {
      {"RAILWAY_ENVIRONMENT", CloudProvider::kRailway},
      {"AWS_EXECUTION_ENV", CloudProvider::kAws},
      {"ECS_CONTAINER_METADATA_URI_V4", CloudProvider::kAws},
      {"K_SERVICE", CloudProvider::kGcp},
      {"CONTAINER_APP_NAME", CloudProvider::kAzure},
  };
  for (const auto& [name, cloud] : cases) {
    SCOPED_TRACE(name);
    ASSERT_EQ(setenv(name, "test", 1), 0);
    auto info = PlatformInfo::Create(root_);
    EXPECT_EQ(info.cloud, cloud);
    EXPECT_FALSE(info.dmi.sys_vendor);
    ASSERT_EQ(unsetenv(name), 0);
  }
}

TEST_F(PlatformInfoTest, CloudEvidencePrecedence) {
  WriteDmi("Amazon EC2");
  ASSERT_EQ(setenv("K_SERVICE", "test", 1), 0);
  EXPECT_EQ(PlatformInfo::Create(root_).cloud, CloudProvider::kAws);
  ASSERT_EQ(setenv("RAILWAY_ENVIRONMENT", "test", 1), 0);
  EXPECT_EQ(PlatformInfo::Create(root_).cloud, CloudProvider::kRailway);
}

TEST_F(PlatformInfoTest, VirtualizationEvidenceAndUnknown) {
  auto info = PlatformInfo::Create(root_);
  EXPECT_EQ(info.cloud, CloudProvider::kUnknown);
  EXPECT_EQ(info.virtualization, Virtualization::kUnknown);
  ASSERT_FALSE(info.cpu);
  EXPECT_EQ(info.cpu.error(), (std::error_code{ENOENT, std::system_category()}));

  WriteCpu("flags : fpu\n");
  info = PlatformInfo::Create(root_);
  ASSERT_TRUE(info.cpu);
  EXPECT_EQ(info.cpu->hypervisor, io::HypervisorStatus::kAbsent);
  EXPECT_EQ(info.virtualization, Virtualization::kUnknown);

  WriteCpu("flags : fpu hypervisor\n");
  info = PlatformInfo::Create(root_);
  EXPECT_EQ(info.virtualization, Virtualization::kOther);
}

TEST_F(PlatformInfoTest, DeploymentPrecedence) {
  EXPECT_EQ(PlatformInfo::Create(root_).deployment, DeploymentEnv::kOther);
  ASSERT_EQ(setenv("INVOCATION_ID", "test", 1), 0);
  EXPECT_EQ(PlatformInfo::Create(root_).deployment, DeploymentEnv::kSystemd);
  ASSERT_EQ(setenv("container", "test", 1), 0);
  EXPECT_EQ(PlatformInfo::Create(root_).deployment, DeploymentEnv::kContainer);
  ASSERT_EQ(unsetenv("container"), 0);

  io::WriteStringToFileOrDie("\n", root_ + "/run/.containerenv");
  EXPECT_EQ(PlatformInfo::Create(root_).deployment, DeploymentEnv::kContainer);
  io::WriteStringToFileOrDie("\n", root_ + "/.dockerenv");
  EXPECT_EQ(PlatformInfo::Create(root_).deployment, DeploymentEnv::kDocker);
  ASSERT_EQ(setenv("KUBERNETES_SERVICE_HOST", "test", 1), 0);
  EXPECT_EQ(PlatformInfo::Create(root_).deployment, DeploymentEnv::kKubernetes);
}

TEST_F(PlatformInfoTest, MarkerReadErrorsAreRetained) {
  auto info = PlatformInfo::Create(root_);
  ASSERT_TRUE(info.docker_marker);
  EXPECT_FALSE(*info.docker_marker);
  ASSERT_TRUE(info.container_marker);
  EXPECT_FALSE(*info.container_marker);

  std::string marker = root_ + "/.dockerenv";
  ASSERT_EQ(symlink(".dockerenv", marker.c_str()), 0);
  absl::Cleanup remove_marker = [&] { EXPECT_EQ(unlink(marker.c_str()), 0); };
  info = PlatformInfo::Create(root_);
  ASSERT_FALSE(info.docker_marker);
  EXPECT_EQ(info.docker_marker.error(), (std::error_code{ELOOP, std::system_category()}));
  EXPECT_EQ(info.deployment, DeploymentEnv::kOther);
}

}  // namespace
}  // namespace util::cloud
