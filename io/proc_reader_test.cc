// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "io/proc_reader.h"

#include <absl/strings/str_cat.h>

#include <filesystem>

#include "base/gtest.h"
#include "io/file.h"
#include "io/file_util.h"

namespace io {
namespace {

class ProcReaderTest : public testing::Test {
 protected:
  void SetUp() override {
    root_ = base::GetTestTempPath(testing::UnitTest::GetInstance()->current_test_info()->name());
    std::error_code ec;
    std::filesystem::create_directories(root_ + "/proc", ec);
    ASSERT_FALSE(ec) << ec;
    std::filesystem::create_directories(root_ + "/sys/class/dmi/id", ec);
    ASSERT_FALSE(ec) << ec;
  }

  void WriteCpu(std::string_view content) {
    WriteStringToFileOrDie(content, root_ + "/proc/cpuinfo");
  }

  std::string root_;
};

TEST_F(ProcReaderTest, CpuLargeFileAndLongFlags) {
  std::string content = "\nno delimiter\nprocessor : 0\nmodel name : ";
  content.append(9000, 'x');
  content += "\n\nflags\t : ";
  for (unsigned i = 0; i < 1000; ++i)
    content += "fpu sse ";
  content += "hypervisor\n\nprocessor : 1\nflags : fpu\n";
  ASSERT_GT(content.size(), 4096);
  WriteCpu(content);

  auto info = ReadCpuInfo(root_ + "/proc/cpuinfo");
  ASSERT_TRUE(info) << info.error();
  EXPECT_EQ(info->hypervisor, HypervisorStatus::kPresent);
}

TEST_F(ProcReaderTest, CpuFinalLineAndExactTokens) {
  WriteCpu("processor : 0\nflags\t: fpu\thypervisor_suffix not_hypervisor");
  auto info = ReadCpuInfo(root_ + "/proc/cpuinfo");
  ASSERT_TRUE(info) << info.error();
  EXPECT_EQ(info->hypervisor, HypervisorStatus::kAbsent);

  WriteCpu("flags : fpu\thypervisor");
  info = ReadCpuInfo(root_ + "/proc/cpuinfo");
  ASSERT_TRUE(info) << info.error();
  EXPECT_EQ(info->hypervisor, HypervisorStatus::kPresent);
}

TEST_F(ProcReaderTest, CpuMissingFlagsAreUnknown) {
  WriteCpu("processor : 0\nFeatures : fp asimd\nflags : \t\n");
  auto info = ReadCpuInfo(root_ + "/proc/cpuinfo");
  ASSERT_TRUE(info) << info.error();
  EXPECT_EQ(info->hypervisor, HypervisorStatus::kUnknown);
}

TEST_F(ProcReaderTest, CpuUsesFirstFlagsRecord) {
  WriteCpu("flags : fpu\n\nprocessor : 1\nflags : hypervisor\n");
  auto info = ReadCpuInfo(root_ + "/proc/cpuinfo");
  ASSERT_TRUE(info) << info.error();
  EXPECT_EQ(info->hypervisor, HypervisorStatus::kAbsent);
}

TEST_F(ProcReaderTest, CpuReadErrors) {
  auto info = ReadCpuInfo(root_ + "/proc/cpuinfo");
  ASSERT_FALSE(info);
  EXPECT_EQ(info.error(), (std::error_code{ENOENT, std::system_category()}));

  info = ReadCpuInfo(root_ + "/proc");
  ASSERT_FALSE(info);
  EXPECT_EQ(info.error(), (std::error_code{EISDIR, std::system_category()}));
}

TEST_F(ProcReaderTest, DmiTrimsAttributesAndRetainsErrors) {
  std::string directory = root_ + "/sys/class/dmi/id/";
  WriteStringToFileOrDie(" \tMicrosoft Corporation\n", directory + "sys_vendor");
  WriteStringToFileOrDie("\nVirtual Machine\r\n", directory + "product_name");
  WriteStringToFileOrDie("  tag \n", directory + "chassis_asset_tag");
  WriteStringToFileOrDie("bios\n", directory + "bios_version");
  auto info = ReadDmiInfo(directory);
  ASSERT_TRUE(info.sys_vendor);
  EXPECT_EQ(*info.sys_vendor, "Microsoft Corporation");
  ASSERT_TRUE(info.product_name);
  EXPECT_EQ(*info.product_name, "Virtual Machine");
  ASSERT_TRUE(info.chassis_asset_tag);
  EXPECT_EQ(*info.chassis_asset_tag, "tag");
  ASSERT_TRUE(info.bios_version);
  EXPECT_EQ(*info.bios_version, "bios");

  ASSERT_TRUE(Delete(directory + "chassis_asset_tag"));
  std::error_code ec;
  std::filesystem::create_directory(directory + "chassis_asset_tag", ec);
  ASSERT_FALSE(ec) << ec;
  ASSERT_TRUE(Delete(directory + "bios_version"));
  info = ReadDmiInfo(directory);
  EXPECT_TRUE(info.sys_vendor);
  EXPECT_TRUE(info.product_name);
  ASSERT_FALSE(info.chassis_asset_tag);
  EXPECT_EQ(info.chassis_asset_tag.error(), (std::error_code{EISDIR, std::system_category()}));
  ASSERT_FALSE(info.bios_version);
  EXPECT_EQ(info.bios_version.error(), (std::error_code{ENOENT, std::system_category()}));
}

#ifdef __linux__
TEST_F(ProcReaderTest, ReadLiveCpuInfo) {
  auto info = ReadCpuInfo();
  ASSERT_TRUE(info) << info.error();
}
#endif

}  // namespace
}  // namespace io
