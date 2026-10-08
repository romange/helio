// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/cloud/platform_info.h"

#include <absl/strings/ascii.h>
#include <absl/strings/match.h>
#include <absl/strings/str_cat.h>
#include <sys/stat.h>

#include <cstdlib>

namespace util::cloud {

using namespace std;
using nonstd::make_unexpected;

namespace {

io::Result<bool> HasFile(const string& path) {
  struct stat sb;
  if (stat(path.c_str(), &sb) == 0)
    return true;
  if (errno == ENOENT || errno == ENOTDIR)
    return false;
  return make_unexpected(error_code{errno, system_category()});
}

string_view DmiValue(const io::Result<string>& attribute) {
  return attribute ? string_view(*attribute) : string_view{};
}

CloudProvider DetectCloud(const io::DmiInfo& dmi) {
  if (getenv("RAILWAY_ENVIRONMENT"))
    return CloudProvider::kRailway;

  string_view vendor = DmiValue(dmi.sys_vendor);
  string_view product = DmiValue(dmi.product_name);
  string_view asset_tag = DmiValue(dmi.chassis_asset_tag);
  string bios = absl::AsciiStrToLower(DmiValue(dmi.bios_version));

  if (absl::StartsWith(vendor, "Amazon") || absl::StrContains(bios, "amazon"))
    return CloudProvider::kAws;
  if (absl::StartsWith(vendor, "Google") || product == "Google Compute Engine")
    return CloudProvider::kGcp;
  if (asset_tag == "7783-7084-3265-9085-8269-3286-77")
    return CloudProvider::kAzure;
  if (asset_tag == "OracleCloud.com")
    return CloudProvider::kOracle;
  if (vendor == "Hetzner")
    return CloudProvider::kHetzner;
  if (absl::StartsWith(product, "OpenStack"))
    return CloudProvider::kOpenStack;

  if (getenv("AWS_EXECUTION_ENV") || getenv("ECS_CONTAINER_METADATA_URI_V4"))
    return CloudProvider::kAws;
  if (getenv("K_SERVICE"))
    return CloudProvider::kGcp;
  if (getenv("CONTAINER_APP_NAME"))
    return CloudProvider::kAzure;
  return CloudProvider::kUnknown;
}

Virtualization DetectVirtualization(const io::DmiInfo& dmi, const io::Result<io::CpuInfo>& cpu) {
  string_view vendor = DmiValue(dmi.sys_vendor);
  string_view product = DmiValue(dmi.product_name);
  if (absl::StartsWith(vendor, "VMware"))
    return Virtualization::kVmware;
  if (vendor == "QEMU" || absl::StrContains(product, "KVM"))
    return Virtualization::kKvm;
  if (absl::StartsWith(vendor, "Microsoft"))
    return Virtualization::kHyperV;
  if (vendor == "Xen")
    return Virtualization::kXen;
  if (cpu && cpu->hypervisor == io::HypervisorStatus::kPresent)
    return Virtualization::kOther;
  return Virtualization::kUnknown;
}

}  // namespace

PlatformInfo PlatformInfo::Create(string_view root) {
  PlatformInfo info;
  info.cpu = io::ReadCpuInfo(absl::StrCat(root, "/proc/cpuinfo"));
  info.dmi = io::ReadDmiInfo(absl::StrCat(root, "/sys/class/dmi/id"));
  info.docker_marker = HasFile(absl::StrCat(root, "/.dockerenv"));
  info.container_marker = HasFile(absl::StrCat(root, "/run/.containerenv"));
  info.cloud = DetectCloud(info.dmi);
  info.virtualization = DetectVirtualization(info.dmi, info.cpu);

  if (getenv("KUBERNETES_SERVICE_HOST"))
    info.deployment = DeploymentEnv::kKubernetes;
  else if (info.docker_marker && *info.docker_marker)
    info.deployment = DeploymentEnv::kDocker;
  else if ((info.container_marker && *info.container_marker) || getenv("container"))
    info.deployment = DeploymentEnv::kContainer;
  else if (getenv("INVOCATION_ID"))
    info.deployment = DeploymentEnv::kSystemd;
  return info;
}

}  // namespace util::cloud
