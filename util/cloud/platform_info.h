// Copyright 2026, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include "io/proc_reader.h"

namespace util::cloud {

enum class CloudProvider { kUnknown, kAws, kGcp, kAzure, kOracle, kHetzner, kOpenStack, kRailway };
enum class Virtualization { kUnknown, kVmware, kKvm, kHyperV, kXen, kOther };
enum class DeploymentEnv { kOther, kKubernetes, kDocker, kContainer, kSystemd };

struct PlatformInfo {
  // Best-effort local detection; raw results retain unavailable signals and their errors.
  // Unknown virtualization includes absent or unavailable CPU hypervisor flags, not confirmed
  // metal. root prefixes absolute file paths, but environment markers describe the current process.
  ABSL_MUST_USE_RESULT static PlatformInfo Create(std::string_view root = "");

  CloudProvider cloud = CloudProvider::kUnknown;
  Virtualization virtualization = Virtualization::kUnknown;
  DeploymentEnv deployment = DeploymentEnv::kOther;

  io::Result<io::CpuInfo> cpu;
  io::DmiInfo dmi;
  io::Result<bool> docker_marker;
  io::Result<bool> container_marker;
};

}  // namespace util::cloud
