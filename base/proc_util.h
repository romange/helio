// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef PROC_STATUS_H
#define PROC_STATUS_H

#pragma once

#include <ostream>

namespace base {

struct ProcessStats {
  size_t vm_peak = 0;
  size_t vm_rss = 0;
  size_t vm_size = 0;

  // Start time of the process in seconds since epoch.
  int64_t start_time_seconds = 0;

  static ProcessStats Read();
};

namespace sys {
  struct KernelVersion {
    unsigned kernel;
    unsigned major;
    unsigned minor;
    unsigned patch;
  };

  void GetKernelVersion(KernelVersion* version);
}  // namespace sys

// Runs sh with the command. Returns 0 if succeeded. Child status is ignored.
int sh_exec(const char* cmd);

}  // namespace base

std::ostream& operator<<(std::ostream& os, const base::ProcessStats& stats);

#endif  // PROC_STATUS_H
