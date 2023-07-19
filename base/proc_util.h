// Copyright 2023, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#ifndef PROC_STATUS_H
#define PROC_STATUS_H

#pragma once

namespace base {

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

#endif  // PROC_STATUS_H
