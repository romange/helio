// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/proc_util.h"

#include <spawn.h>
#include <sys/utsname.h>
#include <sys/wait.h>
#include <unistd.h>


#include "base/integral_types.h"
#include "base/logging.h"

// for MacOS we declare explicitly.
extern "C" char **environ;

namespace base {


// Runs a child process efficiently.
// argv must be null terminated array of arguments where the first item in the array is the base
// name of the executable passed by path.
// Returns 0 if child process run successfully and updates child_status with its return status.
static int spawn(const char* path, char* argv[], int* child_status) {
  pid_t pid;
  // posix_spawn in this configuration calls vfork without duplicating parents
  // virtual memory for the child. That's all we want.
  int status = posix_spawn(&pid, path, NULL, NULL, argv, environ);
  if (status != 0)
    return status;

  if (waitpid(pid, child_status, 0) == -1)
    return errno;
  return 0;
}

namespace sys {

void GetKernelVersion(KernelVersion* version) {
  struct utsname buffer;

  CHECK_EQ(0, uname(&buffer));

  int res = sscanf(buffer.release, "%u.%u.%u-%u", &version->kernel, &version->major,
                   &version->minor, &version->patch);
  CHECK(res == 3 || res == 4) << res;
}

}  // namespace sys

int sh_exec(const char* cmd) {
  char sh_bin[] = "sh";
  char arg1[] = "-c";

  std::string cmd2(cmd);

  char* argv[] = {sh_bin, arg1, &cmd2.front(), NULL};
  int child_status = 0;
  return spawn("/bin/sh", argv, &child_status);
}

}  // namespace base

