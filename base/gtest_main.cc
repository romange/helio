// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <dirent.h>
#include <gperftools/profiler.h>

#include "base/gtest.h"
#include "base/init.h"
#include "base/logging.h"

DEFINE_bool(bench, false, "Run benchmarks");

using namespace std;

namespace base {

static char test_path[1024] = {0};

std::string GetTestTempDir() {
  if (test_path[0] == '\0') {
    strcpy(test_path, "/tmp/XXXXXX");
    CHECK(mkdtemp(test_path)) << test_path;
    LOG(INFO) << "Creating test directory " << test_path;
  }
  return test_path;
}

std::string GetTestTempPath(const std::string& base_name) {
  string path = GetTestTempDir();
  path.append("/").append(base_name);
  return path;
}

void DeleteRecursively(const char* name) {
  // We don't care too much about error checking here since this is only used
  // in tests to delete temporary directories that are under /tmp anyway.

  // Use opendir()!  Yay!
  // lstat = Don't follow symbolic links.
  struct stat stats;
  if (lstat(name, &stats) != 0)
    return;

  if (S_ISREG(stats.st_mode)) {
    remove(name);
    return;
  }
  DIR* dir = opendir(name);
  if (dir == nullptr) {
    return;
  }
  string tmp(name);
  while (true) {
    struct dirent* entry = readdir(dir);
    if (entry == NULL)
      break;
    string entry_name = entry->d_name;
    if (entry_name != "." && entry_name != "..") {
      string item = tmp + "/" + entry_name;
      DeleteRecursively(item.c_str());
    }
  }
  closedir(dir);
  rmdir(name);
}

std::string RandStr(const unsigned len) {
  static const char alphanum[] =
      "0123456789"
      "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
      "abcdefghijklmnopqrstuvwxyz";

  string s(len, '\0');
  for (unsigned i = 0; i < len; ++i) {
    s[i] = alphanum[rand() % (sizeof(alphanum) - 1)];
  }

  s[len] = 0;

  return s;
}

string ProgramRunfilesPath() {
  return ProgramAbsoluteFileName().append(".runfiles/");
}

string ProgramRunfile(const string& relative_path) {
  return ProgramRunfilesPath().append(relative_path);
}

}  // namespace base

int main(int argc, char** argv) {
  testing::InitGoogleTest(&argc, argv);
  ProfilerEnable();  // Dummy call to force linker to use profiler lib.

  for (int i = 1; i < argc; ++i) {
    if (strcmp(argv[i], "--bench") == 0) {
      benchmark::Initialize(&argc, argv);
      break;
    }
  }

  MainInitGuard guard(&argc, &argv);

  LOG(INFO) << "Starting tests in " << argv[0];

  int res = RUN_ALL_TESTS();

  if (FLAGS_bench) {
    benchmark::RunSpecifiedBenchmarks();
  }
  if (res == 0 && base::test_path[0]) {
    base::DeleteRecursively(base::test_path);
  }
  return res;
}
