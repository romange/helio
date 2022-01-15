// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <sys/types.h>
#include <vector>
#include "io/io.h"

namespace io {

struct StatShort {
  std::string name;
  time_t last_modified;  // nanoseconds since Epoch time.
  uint64_t size;
  mode_t st_mode;
};

using StatShortVec = std::vector<StatShort>;

Result<StatShortVec> StatFiles(std::string_view path);

// Create a file and write a std::string to it.
void WriteStringToFileOrDie(std::string_view contents, std::string_view name);


} //