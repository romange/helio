// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <limits.h>  // PTHREAD_STACK_MIN
#include <pthread.h>

#include <functional>

#include "base/logging.h"

#define PTHREAD_CHECK(x)                                          \
  do {                                                            \
    int my_err = pthread_##x;                                     \
    CHECK_EQ(0, my_err) << #x << ", error: " << strerror(my_err); \
  } while (false)

namespace base {

constexpr size_t kThreadStackSize = 1 << 18;

void InitCondVarWithClock(clockid_t clock_id, pthread_cond_t* var);

// See numa(7) ("MEMORY POLICY" / local allocation aka first-touch).
// cpu_affinity: if >= 0, pins the thread to that cpu id before it starts running, avoiding a
// first-touch NUMA placement race on whatever the thread allocates early on. Falls back to
// pinning after creation on libcs without pthread_attr_setaffinity_np(3), e.g. musl.
pthread_t StartThread(const char* name, void* (*start_routine)(void*), void* arg,
                      int cpu_affinity = -1);
pthread_t StartThread(const char* name, std::function<void()> f, int cpu_affinity = -1);

}  // namespace base
