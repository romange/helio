// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <limits.h>  // PTHREAD_STACK_MIN
#include <pthread.h>
#include <functional>
#include "base/logging.h"

#define PTHREAD_CHECK(x) \
  do { \
    int my_err = pthread_ ## x; \
    CHECK_EQ(0, my_err) << #x << ", error: " << strerror(my_err); \
  } while(false)

namespace base {

constexpr size_t kThreadStackSize = 1 << 18;

void InitCondVarWithClock(clockid_t clock_id, pthread_cond_t* var);


pthread_t StartThread(const char* name, void *(*start_routine) (void *), void *arg);
pthread_t StartThread(const char* name, std::function<void()> f);

}  // namespace base
