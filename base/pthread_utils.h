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

// cpu_affinity: if >= 0, the thread is created already pinned to that cpu id
// (via pthread_attr_setaffinity_np(3)), avoiding the race of pinning a thread
// after it has already started running.
//
// Rationale: pthread_setaffinity_np(3) on an already-running thread only
// forces the kernel to migrate its *execution* to an allowed cpu
// (sched_setaffinity(2): "If the thread ... is not currently running on one
// of the CPUs specified in mask, then it is migrated to one of those CPUs").
// It does NOT relocate memory the thread already faulted in. Linux's default
// NUMA memory policy is "local allocation" a.k.a. first-touch (see numa(7),
// "MEMORY POLICY" / "local allocation"): a page is placed on the node of the
// CPU that faults it in, at fault time. So anything the thread touches
// before the affinity call lands (e.g. thread-local heap arenas, or in our
// proactor case the io_uring SQ/CQ ring pages allocated in
// UringProactor::Init) can get first-touched on whatever node the thread
// happened to start on, and stays there for the life of the process unless
// AutoNUMA balancing (see Documentation/admin-guide/sysctl/kernel.rst,
// numa_balancing) opportunistically migrates it later. Setting affinity in
// the pthread_attr_t before pthread_create(3) avoids the window entirely.
pthread_t StartThread(const char* name, void* (*start_routine)(void*), void* arg,
                       int cpu_affinity = -1);
pthread_t StartThread(const char* name, std::function<void()> f, int cpu_affinity = -1);

}  // namespace base
