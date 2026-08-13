// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/pthread_utils.h"

#include "base/logging.h"

#if defined(__APPLE__) && defined(__MACH__)
#define _MAC_OS_ 1
#endif

#if defined(__FreeBSD__)
#include <pthread_np.h>
#endif

namespace base {

#if defined(__linux__) || defined(__FreeBSD__)
static cpu_set_t SingleCpuSet(int cpu) {
  cpu_set_t cps;
  CPU_ZERO(&cps);
  CPU_SET(cpu, &cps);
  return cps;
}
#endif

static void* start_cpp_function(void* arg) {
  std::function<void()>* fp = (std::function<void()>*)arg;
  CHECK(*fp);
  (*fp)();
  delete fp;

  return nullptr;
}

void InitCondVarWithClock(clockid_t clock_id, pthread_cond_t* var) {
  pthread_condattr_t attr;
  PTHREAD_CHECK(condattr_init(&attr));
#ifndef _MAC_OS_
  PTHREAD_CHECK(condattr_setclock(&attr, clock_id));
#endif
  PTHREAD_CHECK(cond_init(var, &attr));
  PTHREAD_CHECK(condattr_destroy(&attr));
}

pthread_t StartThread(const char* name, void* (*start_routine)(void*), void* arg,
                      int cpu_affinity) {
  CHECK_LT(strlen(name), 16U);

  pthread_attr_t attrs;
  PTHREAD_CHECK(attr_init(&attrs));
  PTHREAD_CHECK(attr_setstacksize(&attrs, kThreadStackSize));

#if defined(__GLIBC__) || defined(__FreeBSD__)
  // musl (e.g. Alpine) lacks pthread_attr_setaffinity_np, so it falls back below instead.
  if (cpu_affinity >= 0) {
    cpu_set_t cps = SingleCpuSet(cpu_affinity);
    int rc = pthread_attr_setaffinity_np(&attrs, sizeof(cps), &cps);
    CHECK_EQ(0, rc) << "Could not set affinity attr to cpu " << cpu_affinity << ": "
                    << strerror(rc);
  }
#elif !defined(__linux__)
  (void)cpu_affinity;
#endif

  pthread_t result;
  VLOG(1) << "Starting thread " << name;

  PTHREAD_CHECK(create(&result, &attrs, start_routine, arg));

#if defined(__linux__) && !defined(__GLIBC__)
  // Fallback for musl: no pre-creation affinity API, so pin after creation instead.
  if (cpu_affinity >= 0) {
    cpu_set_t cps = SingleCpuSet(cpu_affinity);
    int rc = pthread_setaffinity_np(result, sizeof(cps), &cps);
    CHECK_EQ(0, rc) << "Could not set affinity to cpu " << cpu_affinity << ": " << strerror(rc);
  }
#endif

#ifndef _MAC_OS_
  int my_err = pthread_setname_np(result, name);
  if (my_err != 0) {
    LOG(WARNING) << "Could not set name on thread " << result << " : " << strerror(my_err);
  }
#endif
  PTHREAD_CHECK(attr_destroy(&attrs));
  return result;
}

pthread_t StartThread(const char* name, std::function<void()> f, int cpu_affinity) {
  return StartThread(name, start_cpp_function, new std::function<void()>(std::move(f)),
                     cpu_affinity);
}

}  // namespace base
