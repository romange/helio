// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "base/logging.h"
#include "base/pthread_utils.h"

#if defined(__APPLE__) && defined(__MACH__)
#define _MAC_OS_ 1
#endif

namespace base {

static void* start_cpp_function(void *arg) {
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


pthread_t StartThread(const char* name, void *(*start_routine) (void *), void *arg) {
  CHECK_LT(strlen(name), 16U);

  pthread_attr_t attrs;
  PTHREAD_CHECK(attr_init(&attrs));
  PTHREAD_CHECK(attr_setstacksize(&attrs, kThreadStackSize));


  pthread_t result;
  VLOG(1) << "Starting thread " << name;

  PTHREAD_CHECK(create(&result, &attrs, start_routine, arg));
#ifndef _MAC_OS_
  int my_err = pthread_setname_np(result, name);
  if (my_err != 0) {
    LOG(WARNING) << "Could not set name on thread " << result << " : " << strerror(my_err);
  }
#endif
  PTHREAD_CHECK(attr_destroy(&attrs));
  return result;
}

pthread_t StartThread(const char* name, std::function<void()> f) {
  return StartThread(name, start_cpp_function, new std::function<void()>(std::move(f)));
}

}  // namespace base
