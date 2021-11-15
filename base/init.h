// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include "base/flags.h"
#include "base/logging.h"

namespace __internal__ {

class ModuleInitializer {
 public:
  typedef void (*VoidFunction)(void);

  ModuleInitializer(VoidFunction ctor, bool is_ctor);

  static void RunFtors(bool is_ctor);

 private:
  struct CtorNode {
    VoidFunction func;
    CtorNode* next;

    bool is_ctor;
  } __attribute__((packed));

  CtorNode node_;

  static CtorNode* & global_list();

  ModuleInitializer(const ModuleInitializer&) = delete;
  void operator=(const ModuleInitializer&) = delete;
};

}  // __internal__

#define REGISTER_MODULE_INITIALIZER(name, body)             \
  namespace {                                               \
    static void google_init_module_##name () { body; }      \
    __internal__::ModuleInitializer google_initializer_module_##name(     \
            google_init_module_##name, true);            \
  }

#define REGISTER_MODULE_DESTRUCTOR(name, body)                  \
  namespace {                                                   \
    static void google_destruct_module_##name () { body; }      \
    __internal__::ModuleInitializer google_destructor_module_##name( \
        google_destruct_module_##name, false);               \
  }

class MainInitGuard {
 public:
   MainInitGuard(int* argc, char*** argv, uint32_t flags = 0);

   ~MainInitGuard();
};

#define MainInitGuard(x, y) static_assert(false, "Forgot variable name")
