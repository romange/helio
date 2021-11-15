// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <functional>
#include <string>

#include "base/RWSpinLock.h"
#include "base/varz_value.h"

namespace base {

class VarzListNode {
 public:
  typedef base::VarzValue AnyValue;

  explicit VarzListNode(const char* name);
  virtual ~VarzListNode();

  // New interface. Func is a function accepting 'const char*' and VarzValue&&.
  static void Iterate(std::function<void(const char*, AnyValue&&)> f);

  // Old interface. Appends string representations of each active node in the list to res.
  // Used for outputting the current state.
  static void IterateValues(std::function<void(const std::string&, const std::string&)> cb) {
    Iterate([&](const char* name, AnyValue&& av) { cb(name, Format(av)); });
  }

 protected:
  virtual AnyValue GetData() const = 0;

  const char* name_;

  static std::string Format(const AnyValue& av);

 private:
  // Returns the head to varz linked list. Note that the list becomes invalid after at least one
  // linked list node was destroyed.
  static VarzListNode*& global_list();
  static folly::RWSpinLock g_varz_lock;

  VarzListNode* next_;
  VarzListNode* prev_;
};

}  // namespace base
