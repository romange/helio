// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <functional>

#include "base/varz_node.h"
#include "util/proactor_pool.h"  // Kept for users that rely on it transitively.

namespace util {

class VarzFunction : public base::VarzListNode {
 public:
  typedef AnyValue::Map KeyValMap;
  typedef std::function<KeyValMap()> MapCb;

  explicit VarzFunction(const char* varname, MapCb cb) : VarzListNode(varname), cb_(cb) {
  }

 private:
  AnyValue GetData() const override;

  MapCb cb_;
};

}  // namespace util
