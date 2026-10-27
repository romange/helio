// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include "util/varz.h"

#include "base/logging.h"

using base::VarzValue;
using namespace std;

namespace util {

VarzValue VarzFunction::GetData() const {
  AnyValue::Map result = cb_();
  return AnyValue(result);
}

}  // namespace util
