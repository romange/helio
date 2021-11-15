// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/fibers/fibers_ext.h"

namespace std {

ostream& operator<<(ostream& o, const ::boost::fibers::channel_op_status op) {
  using ::boost::fibers::channel_op_status;
  if (op == channel_op_status::success) {
    o << "success";
  } else if (op == channel_op_status::closed) {
    o << "closed";
  } else if (op == channel_op_status::full) {
    o << "full";
  } else if (op == channel_op_status::empty) {
    o << "empty";
  } else if (op == channel_op_status::timeout) {
    o << "timeout";
  }
  return o;
}

}  // namespace std

namespace util {
namespace fibers_ext {


}  // namespace fibers_ext
}  // namespace util
