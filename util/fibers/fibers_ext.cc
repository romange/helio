// Copyright 2019, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/fibers/fibers_ext.h"

#include "base/logging.h"

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

Barrier::Barrier(std::size_t initial) : initial_{initial}, current_{initial_} {
  DCHECK_NE(0, initial);
}

bool Barrier::Wait() {
  std::unique_lock<boost::fibers::mutex> lk{mtx_};
  const std::size_t cycle = cycle_;
  if (0 == --current_) {
    ++cycle_;
    current_ = initial_;
    lk.unlock();  // no pessimization
    cond_.notify_all();
    return true;
  }

  cond_.wait(
      lk, [&] { return (cycle != cycle_) || (cycle_ == std::numeric_limits<std::size_t>::max()); });
  return false;
}

void Barrier::Cancle() {
  {
    std::lock_guard lg{mtx_};
    cycle_ = std::numeric_limits<std::size_t>::max();
  }
  cond_.notify_all();
}

}  // namespace fibers_ext
}  // namespace util
