// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

namespace util {
namespace detail {

template <typename R> class ResultMover {
  R r_;  // todo: to set as optional to support objects without default c'tor.
 public:
  template <typename Func> void Apply(Func&& f) {
    r_ = f();
  }

  // Returning rvalue-reference means returning the same object r_ instead of creating a
  // temporary R{r_}. Please note that when we return function-local object, we do not need to
  // return rvalue because RVO eliminates redundant object creation.
  // But for returning data member r_ it's more efficient.
  // "get() &&" means you can call this function only on rvalue ResultMover&& object.
  R&& get() && {
    return std::forward<R>(r_);
  }
};

template <> class ResultMover<void> {
 public:
  template <typename Func> void Apply(Func&& f) {
    f();
  }
  void get() {
  }
};

}  // namespace detail

}  // namespace util
