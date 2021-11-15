// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/sliding_counter.h"

#include "absl/time/clock.h"
#include "base/logging.h"

using namespace std;

namespace util {
namespace detail {

void SlidingCounterBase::InitInternal(ProactorPool* pp) {
  CHECK(pp_ == nullptr);
  pp_ = CHECK_NOTNULL(pp);
}

void SlidingCounterBase::CheckInit() const {
  CHECK_NOTNULL(pp_);
}

unsigned SlidingCounterBase::ProactorThreadIndex() const {
  int32_t tnum = CHECK_NOTNULL(pp_)->size();

  int32_t indx = ProactorBase::GetIndex();
  CHECK_GE(indx, 0) << "Must be called from proactor thread!";
  CHECK_LT(indx, tnum) << "Invalid thread index " << indx;

  return unsigned(indx);
}

}  // namespace detail
}  // namespace util
