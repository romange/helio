// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include "util/varz.h"
#include "base/logging.h"

using namespace boost;

using base::VarzValue;
using namespace std;

namespace util {

VarzValue VarzQps::GetData() const {
  uint32_t qps = val_.SumTail() / (Counter::WIN_SIZE - 1);  // Average over kWinSize values.
  return VarzValue::FromInt(qps);
}

VarzMapAverage::~VarzMapAverage() {
}

void VarzMapAverage::Init(ProactorPool* pp) {
  CHECK(pp_ == nullptr);
  pp_ = CHECK_NOTNULL(pp);
  avg_map_.reset(new Map[pp->size()]);
}

void VarzMapAverage::Shutdown() {
  avg_map_.reset();
  pp_ = nullptr;
}


unsigned VarzMapAverage::ProactorThreadIndex() const {
  unsigned tnum = CHECK_NOTNULL(pp_)->size();

  int32_t indx = ProactorBase::GetIndex();
  CHECK_GE(indx, 0) << "Must be called from proactor thread!";
  CHECK_LT(unsigned(indx), tnum) << "Invalid thread index " << indx;

  return unsigned(indx);
}

auto VarzMapAverage::FindSlow(string_view key) -> Map::iterator {
  auto str = pp_->GetString(key);
  auto& map = avg_map_[ProactorBase::GetIndex()];
  auto res = map.emplace(str, SumCnt{});

  CHECK(res.second);
  return res.first;
}

VarzValue VarzMapAverage::GetData() const {
  CHECK(pp_);

  AnyValue::Map result;
  fibers::mutex mu;

  auto cb = [&](unsigned index, auto*) {
    auto& map = avg_map_[index];

    for (const auto& k_v : map) {
      AnyValue::Map items;
      int64 count = k_v.second.second.Sum();
      int64 sum = k_v.second.first.Sum();
      items.emplace_back("count", VarzValue::FromInt(count));
      items.emplace_back("sum", VarzValue::FromInt(sum));

      if (count) {
        double avg = count > 0 ? double(sum) / count : 0;
        items.emplace_back("average", VarzValue::FromDouble(avg));
      }

      lock_guard lk(mu);
      result.emplace_back(string(k_v.first), std::move(items));
    }
  };
  pp_->AwaitFiberOnAll(cb);

  return result;
}

VarzValue VarzFunction::GetData() const {
  AnyValue::Map result = cb_();
  return AnyValue(result);
}

void VarzCount::Init(ProactorPool* pp) {
  CHECK_NOTNULL(pp);
}


void VarzCount::Shutdown() {
}

VarzValue VarzCount::GetData() const {
  AnyValue res = VarzValue::FromInt(count_.load(memory_order_relaxed));
  return res;
}

VarzCount::~VarzCount() {
}

}  // namespace util
