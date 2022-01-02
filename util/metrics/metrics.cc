// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/metrics/metrics.h"

#include "base/hash.h"
#include "base/logging.h"
#include "util/proactor_pool.h"

namespace util {
namespace metrics {
using namespace std;

namespace {
thread_local XXH3_state_t xxh3_state;
}

namespace detail {

void SingleFamily::Init(ProactorPool* pp, initializer_list<Label> list) {
  InitBase(pp, list);
  per_thread_.reset(new PerThread[pp->size()]);
}

void SingleFamily::Shutdown() {
  ShutdownBase();
  per_thread_.reset();
}

void SingleFamily::IncBy(absl::Span<const std::string_view> labels, double val) {
  CHECK_EQ(label_names_.size(), labels.size());
  int32_t index = ProactorBase::GetIndex();
  if (index < 0)  // not in proactor thread, silently exit.
    return;

  DenseId dense_id = GetDenseId(index, labels);
  per_thread_[index].metric_vec[dense_id].IncBy(val);
}

void SingleFamily::Combine(unsigned thread_index, absl::Span<double> dest) const {
  const PerThread& pt = per_thread_[thread_index];
  // there may be case that pt.metric_vec is smaller or larger that dest, because family
  // metrics could grow since they were snapshoted or because current thread does not
  // have all the tuples present in central data-structures.
  // however we can merge the minimum indices and this will be correct (eventually consistent).
  size_t sz = min(dest.size(), pt.metric_vec.size());
  for (size_t i = 0; i < sz; ++i) {
    dest[i] += pt.metric_vec[i].val();
  }
}

auto SingleFamily::GetDenseId(unsigned thread_index,
                              absl::Span<const std::string_view> label_values) -> DenseId {
  // Assume that there is 0 chance that different labels will have the same 64bit hash.
  // In practice it's not zero. Don't do it if you design a banking system or a space ship.
  // For metrics I assume it's fine - noone will die.
  XXH3_64bits_reset(&xxh3_state);
  for (auto s : label_values) {
    XXH3_64bits_update(&xxh3_state, s.data(), s.size());
  }
  XXH64_hash_t hash = XXH3_64bits_digest(&xxh3_state);
  auto [dense_id, inserted] = Emplace(hash, label_values, &per_thread_[thread_index].label_map);
  if (inserted) {
    per_thread_[thread_index].metric_vec.resize(dense_id + 1);
  }
  return dense_id;
}

}  // namespace detail

void GaugeFamily::Set(absl::Span<const std::string_view> label_values, double val) {
  CHECK_EQ(label_names_.size(), label_values.size());
  int32_t index = ProactorBase::GetIndex();
  if (index < 0)  // not in proactor thread, silently exit.
    return;

  DenseId dense_id = GetDenseId(index, label_values);
  per_thread_[index].metric_vec[dense_id].set_val(val);
}

}  // namespace metrics
}  // namespace util
