// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "util/metrics/family.h"

#include <shared_mutex>

#include "base/logging.h"
#include "util/proactor_pool.h"

namespace util {
namespace metrics {
using namespace std;

namespace {

shared_mutex list_mu;
Family* family_list = nullptr;

}  // namespace

Family::Family(const char* name, const char* help) : name_(name), help_(help) {
}

Family::~Family() {
  lock_guard lk(list_mu);
  if (next_)
    next_->prev_ = prev_;

  if (prev_)
    prev_->next_ = next_;
  else if (family_list == this)
    family_list = next_;
}

void Family::InitBase(ProactorPool* pp, initializer_list<Label> list) {
  CHECK(pp && pp_ == nullptr);
  label_names_ = list;
  pp_ = pp;

  lock_guard lk(list_mu);
  next_ = family_list;
  if (next_) {
    next_->prev_ = this;
  }
  family_list = this;
}

void Family::ShutdownBase() {
  lock_guard lk(list_mu);
  if (next_)
    next_->prev_ = prev_;

  if (prev_)
    prev_->next_ = next_;
  else if (family_list == this)
    family_list = next_;
  next_ = prev_ = nullptr;
  pp_ = nullptr;

  label_values_.clear();
  label_values_.shrink_to_fit();
  label_names_.clear();
  label_names_.shrink_to_fit();
  label_map_.clear();
  label_map_.rehash(0);
}

auto Family::Emplace(uint64_t hash, absl::Span<const std::string_view> label_values,
                     LabelMap* label_map) -> pair<DenseId, bool> {
  auto it = label_map->find(hash);

  if (it != label_map->end())  // quick path
    return make_pair(it->second, false);

  // opportunistically create LabelValues outside the lock even if at the end we won't add it.
  LabelValues lvals(new string_view[label_values.size()]);
  for (size_t i = 0; i < label_values.size(); ++i) {
    lvals[i] = pp_->GetString(label_values[i]);
  }

  lock_guard lk(mu_);

  bool inserted;
  DenseId dense_id = label_values_.size();

  // verify again because we have preempted on mu_, therefore other fibers could
  // already update local_map.
  tie(it, inserted) = label_map->emplace(hash, dense_id);
  if (inserted) {
    auto [global_it, inserted] = label_map_.emplace(hash, dense_id);
    if (inserted) {  // new hash value
      label_values_.emplace_back(std::move(lvals));
    } else {
      it->second = global_it->second;  // update the local map.
    }
  }

  return make_pair(it->second, inserted);
}

ObservationDescriptor Family::GetDescriptor() const {
  ObservationDescriptor res;
  res.type = metric_type_;
  res.metric_name = name_;
  res.metric_help = help_;
  res.label_names = absl::Span<const Label>{label_names_};
  if (metric_type_ == SUMMARY || metric_type_ == HISTOGRAM) {
    res.adjustments = absl::Span<ObservationDescriptor::Adjustment>{nullptr, cardinality_};
  }
  return res;
}

void Iterate(MetricRowCb cb) {
  shared_lock lk(list_mu);
  if (!family_list)
    return;

  vector<ObservationDescriptor> descrs;
  uint32_t total_metrics = 0;
  uint32_t total_adjustments = 0;

  for (Family* cur = family_list; cur; cur = cur->next_) {
    ObservationDescriptor od = cur->GetDescriptor();

    cur->mu_.lock_shared();
    size_t num_tuples = cur->label_values_.size();
    od.label_values.resize(num_tuples);
    for (size_t i = 0; i < num_tuples; ++i) {
      od.label_values[i] = cur->label_values_[i].get();
    }
    cur->mu_.unlock_shared();

    total_metrics += cur->cardinality_ * num_tuples;
    descrs.push_back(std::move(od));
    total_adjustments += (cur->cardinality_ > 1) ? cur->cardinality_ : 0;
  }

  vector<ObservationDescriptor::Adjustment> adjustments(total_adjustments);
  unsigned offset = 0;
  unsigned index = 0;
  for (Family* cur = family_list; cur; cur = cur->next_) {
    if (cur->metric_type_ == SUMMARY || cur->metric_type_ == HISTOGRAM) {
      absl::Span<ObservationDescriptor::Adjustment> span{adjustments.data() + offset,
                                                         cur->cardinality_};
      descrs[index].adjustments = span;
      offset += cur->cardinality_;
    }
    ++index;
  }

  boost::fibers::mutex mu;
  vector<double> result(total_metrics, 0.0);
  auto per_thread_cb = [&](unsigned tindex, auto*) {
    uint32_t offset = 0;
    uint32_t descr_index = 0;
    for (Family* cur = family_list; cur; cur = cur->next_) {
      unsigned len = descrs[descr_index].metrics_len();

      lock_guard res_lk(mu);
      cur->Combine(tindex, absl::Span<double>{result.data() + offset, len});
      ++descr_index;
      offset += len;
    }
  };

  family_list->pp_->AwaitFiberOnAll(per_thread_cb);
  lk.unlock();

  offset = 0;
  for (const auto& descr : descrs) {
    cb(descr, absl::Span{result.data() + offset, descr.metrics_len()});
    offset += descr.metrics_len();
  }
}

}  // namespace metrics
}  // namespace util
