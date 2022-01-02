// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <absl/container/flat_hash_map.h>
#include <absl/types/span.h>

#include <string_view>

#include "util/fibers/fibers_ext.h"

namespace util {
class ProactorPool;

namespace metrics {

class Label {
 public:
  Label() {
  }
  Label(const char* name) : name_(name) {
  }

  std::string_view name() const {
    return name_;
  }

 private:
  std::string_view name_;
};

using LabelInstance = std::pair<Label, std::string_view>;

/*
A single invocation of MetricRowCb can provide the following FamilyObservation.
3
# HELP engine_query_duration_seconds Query timings
# TYPE engine_query_duration_seconds summary
engine_query_duration_seconds{slice="inner_eval",quantile="0.5"} 9.367e-06
engine_query_duration_seconds{slice="inner_eval",quantile="0.9"} 9.367e-06
engine_query_duration_seconds{slice="inner_eval",quantile="0.99"} 9.367e-06
engine_query_duration_seconds_sum{slice="inner_eval"} 9.367e-06
engine_query_duration_seconds_count{slice="inner_eval"} 1

*/

enum MetricType { COUNTER, GAUGE, SUMMARY, HISTOGRAM };

struct ObservationDescriptor {
  MetricType type = COUNTER;

  std::string_view metric_name;  // metric name. i.e. engine_query_duration_seconds
  std::string_view metric_help;

  absl::Span<const Label> label_names;  // aka "slice"

  // Span of metric rows induced by the label values that been observed through a single family.
  // Each label tuple (entry in the span of type "const std::string_view*") is
  // of constant length - label_names.size().
  // Example above has 1 tuple of len 1: (slice, inner_eval).
  std::vector<const std::string_view*> label_values;

  // Incremental changes to metric rows
  struct Adjustment {
    std::string_view suffix;  // optional suffix, for example, "_sum" or "_count"

    // optional label instance that we should add to this metrics.
    // defined if label.first is not empty. For example, ("quantile", "0.9")
    LabelInstance label;
  };

  // Set only for SUMMARY/HISTOGRAM types. Defines the second dimension of how metric
  // rows are produced. The total number of metric rows will be:
  // adjustments.size() * label_values.size()
  // Rows will be spawned by order (adjustment, label_value)
  absl::Span<Adjustment> adjustments;

  size_t metrics_len() const {
    return (type == HISTOGRAM || type == SUMMARY) ? adjustments.size() * label_values.size()
                                                  : label_values.size();
  }
};

// Called upon each family observation.
using MetricRowCb = std::function<void(const ObservationDescriptor&, absl::Span<const double>)>;
void Iterate(MetricRowCb cb);

class Family {
  friend void Iterate(MetricRowCb cb);

 public:
  Family(const char* name, const char* help);
  virtual ~Family();

  std::string_view name() const {
    return name_;
  }
  std::string_view help() const {
    return help_;
  }

 protected:
  using DenseId = uint32_t;
  using LabelMap = absl::flat_hash_map<uint64_t, DenseId>;

  void InitBase(ProactorPool* pp, std::initializer_list<Label> list);
  void ShutdownBase();

  std::pair<DenseId, bool> Emplace(uint64_t hash, absl::Span<const std::string_view> label_values,
                                   LabelMap* per_thread);

  ObservationDescriptor GetDescriptor() const;

  // Combines values into dest. Dest is ordered by cardinality,
  // i.e. first metric and all its tuples, then the next one and so on.
  virtual void Combine(unsigned thread_index, absl::Span<double> dest) const = 0;

  // Fills up family adjustments for SUMMARY/HISTOGRAM families.
  virtual void SetAdjustments(absl::Span<ObservationDescriptor::Adjustment> dest) const {
  }

  std::string_view name_, help_;
  std::vector<Label> label_names_;  // label names initialized once in Init().

  using LabelValues = std::unique_ptr<std::string_view[]>;

  // single store of label_values, indexed by DenseId.
  std::vector<LabelValues> label_values_;  // Guarded by mu_
  LabelMap label_map_;                     // Guarded by mu_

  fibers_ext::SharedMutex mu_;

  ProactorPool* pp_;
  Family* next_ = nullptr;
  Family* prev_ = nullptr;
  unsigned cardinality_ = 1;
  MetricType metric_type_ = COUNTER;
};

}  // namespace metrics

}  // namespace util
