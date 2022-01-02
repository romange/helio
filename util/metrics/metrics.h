// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include "util/metrics/family.h"

namespace util {

class ProactorPool;

namespace metrics {

namespace detail {

class Metric {
 public:
  Metric() : val_(0) {
  }

  void IncBy(double val) {
    val_ += val;
  }

  double val() const {
    return val_;
  };

  void set_val(double v) {
    val_ = v;
  }

 private:
  double val_;
};

class SingleFamily : public Family {
 public:
  SingleFamily(const char* name, const char* help) : Family(name, help) {
  }

  void Init(ProactorPool* pp, std::initializer_list<Label> list);
  void Shutdown();

  void Inc(absl::Span<const std::string_view> label_values) {
    IncBy(label_values, 1);
  }

  void IncBy(absl::Span<const std::string_view> label_values, double val);

 protected:
  DenseId GetDenseId(unsigned thread_index, absl::Span<const std::string_view> label_values);
  void Combine(unsigned thread_index, absl::Span<double> dest) const final;

  struct PerThread {
    LabelMap label_map;  // mapping from hash(label_values) to dense_id controlled by label_values_
    std::vector<detail::Metric> metric_vec;  // map from dense_id to Counter.
  };

  // array of cardinality ProactorPool::size() and each proactor thread accesses its
  // own per-thread instance.
  std::unique_ptr<PerThread[]> per_thread_;
};

}  // namespace detail

class CounterFamily : public detail::SingleFamily {
 public:
  CounterFamily(const char* name, const char* help) : detail::SingleFamily(name, help) {
    metric_type_ = COUNTER;
  }
};

class GaugeFamily : public detail::SingleFamily {
 public:
  GaugeFamily(const char* name, const char* help) : detail::SingleFamily(name, help) {
    metric_type_ = GAUGE;
  }

  void Dec(absl::Span<const std::string_view> label_values) {
    IncBy(label_values, -1);
  }

  void DecBy(absl::Span<const std::string_view> label_values, double val) {
    IncBy(label_values, -val);
  }

  void Set(absl::Span<const std::string_view> label_values, double val);
};

// TODO: for overview of Histogram vs Summaries see
// https://prometheus.io/docs/practices/histograms/#quantiles and
// https://www.robustperception.io/how-does-a-prometheus-histogram-work
// Seems like histograms are better choice for efficient server side implementation.
// Summaries are implemented in golang based on
// http://dimacs.rutgers.edu/~graham/pubs/slides/bquant-long.pdf
//

}  // namespace metrics
}  // namespace util
