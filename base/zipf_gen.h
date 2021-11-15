//
//  zipfian_generator.h
//  YCSB-cpp
//
//  Copyright (c) 2020 Youngjae Lee <ls4154.lee@gmail.com>.
//  Copyright (c) 2014 Jinglei Ren <jinglei@ren.systems>.
//
// Cosmetic changes by Roman Gershman.

#ifndef YCSB_C_ZIPFIAN_GENERATOR_H_
#define YCSB_C_ZIPFIAN_GENERATOR_H_

#include <cassert>
#include <cmath>
#include <cstdint>
#include <random>

namespace base {

class ZipfianGenerator {
 public:
  static constexpr double kZipfianConst = 0.99;
  static constexpr uint64_t kMaxNumItems = (UINT64_MAX >> 24);

  ZipfianGenerator(uint64_t num_items) : ZipfianGenerator(0, num_items - 1) {
  }

  ZipfianGenerator(uint64_t min, uint64_t max, double zipfian_const = kZipfianConst)
      : ZipfianGenerator(min, max, zipfian_const, Zeta(0, max - min + 1, zipfian_const, 0)) {
  }

  ZipfianGenerator(uint64_t min, uint64_t max, double zipfian_const, double zeta_n)
      : items_(max - min + 1), base_(min), theta_(zipfian_const) {
    assert(items_ >= 2 && items_ < kMaxNumItems);

    zeta_2_ = Zeta(0, 2, theta_, 0);

    alpha_ = 1.0 / (1.0 - theta_);
    zeta_n_ = zeta_n;
    count_for_zeta_ = items_;
    eta_ = Eta();
  }

  template<typename URNG> uint64_t Next(URNG& u);

 private:

  double Eta() {
    return (1 - std::pow(2.0 / items_, 1 - theta_)) / (1 - zeta_2_ / zeta_n_);
  }

  ///
  /// Calculate the zeta constant needed for a distribution.
  /// Do this incrementally from the last_num of items to the cur_num.
  /// Use the zipfian constant as theta. Remember the new number of items
  /// so that, if it is changed, we can recompute zeta.
  ///
  static double Zeta(uint64_t last_num, uint64_t cur_num, double theta, double last_zeta) {
    double zeta = last_zeta;
    for (uint64_t i = last_num + 1; i <= cur_num; ++i) {
      zeta += 1 / std::pow(i, theta);
    }
    return zeta;
  }

  uint64_t items_;
  uint64_t base_;  /// Min number of items to generate

  // Computed parameters for generating the distribution
  double theta_, zeta_n_, eta_, alpha_, zeta_2_;
  uint64_t count_for_zeta_;  /// Number of items used to compute zeta_n
};


template<typename URNG>
uint64_t ZipfianGenerator::Next(URNG& gen) {
  std::uniform_real_distribution<double> uniform(0, 1);
  double u = uniform(gen);
  double uz = u * zeta_n_;

  double res;
  if (uz < 1.0) {
    res = base_;
  } else if (uz < 1.0 + std::pow(0.5, theta_)) {
    res = base_ + 1;
  } else {
    res = base_ + count_for_zeta_ * std::pow(eta_ * u - eta_ + 1, alpha_);
  }
  return res;
}


}  // namespace base

#endif  // YCSB_C_ZIPFIAN_GENERATOR_H_