// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#pragma once

#include <string>
#include <vector>

namespace base {

class VarzValue {
 public:
  typedef std::vector<std::pair<std::string, VarzValue>> Map;

  // Should be union but too complicated to code it in c++11.
  int64_t num;
  double dbl;
  Map key_value_array;
  std::string str;

  enum Type { NUM, STRING, MAP, DOUBLE, TIME } type;

  VarzValue(std::string s) : str(std::move(s)), type(STRING) {
  }

  VarzValue(Map s) : key_value_array(std::move(s)), type(MAP) {
  }

  VarzValue(const VarzValue&) = default;

  VarzValue& operator=(const VarzValue&) = default;

  static VarzValue FromTime(time_t t) {
    return VarzValue{t, TIME};
  }

  static VarzValue FromDouble(double d) {
    return VarzValue{d};
  }

  static VarzValue FromInt(int64_t n) {
    return VarzValue{n, NUM};
  }

 private:
  VarzValue(int64_t n, Type t) : num(n), type(t) {
  }
  VarzValue(double d) : dbl(d), type(DOUBLE) {
  }
};

}  // namespace base
