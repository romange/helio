// Copyright 2017, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#include <algorithm>

#include <ostream>
#include <vector>

#include "base/integral_types.h"


template<typename T> std::ostream& operator<<(std::ostream& o, const std::vector<T>& vec) {
  o << "[";
  for (size_t i = 0; i < vec.size(); ++i) {
    o << vec[i];
    if (i + 1 < vec.size()) o << ",";
  }
  o << "]";
  return o;
}

template<typename T> std::ostream& operator<<(std::ostream& o, std::initializer_list<T> vec) {
  o << "[";
  for (auto it = vec.begin(); it != vec.end(); ++it) {
    o << *it;
    if (it + 1 != vec.end()) o << ",";
  }
  o << "]";
  return o;
}

namespace base {

template<typename T, typename U> bool _in(const T& t,
    std::initializer_list<U> l) {
  return std::find(l.begin(), l.end(), t) != l.end();
}


}  // namespace base
