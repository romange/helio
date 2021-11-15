// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef _CUCKOO_MAP_H
#define _CUCKOO_MAP_H

#include <memory>
#include <vector>
#include <type_traits>

#include "base/bits.h"
#include "base/integral_types.h"
#include "base/cuckoo_map-internal.h"

/* Cuckoo works very bad with non-prime table sizes.
   In particular, for random input we quickly find many numbers pairs that map to the same
   bucket pairs, i.e. n1..n8 map to buckets x and y. Once this happens cuckoo table must regrow
   no matter how unutilized the table is. However, with table prime sizes,
   those collisions disapear.
   */
namespace base {

uint64 GetPrimeNotLessThan(uint64 value);
/*
  Cuckoo Set.
*/
class CuckooSet : public CuckooMapTableWrapperBase {
public:
  // Allocates space for the minimal number of values.
  explicit CuckooSet(uint32 capacity = 0) : CuckooMapTableWrapperBase(0, capacity) {}

  // Inserts x into the map. This function invalidates all dense_ids.
  std::pair<DenseId, bool> Insert(KeyType v) { return table_.Insert(v, nullptr); }
  KeyType FromDenseId(DenseId d) const { return table_.FromDenseId(d).first;}
};

/*
  Cuckoo Map. T must be trivially copyable type.
*/
template<typename T> class CuckooMap : public CuckooMapTableWrapperBase {
public:
  explicit CuckooMap(uint32 capacity = 0) : CuckooMapTableWrapperBase(sizeof(T), capacity) {
    // TODO(roman): to add is_trivially_copyable restriction once it's supported by gcc.
    // static_assert(std::is_trivially_copyable<T>::value, "T should be copied trvially");
  }

  // Inserts x into the map. This function invalidates all dense_ids.
  std::pair<DenseId, bool> Insert(KeyType v, const T& t) {
    return table_.Insert(v, reinterpret_cast<const uint8*>(&t));
  }

  std::pair<KeyType, T*> FromDenseId(DenseId d) {
    auto p = table_.FromDenseId(d);
    return std::pair<KeyType, T*>(p.first, reinterpret_cast<T*>(p.second));
  }

  std::pair<KeyType, const T*> FromDenseId(DenseId d) const {
    auto p = table_.FromDenseId(d);
    return std::pair<KeyType, const T*>(p.first, reinterpret_cast<const T*>(p.second));
  }
};

}  // namespace base

#endif  // _CUCKOO_MAP_H