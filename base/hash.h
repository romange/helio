// Copyright 2013, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#ifndef BASE_HASH_H
#define BASE_HASH_H

#include <cstdint>
#include <string>

#include <optional>
#include <string_view>

// XXH_STATIC_LINKING_ONLY allows accessing internal structs and declare them on stack
// without inlining everyting.
#define XXH_STATIC_LINKING_ONLY
#include <xxhash.h>

namespace base {

uint32_t MurmurHash3_x86_32(const uint8_t* data, uint32_t len, uint32_t seed);

inline uint32_t Murmur32(uint64_t val, uint32_t seed = 10) {
  return MurmurHash3_x86_32(reinterpret_cast<const uint8_t*>(&val), sizeof val, seed);
}

inline uint32_t Murmur32(const std::string& str, uint32_t seed = 10) {
  if (str.empty())
    return seed;
  return MurmurHash3_x86_32(reinterpret_cast<const uint8_t*>(str.data()), str.size(), seed);
}


uint64_t Fingerprint(const char* str, uint32_t len);

inline uint64_t Fingerprint(const std::string& str) {
  return Fingerprint(str.c_str(), str.size());
}

inline uint32_t Fingerprint32(const std::string& str) {
  uint64_t res = Fingerprint(str);
  return uint32_t(res >> 32) ^ uint32_t(res);
}

namespace detail {

// Should use std::has_unique_object_representations but we do not have in C++14.
template<typename Hasher, typename T>
std::enable_if_t<std::is_integral<T>::value || std::is_enum<T>::value>
HashAppend(Hasher& h, const T& t) noexcept {
  h.update(&t, sizeof(t));
}

template<typename Hasher, typename T>
void HashAppend(Hasher& h, const std::optional<T>& x) noexcept {
  if (x)
    HashAppend(h, *x);
}

template<typename Hasher> void HashAppend(Hasher& h, const std::string& t) noexcept {
  h.update(t.data(), t.size());
}

template<typename Hasher> void HashAppend(Hasher& h, const std::string_view& t) noexcept {
  h.update(t.data(), t.size());
}

template <class Hasher, typename T0, typename T1, typename ...T>
void HashAppend(Hasher& h, const T0& t0, const T1& t1, const T& ...t) noexcept {
  HashAppend(h, t0);
  HashAppend(h, t1, t...);
}

template <std::size_t BITS> class XXHashImp;

template <> class XXHashImp<32> {
    XXH32_state_t state_;
public:
  XXHashImp(unsigned seed = 0) noexcept {
    XXH32_reset(&state_, seed);
  }

  void update(void const* key, std::size_t len) noexcept {
    XXH32_update(&state_, key, len);
  }

  uint32_t digest() noexcept {
    return XXH32_digest(&state_);
  }
};

template <> class XXHashImp<64> {
    XXH64_state_t state_;
public:
  XXHashImp(unsigned long long seed = 0) noexcept {
    XXH64_reset(&state_, seed);
  }

  void update(void const* key, std::size_t len) noexcept {
    XXH64_update(&state_, key, len);
  }

  unsigned long long digest() noexcept {
    return XXH64_digest(&state_);
  }
};

}  // namespace detail

template <typename ...T> uint32_t XXHash32(const T&... t) {
  detail::XXHashImp<32> hasher;
  detail::HashAppend(hasher, t...);
  return hasher.digest();
}

template <typename ...T> uint64_t XXHash64(const T&... t) {
  detail::XXHashImp<64> hasher;
  detail::HashAppend(hasher, t...);
  return hasher.digest();
}


template <class ...T, size_t... Is>
uint64_t TupleHashImpl(const std::tuple<T...>& t, std::index_sequence<Is...>) {
  return XXHash64(std::get<Is>(t)...);
}

template <typename ...T> uint64_t XXHash64(const std::tuple<T...>& t) {
  return TupleHashImpl(t, std::index_sequence_for<T...>{});
}

template <typename T, typename U> uint64_t XXHash64(const std::pair<T,U>& t) {
  return XXHash64(t.first, t.second);
}

// Hash functor for std::tuple.
struct TupleHash {
  template <class ...T> size_t operator()(const std::tuple<T...>& t) const { return XXHash64(t); }
};

}  // namespace base

#endif  // BASE_HASH_H
