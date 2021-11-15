// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//

#pragma once

#include <absl/base/internal/endian.h>

namespace base {

namespace LE {

template <typename T> static void StoreT(T val, void* dest);
template <typename T> static T LoadT(const void* dest);

template <> inline void StoreT<uint16_t>(uint16_t val, void* dest) {
  absl::little_endian::Store16(dest, val);
}

template <> inline void StoreT<uint32_t>(uint32_t val, void* dest) {
  absl::little_endian::Store32(dest, val);
}

template <> inline void StoreT<uint64_t>(uint64_t val, void* dest) {
  absl::little_endian::Store64(dest, val);
}

template <> inline uint8_t LoadT<uint8_t>(const void* src) {
  return *reinterpret_cast<const uint8_t*>(src);
}

template <> inline uint16_t LoadT<uint16_t>(const void* src) {
  return absl::little_endian::Load16(src);
}

template <> inline uint32_t LoadT<uint32_t>(const void* src) {
  return absl::little_endian::Load32(src);
}

template <> inline uint64_t LoadT<uint64_t>(const void* src) {
  return absl::little_endian::Load64(src);
}

constexpr bool IsLittleEndian() {
  return absl::little_endian::IsLittleEndian();
}

};  // namespace LE

}  // namespace base
