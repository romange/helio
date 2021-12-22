// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
// Based ClickHouse PODArray.
//
#pragma once

#include <string.h>

#include <algorithm>
#include <cstddef>
#include <memory>
#include <memory_resource>

#include <absl/numeric/bits.h>
#include <absl/base/macros.h>

namespace base {

/**
 * Default c'tor does not allocate. On the first allocation it allocates at least INITIAL_SIZE
 * bytes.
 *
 * Should be quicker than vector::push_back.
 *
 * Please note that for ALIGNMENT > 16 needs to be supported by memory_resource.
 * The default memory resource provides only 16 bytes alignment. For larger alignments use
 * custom memory resource, for example, one that wraps tcmalloc directly.
 */

template <size_t ELEM_SIZE, size_t ALIGNMENT> class PODArrayBase {
  static_assert((ALIGNMENT & (ALIGNMENT - 1)) == 0, "");

 protected:
  char* c_start_ = nullptr;
  char* c_end_ = nullptr;
  char* c_end_of_storage_ = nullptr;  /// Не включает в себя pad_right.
  std::pmr::memory_resource* mr_;

  PODArrayBase(std::pmr::memory_resource* mr) : mr_(mr ? mr : std::pmr::get_default_resource()) {
  }

  void alloc(size_t bytes) {
    bytes = absl::bit_ceil(bytes);
    c_start_ = c_end_ = reinterpret_cast<char*>(mr_->allocate(bytes, ALIGNMENT));
    c_end_of_storage_ = c_start_ + bytes;
  }

  void dealloc() {
    if (c_start_ == nullptr)
      return;
    mr_->deallocate(c_start_, allocated_size(), ALIGNMENT);
  }

  /// Количество памяти, занимаемое num_elements элементов.
  static constexpr size_t byte_size(size_t num_elements) {
    return num_elements * ELEM_SIZE;
  }

  /// Минимальное количество fпамяти, которое нужно выделить для num_elements элементов,
  // включая padding.
  static constexpr size_t minimum_memory_for_elements(size_t num_elements) {
    return std::max(ALIGNMENT, byte_size(num_elements));
  }

  void alloc_for_num_elements(size_t num_elements) {
    alloc(minimum_memory_for_elements(num_elements));
  }

  void realloc(size_t bytes) {
    if (c_start_ == nullptr)
      return alloc(bytes);
    bytes = absl::bit_ceil(bytes);
    char* new_start = reinterpret_cast<char*>(mr_->allocate(bytes, ALIGNMENT));
    ptrdiff_t sz = c_end_ - c_start_;
    memcpy(new_start, c_start_, sz);

    mr_->deallocate(c_start_, allocated_size(), ALIGNMENT);
    c_end_ = new_start + sz;
    c_start_ = new_start;
    c_end_of_storage_ = c_start_ + bytes;
  }

 public:
  size_t allocated_size() const {
    return c_end_of_storage_ - c_start_;
  }
  void clear() {
    c_end_ = c_start_;
  }

  void reserve(size_t n) {
    if (n > capacity()) {
      realloc(minimum_memory_for_elements(n));
    }
  }

  void resize(size_t n) {
    reserve(n);
    resize_assume_reserved(n);
  }

  void resize_assume_reserved(const size_t n) {
    c_end_ = c_start_ + byte_size(n);
  }

  /// Как resize, но обнуляет новые элементы.
  void resize_fill(size_t n) {
    size_t old_size = size();
    if (n > old_size) {
      reserve(n);
      memset(c_end_, 0, n - old_size);
    }
    c_end_ = c_start_ + byte_size(n);
  }

  bool empty() const {
    return c_end_ == c_start_;
  }
  size_t capacity() const {
    return (c_end_of_storage_ - c_start_) / ELEM_SIZE;
  }
  size_t size() const {
    return (c_end_ - c_start_) / ELEM_SIZE;
  }

  void swap(PODArrayBase& other) {
    // must be allocated from the same memory resource.
    std::swap(c_start_, other.c_start_);
    std::swap(c_end_, other.c_end_);
    std::swap(c_end_of_storage_, other.c_end_of_storage_);
  }

  std::pmr::memory_resource* mr() {
    return mr_;
  }
};

// PODArray
template <typename T, size_t ALIGNMENT = 16>
class PODArray : public PODArrayBase<sizeof(T), ALIGNMENT> {
 private:
  typedef PODArrayBase<sizeof(T), ALIGNMENT> ParentClass;
  using ParentClass::alloc_for_num_elements;
  using ParentClass::byte_size;
  using ParentClass::c_end_;
  using ParentClass::c_end_of_storage_;
  using ParentClass::c_start_;
  using ParentClass::dealloc;
  using ParentClass::minimum_memory_for_elements;
  using ParentClass::realloc;

  static constexpr size_t REAL_MIN_SIZE = ALIGNMENT;

  T* t_start() {
    return reinterpret_cast<T*>(c_start_);
  }
  T* t_end() {
    return reinterpret_cast<T*>(c_end_);
  }
  T* t_end_of_storage() {
    return reinterpret_cast<T*>(c_end_of_storage_);
  }

  const T* t_start() const {
    return reinterpret_cast<const T*>(c_start_);
  }
  const T* t_end() const {
    return reinterpret_cast<const T*>(c_end_);
  }
  const T* t_end_of_storage() const {
    return reinterpret_cast<const T*>(c_end_of_storage_);
  }

  void reserve() {
    if (c_start_ == nullptr)
      this->alloc(std::max(size_t(REAL_MIN_SIZE), minimum_memory_for_elements(1)));
    else
      realloc(minimum_memory_for_elements(capacity() * 2));
  }

 public:
  using value_type = T;
  using const_iterator = const T*;
  using iterator = T*;
  static constexpr size_t alignment_v = ALIGNMENT;

  using ParentClass::allocated_size;
  using ParentClass::capacity;
  using ParentClass::reserve;
  using ParentClass::resize;
  using ParentClass::resize_assume_reserved;
  using ParentClass::size;

  PODArray(std::pmr::memory_resource* mr = nullptr) : ParentClass(mr) {
  }

  PODArray(size_t n, std::pmr::memory_resource* mr) : ParentClass(mr) {
    alloc_for_num_elements(n);
    c_end_ += byte_size(n);
  }

  PODArray(size_t n, const T& x, std::pmr::memory_resource* mr) : ParentClass(mr) {
    alloc_for_num_elements(n);
    assign(n, x);
  }

  PODArray(const_iterator from_begin, const_iterator from_end, std::pmr::memory_resource* mr)
      : ParentClass(mr) {
    alloc_for_num_elements(from_end - from_begin);
    insert(from_begin, from_end);
  }

  ~PODArray() {
    dealloc();
  }

  PODArray(PODArray&& other) noexcept : ParentClass(nullptr) {
    this->swap(other);
  }

  PODArray& operator=(PODArray&& other) noexcept {
    this->swap(other);
    return *this;
  }

  T* data() {
    return t_start();
  }
  const T* data() const {
    return t_start();
  }

  T& operator[](size_t n) {
    return t_start()[n];
  }
  const T& operator[](size_t n) const {
    return t_start()[n];
  }

  T& front() {
    return t_start()[0];
  }
  T& back() {
    return t_end()[-1];
  }
  const T& front() const {
    return t_start()[0];
  }
  const T& back() const {
    return t_end()[-1];
  }

  iterator begin() {
    return t_start();
  }
  iterator end() {
    return t_end();
  }
  const_iterator begin() const {
    return t_start();
  }
  const_iterator end() const {
    return t_end();
  }
  const_iterator cbegin() const {
    return t_start();
  }
  const_iterator cend() const {
    return t_end();
  }

  void resize_fill(size_t n, const T& value) {
    size_t old_size = size();
    if (n > old_size) {
      reserve(n);
      std::fill(t_end(), t_end() + n - old_size, value);
    }
    c_end_ = c_start_ + byte_size(n);
  }

  void push_back(const T& x) {
    if (ABSL_PREDICT_FALSE(c_end_ == c_end_of_storage_))
      reserve();

    *t_end() = x;
    c_end_ += byte_size(1);
  }

  template <typename... Args> void emplace_back(Args&&... args) {
    if (ABSL_PREDICT_FALSE(c_end_ == c_end_of_storage_))
      reserve();

    new (t_end()) T(std::forward<Args>(args)...);
    c_end_ += byte_size(1);
  }

  void pop_back() {
    c_end_ -= byte_size(1);
  }

  /// Не вставляйте в массив кусок самого себя. Потому что при ресайзе,
  // итераторы на самого себя могут инвалидироваться.
  template <typename It1, typename It2> void insert(It1 from_begin, It2 from_end) {
    size_t required_capacity = size() + (from_end - from_begin);
    if (required_capacity > capacity())
      reserve(absl::bit_ceil(required_capacity));

    insert_assume_reserved(from_begin, from_end);
  }

  template <typename It1, typename It2> void insert(iterator it, It1 from_begin, It2 from_end) {
    size_t required_capacity = size() + (from_end - from_begin);

    size_t bytes_to_copy = byte_size(from_end - from_begin);

    // we compute bytes_to_move before reserve() call, otherwise 'it' can be invalidated.
    size_t bytes_to_move = (end() - it) * sizeof(T);

    if (required_capacity > capacity())
      reserve(absl::bit_ceil(required_capacity));

    if (ABSL_PREDICT_FALSE(bytes_to_move))
      memmove(c_end_ + bytes_to_copy - bytes_to_move, c_end_ - bytes_to_move, bytes_to_move);

    memcpy(c_end_ - bytes_to_move, reinterpret_cast<const void*>(&(*from_begin)), bytes_to_copy);
    c_end_ += bytes_to_copy;
  }

  template <typename It1, typename It2> void insert_assume_reserved(It1 from_begin, It2 from_end) {
    size_t bytes_to_copy = byte_size(from_end - from_begin);
    memcpy(c_end_, reinterpret_cast<const void*>(&*from_begin), bytes_to_copy);
    c_end_ += bytes_to_copy;
  }

  void assign(size_t n, const T& x) {
    resize(n);
    std::fill(begin(), end(), x);
  }

  template <typename It1, typename It2> void assign(It1 from_begin, It2 from_end) {
    size_t required_capacity = from_end - from_begin;
    if (required_capacity > capacity())
      reserve(absl::bit_ceil(required_capacity));

    size_t bytes_to_copy = byte_size(required_capacity);
    memcpy(c_start_, reinterpret_cast<const void*>(&*from_begin), bytes_to_copy);
    c_end_ = c_start_ + bytes_to_copy;
  }

  void assign(const PODArray& from) {
    assign(from.begin(), from.end());
  }

  bool operator==(const PODArray& other) const {
    if (size() != other.size())
      return false;

    const_iterator this_it = begin();
    const_iterator that_it = other.begin();

    while (this_it != end()) {
      if (*this_it != *that_it)
        return false;

      ++this_it;
      ++that_it;
    }

    return true;
  }

  bool operator!=(const PODArray& other) const {
    return !operator==(other);
  }
};

template <typename T, size_t pad_right_>
void swap(PODArray<T, pad_right_>& lhs, PODArray<T, pad_right_>& rhs) {
  lhs.swap(rhs);
}

}  // namespace base
