// Copyright 2022, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#pragma once

#include <absl/base/optimization.h>

#include <array>
#include <cstring>
#include <stdexcept>
#include <string_view>

namespace base {

/**
 * @brief string_view with small string optimization.
 *
 * If we hold strings in a flat container, we can not reference them with string_view:
 * upon each insertion/deletion a string object may be moved to another place and invalidate
 * our string_view reference. This class is a drop-in replacement to string_view, but solves
 * the problem above. It does it by hosting a sso itself in a compatible way with gcc/clang
 * implementations.
 *
 *
 */
class string_view_sso {
 public:
  using traits_type = std::char_traits<char>;
  using value_type = char;
  using pointer = char*;
  using const_pointer = const char*;
  using reference = char&;
  using const_reference = const char&;
  using const_iterator = const char*;
  using iterator = const_iterator;
  using const_reverse_iterator = std::reverse_iterator<const_iterator>;
  using reverse_iterator = const_reverse_iterator;
  using size_type = size_t;
  using difference_type = std::ptrdiff_t;

  static constexpr size_type npos = static_cast<size_type>(-1);

  // Null `string_view_sso` constructor
  constexpr string_view_sso() noexcept : size_(0) {
  }

  // Implicit constructors

  template <typename Allocator>
  string_view_sso(const std::basic_string<char, std::char_traits<char>, Allocator>& str) noexcept
      : string_view_sso(str.data(), str.size()) {
  }

  constexpr string_view_sso(std::string_view str) : string_view_sso(str.data(), str.size()) {
  }

  // Implicit constructor of a `string_view_sso` from NUL-terminated `str`. When
  // accepting possibly null strings, use `absl::NullSafeStringView(str)`
  // instead (see below).
  constexpr string_view_sso(const char* str)
      : buffer_(str, std::strlen(str)), size_(str ? std::strlen(str) : 0) {
  }

  // Implicit constructor of a `string_view_sso` from a `const char*` and length.
  constexpr string_view_sso(const char* data, size_type len) : buffer_(data, len), size_(len) {
  }

  // NOTE: Harmlessly omitted to work around gdb bug.
  //   constexpr string_view_sso(const string_view_sso&) noexcept = default;
  //   string_view_sso& operator=(const string_view_sso&) noexcept = default;

  // Iterators

  // string_view::begin()
  //
  // Returns an iterator pointing to the first character at the beginning of the
  // `string_view`, or `end()` if the `string_view_sso` is empty.
  constexpr const_iterator begin() const noexcept {
    return ptr();
  }

  // string_view_sso::end()
  //
  // Returns an iterator pointing just beyond the last character at the end of
  // the `string_view_sso`. This iterator acts as a placeholder; attempting to
  // access it results in undefined behavior.
  constexpr const_iterator end() const noexcept {
    return ptr() + size_;
  }

  // string_view_sso::cbegin()
  //
  // Returns a const iterator pointing to the first character at the beginning
  // of the `string_view_sso`, or `end()` if the `string_view_sso` is empty.
  constexpr const_iterator cbegin() const noexcept {
    return begin();
  }

  // string_view_sso::cend()
  //
  // Returns a const iterator pointing just beyond the last character at the end
  // of the `string_view_sso`. This pointer acts as a placeholder; attempting to
  // access its element results in undefined behavior.
  constexpr const_iterator cend() const noexcept {
    return end();
  }

  // string_view_sso::rbegin()
  //
  // Returns a reverse iterator pointing to the last character at the end of the
  // `string_view`, or `rend()` if the `string_view` is empty.
  const_reverse_iterator rbegin() const noexcept {
    return const_reverse_iterator(end());
  }

  // string_view_sso::rend()
  //
  // Returns a reverse iterator pointing just before the first character at the
  // beginning of the `string_view_sso`. This pointer acts as a placeholder;
  // attempting to access its element results in undefined behavior.
  const_reverse_iterator rend() const noexcept {
    return const_reverse_iterator(begin());
  }

  // string_view_sso::crbegin()
  //
  // Returns a const reverse iterator pointing to the last character at the end
  // of the `string_view_sso`, or `crend()` if the `string_view_sso` is empty.
  const_reverse_iterator crbegin() const noexcept {
    return rbegin();
  }

  // string_view_sso::crend()
  //
  // Returns a const reverse iterator pointing just before the first character
  // at the beginning of the `string_view_sso`. This pointer acts as a placeholder;
  // attempting to access its element results in undefined behavior.
  const_reverse_iterator crend() const noexcept {
    return rend();
  }

  // Capacity Utilities

  // string_view_sso::size()
  //
  // Returns the number of characters in the `string_view_sso`.
  constexpr size_type size() const noexcept {
    return size_;
  }

  // string_view_sso::length()
  //
  // Returns the number of characters in the `string_view_sso`. Alias for `size()`.
  constexpr size_type length() const noexcept {
    return size();
  }

  // string_view_sso::max_size()
  //
  // Returns the maximum number of characters the `string_view_sso` can hold.
  constexpr size_type max_size() const noexcept {
    return UINT32_MAX;
  }

  // string_view_sso::empty()
  //
  // Checks if the `string_view_sso` is empty (refers to no characters).
  constexpr bool empty() const noexcept {
    return size_ == 0;
  }

  // string_view_sso::operator[]
  //
  // Returns the ith element of the `string_view_sso` using the array operator.
  // Note that this operator does not perform any bounds checking.
  constexpr const_reference operator[](size_type i) const {
    return ptr()[i];
  }

  // string_view_sso::at()
  //
  // Returns the ith element of the `string_view_sso`. Bounds checking is performed,
  // and an exception of type `std::out_of_range` will be thrown on invalid
  // access.
  constexpr const_reference at(size_type i) const {
    if (ABSL_PREDICT_FALSE(i >= size())) {
      throw std::out_of_range{"base::string_view_sso::at"};
    }

    return ptr()[i];
  }

  // string_view_sso::front()
  //
  // Returns the first element of a `string_view_sso`.
  constexpr const_reference front() const {
    return ptr()[0];
  }

  // string_view_sso::back()
  //
  // Returns the last element of a `string_view_sso`.
  constexpr const_reference back() const {
    return ptr()[size() - 1];
  }

  // string_view_sso::data()
  //
  // Returns a pointer to the underlying character array (which is of course
  // stored elsewhere). Note that `string_view_sso::data()` may contain embedded nul
  // characters, but the returned buffer may or may not be NUL-terminated;
  // therefore, do not pass `data()` to a routine that expects a NUL-terminated
  // string.
  constexpr const_pointer data() const noexcept {
    return ptr();
  }

#if 0
  // Modifiers

  // string_view_sso::remove_prefix()
  //
  // Removes the first `n` characters from the `string_view_sso`. Note that the
  // underlying string is not changed, only the view.
  void remove_prefix(size_type n) {
    ptr() += n;
    size_ -= n;
  }

  // string_view_sso::remove_suffix()
  //
  // Removes the last `n` characters from the `string_view_sso`. Note that the
  // underlying string is not changed, only the view.
  void remove_suffix(size_type n) {
    size_ -= n;
  }
#endif

  // string_view_sso::swap()
  //
  // Swaps this `string_view_sso` with another `string_view_sso`.
  void swap(string_view_sso& s) noexcept {
    auto t = *this;
    *this = s;
    s = t;
  }

  // Explicit conversion operators

  // Converts to `std::basic_string`.
  template <typename A> explicit operator std::basic_string<char, traits_type, A>() const {
    if (!data())
      return {};
    return std::basic_string<char, traits_type, A>(data(), size());
  }

  constexpr explicit operator std::string_view() const {
    return std::string_view{ptr(), size_};
  }

  // string_view_sso::copy()
  //
  // Copies the contents of the `string_view_sso` at offset `pos` and length `n`
  // into `buf`.
  size_type copy(char* buf, size_type n, size_type pos = 0) const {
    if (ABSL_PREDICT_FALSE(pos > size_)) {
      throw std::out_of_range("absl::string_view_sso::copy");
    }
    size_type rlen = Min(size_ - pos, n);
    if (rlen > 0) {
      const char* start = ptr() + pos;
      traits_type::copy(buf, start, rlen);
    }
    return rlen;
  }

  // string_view_sso::substr()
  //
  // Returns a "substring" of the `string_view_sso` (at offset `pos` and length
  // `n`) as another string_view_sso. This function throws `std::out_of_bounds` if
  // `pos > size`.
  constexpr string_view_sso substr(size_type pos, size_type n = npos) const {
    if (ABSL_PREDICT_FALSE(pos > size_)) {
      throw std::out_of_range("absl::string_view_sso::substr");
    }
    return string_view_sso(ptr() + pos, Min(n, size_ - pos));
  }

  // string_view_sso::compare()
  //
  // Performs a lexicographical comparison between the `string_view_sso` and
  // another `absl::string_view`, returning -1 if `this` is less than, 0 if
  // `this` is equal to, and 1 if `this` is greater than the passed string
  // view. Note that in the case of data equality, a further comparison is made
  // on the respective sizes of the two `string_view`s to determine which is
  // smaller, equal, or greater.
  constexpr int compare(string_view_sso x) const noexcept {
    std::string_view me = static_cast<std::string_view>(*this);
    std::string_view her = static_cast<std::string_view>(x);
    return me.compare(her);
  }

  // Overload of `string_view_sso::compare()` for comparing a substring of the
  // 'string_view_sso` and another `absl::string_view_sso`.
  int compare(size_type pos1, size_type count1, string_view_sso v) const {
    return substr(pos1, count1).compare(v);
  }

  // Overload of `string_view_sso::compare()` for comparing a substring of the
  // `string_view_sso` and a substring of another `absl::string_view_sso`.
  int compare(size_type pos1, size_type count1, string_view_sso v, size_type pos2,
              size_type count2) const {
    return substr(pos1, count1).compare(v.substr(pos2, count2));
  }

  // Overload of `string_view_sso::compare()` for comparing a `string_view_sso` and a
  // a different  C-style string `s`.
  constexpr int compare(const char* s) const {
    return compare(string_view_sso(s));
  }

  // Overload of `string_view_sso::compare()` for comparing a substring of the
  // `string_view_sso` and a different string C-style string `s`.
  int compare(size_type pos1, size_type count1, const char* s) const {
    return substr(pos1, count1).compare(string_view_sso(s));
  }

  // Overload of `string_view_sso::compare()` for comparing a substring of the
  // `string_view_sso` and a substring of a different C-style string `s`.
  int compare(size_type pos1, size_type count1, const char* s, size_type count2) const {
    return substr(pos1, count1).compare(string_view_sso(s, count2));
  }

  // Find Utilities

  // string_view_sso::find()
  //
  // Finds the first occurrence of the substring `s` within the `string_view_sso`,
  // returning the position of the first character's match, or `npos` if no
  // match was found.
  size_type find(string_view_sso s, size_type pos = 0) const noexcept;

  // Overload of `string_view_sso::find()` for finding the given character `c`
  // within the `string_view_sso`.
  size_type find(char c, size_type pos = 0) const noexcept;

  // string_view_sso::rfind()
  //
  // Finds the last occurrence of a substring `s` within the `string_view_sso`,
  // returning the position of the first character's match, or `npos` if no
  // match was found.
  size_type rfind(string_view_sso s, size_type pos = npos) const noexcept;

  // Overload of `string_view_sso::rfind()` for finding the last given character `c`
  // within the `string_view_sso`.
  size_type rfind(char c, size_type pos = npos) const noexcept;

  // string_view_sso::find_first_of()
  //
  // Finds the first occurrence of any of the characters in `s` within the
  // `string_view_sso`, returning the start position of the match, or `npos` if no
  // match was found.
  size_type find_first_of(string_view_sso s, size_type pos = 0) const noexcept;

  // Overload of `string_view_sso::find_first_of()` for finding a character `c`
  // within the `string_view_sso`.
  size_type find_first_of(char c, size_type pos = 0) const noexcept {
    return find(c, pos);
  }

  // string_view_sso::find_last_of()
  //
  // Finds the last occurrence of any of the characters in `s` within the
  // `string_view_sso`, returning the start position of the match, or `npos` if no
  // match was found.
  size_type find_last_of(string_view_sso s, size_type pos = npos) const noexcept;

  // Overload of `string_view_sso::find_last_of()` for finding a character `c`
  // within the `string_view_sso`.
  size_type find_last_of(char c, size_type pos = npos) const noexcept {
    return rfind(c, pos);
  }

  // string_view_sso::find_first_not_of()
  //
  // Finds the first occurrence of any of the characters not in `s` within the
  // `string_view_sso`, returning the start position of the first non-match, or
  // `npos` if no non-match was found.
  size_type find_first_not_of(string_view_sso s, size_type pos = 0) const noexcept;

  // Overload of `string_view_sso::find_first_not_of()` for finding a character
  // that is not `c` within the `string_view_sso`.
  size_type find_first_not_of(char c, size_type pos = 0) const noexcept;

  // string_view_sso::find_last_not_of()
  //
  // Finds the last occurrence of any of the characters not in `s` within the
  // `string_view_sso`, returning the start position of the last non-match, or
  // `npos` if no non-match was found.
  size_type find_last_not_of(string_view_sso s, size_type pos = npos) const noexcept;

  // Overload of `string_view_sso::find_last_not_of()` for finding a character
  // that is not `c` within the `string_view_sso`.
  size_type find_last_not_of(char c, size_type pos = npos) const noexcept;

 private:
  static constexpr size_t Min(size_type length_a, size_type length_b) {
    return length_a < length_b ? length_a : length_b;
  }

  static constexpr int CompareImpl(size_type length_a, size_type length_b, int compare_result) {
    return compare_result == 0
               ? static_cast<int>(length_a > length_b) - static_cast<int>(length_a < length_b)
               : (compare_result < 0 ? -1 : 1);
  }
  enum { LOCAL_CAPACITY = 15 };

  constexpr const char* ptr() const {
    return size_ <= LOCAL_CAPACITY ? buffer_.local.data() : buffer_.begin;
  }

  union Buffer {
    const char* begin;
    std::array<char, LOCAL_CAPACITY + 1> local;

    constexpr Buffer() : begin(nullptr) {
    }
    constexpr Buffer(const char* s, size_t len) : local{} {
      if (len <= LOCAL_CAPACITY) {
        for (size_t i = 0; i < len; ++i) {
          local[i] = s[i];
        }
        local[len] = '\0';
      } else {
        begin = s;
      }
    }
  };

  Buffer buffer_;
  size_t size_;
};

// This large function is defined inline so that in a fairly common case where
// one of the arguments is a literal, the compiler can elide a lot of the
// following comparisons.
constexpr bool operator==(string_view_sso x, string_view_sso y) noexcept {
  return static_cast<std::string_view>(x) == static_cast<std::string_view>(y);
}

constexpr bool operator!=(string_view_sso x, string_view_sso y) noexcept {
  return !(x == y);
}

constexpr bool operator<(string_view_sso x, string_view_sso y) noexcept {
  return x.compare(y) < 0;
}

constexpr bool operator>(string_view_sso x, string_view_sso y) noexcept {
  return y < x;
}

constexpr bool operator<=(string_view_sso x, string_view_sso y) noexcept {
  return !(y < x);
}

constexpr bool operator>=(string_view_sso x, string_view_sso y) noexcept {
  return !(x < y);
}

// IO Insertion Operator
inline std::ostream& operator<<(std::ostream& o, string_view_sso piece) {
  return operator<<(o, static_cast<std::string_view>(piece));
}

}  // namespace base

namespace std {

// expected: hash support

template <> struct hash<base::string_view_sso> {
  size_t operator()(base::string_view_sso arg) const {
    return hash<string_view>{}(static_cast<string_view>(arg));
  }
};
}  // namespace std