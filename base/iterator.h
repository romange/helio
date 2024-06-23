// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#pragma once

#include <optional>
#include <utility>
#include <variant>
#include <optional>
#include <iterator>
#include <type_traits>

namespace base::it {

// Compound iterator for iterating over a multitude of different containers
template <typename F, typename... Its> struct CompoundIterator {
 private:
  using FirstIt = std::tuple_element_t<0, std::tuple<Its...>>;
  using FirstItReturn = typename std::iterator_traits<FirstIt>::value_type;

 public:
  // Start iterator
  template <typename T> CompoundIterator(F f, T&& it) : f(std::move(f)), it(std::forward<T>(it)) {
  }

  // End iterator
  template <typename T>
  CompoundIterator(std::nullopt_t, T&& it) : f(std::nullopt), it(std::forward<T>(it)) {
  }

  using iterator_category = std::forward_iterator_tag;
  using value_type = std::invoke_result_t<F, FirstItReturn>;
  using difference_type = std::ptrdiff_t;
  using pointer = value_type;
  using reference = value_type;

  value_type operator*() {
    return visit([this](auto& it) -> value_type { return (*f)(*it); }, it);
  }

  CompoundIterator& operator++() {
    visit([](auto& it) { ++it; }, it);
    return *this;
  }

  friend bool operator==(const CompoundIterator &a, const CompoundIterator &b) {
    return a.it == b.it;
  }

  friend bool operator!=(const CompoundIterator &a, const CompoundIterator &b) {
    return a.it != b.it;
  }

 private:
  std::optional<F> f;
  std::variant<Its...> it;
};

// Wrapper for providing for loops over iterator pair
template <typename T> struct Range : public std::pair<T, T> {
  Range(std::pair<T, T> it_pair) : std::pair<T, T>(it_pair) {
  }
  Range(T it_begin, T it_end) : std::pair<T, T>(std::move(it_begin), std::move(it_end)) {
  }

  auto begin() {
    return this->first;
  }

  auto end() {
    return this->second;
  }
};

// Apply function to iterator range
template <typename F, typename T> static auto Transform(F func, Range<T> range) {
  using CT = CompoundIterator<F, T>;
  return Range(CT(std::move(func), range.begin()), CT(std::nullopt, range.end()));
}

// Iterate over variant of containers while applying func.
// For example, use it iterate over variant<Span<const string>, Span<const string_view>>
template <typename F, typename... Ts>
static auto Wrap(F func, const std::variant<Ts...>& containers) {
  using It = CompoundIterator<F, typename Ts::const_iterator...>;
  auto it_begin =
      std::visit([&func](auto& it) -> It { return {std::move(func), it.begin()}; }, containers);
  auto it_end = std::visit([](auto& it) -> It { return {std::nullopt, it.end()}; }, containers);
  return Range(it_begin, it_end);
}

template<typename T>
auto WithIndex(Range<T> range) {
  auto f = [idx = size_t(0)](auto v) mutable { return std::make_pair(idx++, v); };
  return base::it::Transform(std::move(f), {range.first, range.second});
}

}  // namespace base::it
