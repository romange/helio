// Copyright 2021, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "base/gtest.h"
#include "base/logging.h"
#include "base/string_view_sso.h"
#include <absl/container/flat_hash_set.h>

using namespace std;

namespace base {

class CxxTest : public testing::Test {
 protected:
};

struct T {
  T(const T&) = delete;
  T(T&&) {
  }
};

T fn(T t) {
  return t;  // move used implicitly
}

struct A {};

A fn() {
  A t;
  return t;
}

static unsigned creations = 0;
static unsigned dtors = 0;
static unsigned move_ctors = 0;
static unsigned copy_ctors = 0;
static unsigned moves_ops = 0;
static unsigned copy_ops = 0;

class MyType {
 public:
  MyType(std::string str) : mName(std::move(str)) {
    creations++;
  }

  ~MyType() {
    dtors++;
  }

  MyType(const MyType& other) : mName(other.mName) {
    copy_ctors++;
  }

  // without noexcept vector does not use move c'tor.
  MyType(MyType&& other) noexcept : mName(std::move(other.mName)) {
    move_ctors++;
  }

  MyType& operator=(const MyType& other) {
    if (this != &other)
      mName = other.mName;
    copy_ops++;
    return *this;
  }

  MyType& operator=(MyType&& other) noexcept {
    if (this != &other)
      mName = std::move(other.mName);
    moves_ops++;
    return *this;
  }

 private:
  std::string mName;
};

template<typename T> class Wrapper {
public:
  Wrapper(T s) : t(move(s)) {}

  T t;
};

Wrapper<MyType> GetWrapper() {
  return MyType{"bar"};
};

struct HasVector {
  vector<int> vals;
};

TEST_F(CxxTest, Move) {
  MyType t{"foo"};
  MyType a = move(t);
  EXPECT_EQ(0, copy_ctors);

  vector<MyType> v;
  v.emplace_back("b1");
  v.emplace_back("b2");
  v.emplace_back("b3");
  v.emplace_back("b4");
  EXPECT_EQ(0, copy_ctors);
  EXPECT_EQ(0, copy_ops);
  EXPECT_GT(move_ctors, 2);

  auto w = GetWrapper();
  EXPECT_EQ(0, copy_ctors);
}

TEST_F(CxxTest, UnderDebugger) {
  vector<HasVector> table;

  vector<int> ints(1024);
  table.emplace_back(HasVector{.vals = move(ints)});
  table.emplace_back();  // verified that HasVector was moved without copying the array.
}

TEST_F(CxxTest, StringViewSSO) {
  constexpr string_view_sso s1("aaaa");
  static_assert(-1 == s1.compare("bbbb"));
  string s2("cccc");
  string_view_sso s3(s2);

  EXPECT_EQ(s3, s2);
  EXPECT_NE(s1, s2);
  EXPECT_NE(s1, s3);
  absl::flat_hash_set<string_view_sso> set;
  set.emplace("a");
  set.emplace("b");
  set.emplace("b");
  set.emplace(string_view{"foo"});
  EXPECT_EQ(3, set.size());
}

}  // namespace base