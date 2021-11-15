// Copyright 2021, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include "base/gtest.h"
#include "base/logging.h"

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

}  // namespace base