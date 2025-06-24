// Copyright 2025, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//

#include <nonstd/expected.hpp>
#include <string>
#include <system_error>

#include "base/flags.h"
#include "base/gtest.h"

namespace base {

class ExpectedTest : public testing::Test {};

using namespace nonstd;
using namespace std;

TEST_F(ExpectedTest, MonadicOperations) {
  expected<string, error_code> expected{"works"s};
  auto result = expected
    .transform([](const string& s) -> size_t { return s.size(); })
    .transform_error([](const error_code& ec) { return ec.message(); });
  static_assert(is_same_v<decltype(result)::value_type, size_t>);
  static_assert(is_same_v<decltype(result)::error_type, string>);
}

TEST_F(ExpectedTest, NoNoDiscard) {
  auto get_result = [](){
    return expected<int,int>(11);
  };
  get_result(); // should not fail due to nodiscard, check target_compile_definitions
}

}  // namespace base
