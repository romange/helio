// Copyright 2023, Roman Gershman.  All rights reserved.
// See LICENSE for licensing terms.
//
#include "base/string_view_sso.h"

#include "base/flags.h"
#include "base/gtest.h"

namespace base {

class StringViewSSOTest : public testing::Test {};

using namespace std;

TEST_F(StringViewSSOTest, SSO) {
  // Test that string_view_sso has a large-enough internal buffer to work with all std::string.
  for (int i = 0; i < 100; ++i) {
    // Construct a string of size `i`
    string s(i, '.');
    string_view_sso sv(s);
    string new_s = std::move(s);
    s = "bla";
    EXPECT_EQ(new_s, sv);
    EXPECT_NE(new_s, s);
  }
}

}  // namespace base
