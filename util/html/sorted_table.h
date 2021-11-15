// Copyright 2018, Beeri 15.  All rights reserved.
// Author: Roman Gershman (romange@gmail.com)
//
#include <vector>
#include <string_view>

namespace util {
namespace html {

class SortedTable {
 public:
  static std::string HtmlStart();

  static void StartTable(const std::vector<std::string_view>& header, std::string* dest);

  static void Row(const std::vector<std::string_view>& row, std::string* dest);

  static void EndTable(std::string* dest);
};

/*
std::string SortedTable::Start(const Container& header) {
  std::string res(
      absl::StrCat("<table id='", name, "' class='tablesorter' cellspacing='1'> \n<thead>"));
  for (auto v : header) {
    res.append(absl::StrCat("<th>", v, "</th>"));
  }
  res.append("</thead>\n <tbody> \n");
  return res;
}

template <class Container>
std::string SortedTable::Row(std::string_view style, const Container& row) {
  std::string res("<tr>");
  std::string td_tag;
  if (style.empty()) {
    td_tag = "<td>";
  } else {
    td_tag = absl::StrCat("<td style='", style, "'>");
  }
  for (const auto& cell : row) {
    absl::StrAppend(&res, td_tag, cell, "</td>");
  }
  res.append("</tr>\n");
  return res;
}
*/

}  // namespace html
}  // namespace util
