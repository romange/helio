// Copyright 2013, Ubimo.com .  All rights reserved.
// Author: Roman Gershman (roman@ubimo.com)
//
#include "util/html/sorted_table.h"

#include "absl/strings/str_join.h"

namespace util {
namespace html {
using std::string;
namespace {

struct THFormatter {
  const char* token;

  THFormatter(const char* t) : token(t) {}
  void operator()(string* out, std::string_view str) const {
    absl::StrAppend(out, "<", token, ">", str, "</", token, ">");
  }
};

const char kCdnPrefix[] = "https://cdn.jsdelivr.net/gh/romange/async/util/html";
}  // namespace

string SortedTable::HtmlStart() {
  string result = R"(
<html>
<head>
  <meta charset="UTF-8">
  <link rel="stylesheet" href="https://cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.1/css/theme.bootstrap_4.min.css"> -->
  <link rel="stylesheet" href="//cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.1/css/theme.blue.min.css">
  <link rel="stylesheet" href="//cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.1/css/jquery.tablesorter.pager.min.css">
  <link rel="stylesheet" href=")";
  result.append(kCdnPrefix);
  result.append(R"(/style.css">
  <script src="//code.jquery.com/jquery-latest.min.js"></script>
  <script src="//cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.1/js/jquery.tablesorter.min.js"></script>
  <script src="//cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.1/js/jquery.tablesorter.widgets.min.js"></script>
  <script src="//cdnjs.cloudflare.com/ajax/libs/jquery.tablesorter/2.31.1/js/extras/jquery.tablesorter.pager.min.js"></script>
  <script src=")").append(kCdnPrefix);
  result.append(R"(/main.js"></script>
</head>
)");
  return result;
}

void SortedTable::StartTable(const std::vector<std::string_view>& header, string* dest) {
  string col_names = absl::StrCat("\n", absl::StrJoin(header, "\n", THFormatter{"th"}), "\n");
  dest->append(R"(
  <table class="tablesorter" id="tablesorter">
    <thead class="thead-dark"> <!-- add class="thead-light" for a light header -->)").
  append(absl::StrCat("<tr>", col_names, "</tr>", "</thead><tfoot>\n"));
  dest->append(absl::StrCat("<tr>", col_names));
  dest->append(R"(</tr> </tfoot>
    <tbody>)");
}

void SortedTable::EndTable(std::string* dest) {
  dest->append("</tbody></table>\n");
  dest->append(R"(<div class="ts-pager">
    <form>
      <div class="btn-group btn-group-sm mx-1" role="group">
        <button type="button" class="btn btn-secondary first" title="first">⇤</button>
        <button type="button" class="btn btn-secondary prev" title="previous">←</button>
        <button type="button" class="btn btn-secondary next" title="next">→</button>
        <button type="button" class="btn btn-secondary last" title="last">⇥</button>
      </div>
      <span class="pagedisplay"></span>
      <select class="form-control-sm custom-select px-1 pagesize" title="Select page size">
          <option selected="selected" value="10">10</option>
          <option value="20">20</option>
          <option value="30">30</option>
          <option value="all">All Rows</option>
        </select>
        <select class="form-control-sm custom-select px-4 mx-1 pagenum" title="Select page number"></select>
    </form>
  </div>)");
}

void SortedTable::Row(const std::vector<std::string_view>& row, std::string* dest) {
  absl::StrAppend(dest, "<tr>\n", absl::StrJoin(row, "\n", THFormatter{"td"}), "</tr>\n");
}

}  // namespace html
}  // namespace util
