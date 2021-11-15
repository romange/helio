$(function() {

    $("table").tablesorter({
      theme : "blue",

      widthFixed: true,

      // widget code contained in the jquery.tablesorter.widgets.js file
      // use the zebra stripe widget if you plan on hiding any rows (filter widget)
      // the uitheme widget is NOT REQUIRED!
      widgets : [ "filter", "columns", "zebra" ],

      widgetOptions : {
        // using the default zebra striping class name, so it actually isn't included in the theme variable above
        // this is ONLY needed for bootstrap theming if you are using the filter widget, because rows are hidden
        zebra : ["even", "odd"],

        // class names added to columns when sorted
        columns: [ "primary", "secondary", "tertiary" ],
        filter_columnFilters: true,

        // reset filters button
        filter_reset : ".reset",
        filter_placeholder: { search : 'Search...' },
      }
    })
    .tablesorterPager({
      // target the pager markup - see the HTML block below
      container: $(".ts-pager"),

      // target the pager page select dropdown - choose a page
      cssGoto  : ".pagenum",

      // remove rows from the table to speed up the sort of large tables.
      // setting this to false, only hides the non-visible rows; needed if you plan to add/remove rows with the pager enabled.
      removeRows: false,

      // output string - default is '{page}/{totalPages}';
      // possible variables: {page}, {totalPages}, {filteredPages}, {startRow}, {endRow}, {filteredRows} and {totalRows}
      output: '{startRow} - {endRow} / {filteredRows} ({totalRows})',
      size: 10,
    });

  });
