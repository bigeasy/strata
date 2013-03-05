#!/usr/bin/env node

var fs = require("fs"), strata;
require("./proof")(7, function (tmp, equal, step) {
  step(function () {

    fs.writeFile(tmp + "/segment00000000", JSON.stringify([-1]) + " -\n", "utf8", step());

  }, function () {

    var body = JSON.stringify([0,1,0,0,1,[]]) + " -\n" +
               JSON.stringify([1,1,2,"a"]) + " -\n";
    fs.writeFile(tmp + "/segment00000001", body, "utf8", step());

  }, function (Strata) {

    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.open(step());

  }, function (equal) {

    equal(strata.size, 0, "json size before read");

    strata.iterator("a", step());

  }, function (cursor, ok) {

    ok(! cursor.exclusive, "shared");
    equal(cursor.index, 0, "index");
    equal(cursor.offset, 0, "offset");
    

    cursor.get(cursor.offset, step());

  }, function (got, cursor) {

    equal(got, "a", "get");
    equal(strata.size, 32, "json size after read");

    cursor.unlock();

    strata.purge(0);
    equal(strata.size, 0, "page");

    strata.close(step());
  });
});
