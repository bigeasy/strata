#!/usr/bin/env node

var fs = require("fs"), strata;
require("./proof")(6, function (tmp, equal, callback) {
  callback(function () {

    fs.writeFile(tmp + "/segment00000000", JSON.stringify([-1,[-1]]) + " -\n", "utf8", callback());

  }, function () {

    var body = JSON.stringify([0,1,0,0,1,[]]) + " -\n" +
               JSON.stringify([1,1,2,"a"]) + " -\n";
    fs.writeFile(tmp + "/segment00000001", body, "utf8", callback());

  }, function (Strata) {

    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.open(callback());

  }, function (equal) {

    equal(strata.stats.size, 0, "json size before read");

    strata.iterator("a", callback("cursor"));

  }, function (cursor, ok) {

    ok(! cursor.exclusive, "shared");
    equal(cursor.index, 0, "index");
    equal(cursor.offset, 0, "offset");
    

    cursor.get(cursor.offset, callback("got"));

  }, function (cursor, got) {

    equal(got, "a", "get");
    equal(strata.stats.size, 32, "json size after read");

    cursor.unlock();

  });
});
