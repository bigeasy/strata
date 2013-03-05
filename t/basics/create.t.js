#!/usr/bin/env node

var fs = require("fs"), strata;
require("./proof")(5, function (Strata, equal, deepEqual, say, tmp, step) {
  step(function () {

    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.create(step());

  }, function () {

    equal(strata.size, 4, "json size");
    strata.close(step());
    
  }, function (ok, load) {

    ok(1, "created");
    load(__dirname + "/fixtures/create.after.json", step());

  }, function (expected, objectify) {

    objectify(tmp, step());

  }, function (actual, expected) {

    say(actual);
    say(expected);

    deepEqual(actual, expected, "written");

    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.open(step());

  }, function () {

    strata.iterator("a", step());

  }, function (cursor, equal) {

    equal(cursor.length - cursor.offset, 0, "empty");

    cursor.unlock()

    strata.purge(0);
    equal(strata.size, 0, "purged");

    strata.close(step());
  });
});
