#!/usr/bin/env node

var fs = require("fs"), strata;
require("./proof")(5, function (Strata, equal, deepEqual, say, tmp, async) {
  async(function () {

    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.create(async());

  }, function () {

    equal(strata.size, 4, "json size");
    strata.close(async());
    
  }, function (ok, load) {

    ok(1, "created");
    load(__dirname + "/fixtures/create.after.json", async());

  }, function (expected, objectify) {

    objectify(tmp, async());

  }, function (actual, expected) {

    say(actual);
    say(expected);

    deepEqual(actual, expected, "written");

    strata = new Strata(tmp, { leafSize: 3, branchSize: 3 });
    strata.open(async());

  }, function () {

    strata.iterator("a", async());

  }, function (cursor, equal) {

    equal(cursor.length - cursor.offset, 0, "empty");

    cursor.unlock()

    strata.purge(0);
    equal(strata.size, 0, "purged");

    strata.close(async());
  });
});
