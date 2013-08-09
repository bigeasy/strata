#!/usr/bin/env node

var fs = require("fs"), strata;
require("./proof")(5, function (Strata, equal, deepEqual, say, tmp, step, ok, load, objectify) {
  step(function () {

    fs.writeFile(tmp + '/.ignore', '', 'utf8', step());

  }, function () {

    strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 });
    strata.create(step());

  }, function () {

    equal(strata.size, 3, "json size");
    strata.close(step());

  }, function () {

    ok(1, "created");
    objectify(tmp, step());
    load(__dirname + "/fixtures/create.after.json", step());

  }, function (actual, expected) {

    say(actual);
    say(expected);

    deepEqual(actual, expected, "written");

    strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 });
    strata.open(step());

  }, function () {

    strata.iterator("a", step());

  }, function (cursor) {

    equal(cursor.length - cursor.offset, 0, "empty");

    cursor.unlock()

    strata.purge(0);
    equal(strata.size, 0, "purged");

    strata.close(step());
  });
});
