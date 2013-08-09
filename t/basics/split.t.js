#!/usr/bin/env node

require("./proof")(3, function (step, tmp, serialize, deepEqual, load, objectify, say, Strata, gather) {
  var fs = require("fs"), strata, records = [];

  step(function () {
    serialize(__dirname + "/fixtures/split.before.json", tmp, step());
  }, function () {
    strata = new Strata({ directory: tmp, leafSize: 3, branchSize: 3 });
    strata.open(step());
  }, function () {
    strata.mutator("b", step());
  }, function (cursor) {
    step(function () {
      cursor.insert("b", "b", ~ cursor.index, step());
    }, function () {
      cursor.unlock();
      gather(step, strata);
    });
  }, function (records) {
    deepEqual(records, [ "a", "b", "c", "d" ], "records");
  }, function () {
    strata.balance(step());
  }, function () {

    objectify(tmp, step());
    load(__dirname + "/fixtures/split.after.json", step());

  }, function(actual, expected) {

    say(actual);
    say(expected);

    deepEqual(actual, expected, "split");

    strata.purge(0);
    deepEqual(strata.size, 0, "purged");

    strata.close(step());
  });
});
